import os
import time
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class Upserter:
    def __init__(self, merge_query, temp_view_name):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name

    def upsert(self, df_micro_batch, batch_id):
        # --- 关键：微批次内去重 ---
        # 如果同一个 post_id 在一次抓取中出现了多次，MERGE 会报错
        # 我们只取最新的那一条记录进行合并
        window_spec = Window.partitionBy("post_id").orderBy(F.col("extracted_time").desc())
        df_deduplicated = (df_micro_batch
            .withColumn("rn", F.row_number().over(window_spec))
            .filter("rn = 1")
            .drop("rn")
        )
        
        df_deduplicated.createOrReplaceTempView(self.temp_view_name)
        df_deduplicated._jdf.sparkSession().sql(self.merge_query)

class Silver():
    def __init__(self, spark_session, db_setup_manager):
        self.spark = spark_session
        self.manager = db_setup_manager 
        self.db_name = "reddit_db"
        self.checkpoint_base = f"s3a://{self.manager.bucket}/_checkpoints/silver"
        print(f"💾 Silver 层 Checkpoint 根路径: {self.checkpoint_base}")
 
    def upsert_reddit_posts_sl(self, once=True, processing_time="15 seconds", startingVersion=0):
        # 获取表路径
        reddit_posts_bz = self.manager._get_table_location("reddit_posts_bz", "s3a")
        reddit_posts_sl = self.manager._get_table_location("reddit_posts_sl", "s3a")

        # --- 优化后的 MERGE 逻辑 ---
        # 只有在关键指标变化时才执行更新操作，减少无效写入
        query = f"""
            MERGE INTO delta.`{reddit_posts_sl}` a
            USING reddit_posts_sl_delta b
            ON a.post_id = b.post_id
            WHEN MATCHED AND (
                a.score != b.score OR 
                a.comments != b.comments OR 
                a.upvote_ratio != b.upvote_ratio
            ) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        data_upserter = Upserter(query, "reddit_posts_sl_delta")

        # 读取流
        df_delta = (self.spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .format("delta")
            .load(reddit_posts_bz)
            # 数据清洗与转换
            .withColumn("is_video", F.col("is_video").cast('boolean'))
            .withColumn("is_self", F.col("is_self").cast('boolean'))
            .withColumn("created_utc", F.to_timestamp(F.col("created_utc"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("update_time", F.current_timestamp())
        )

        return self._write_stream_update(
            df_delta, 
            data_upserter, 
            "reddit_posts_sl", 
            "reddit_posts_sl_upsert_stream", 
            "silver_p1", 
            once, 
            processing_time
        )

    def _write_stream_update(self, df, upserter, path, query_name, pool, once, processing_time):
        checkpoint_path = f"{self.checkpoint_base}/{path}"
        
        stream_writer = (df.writeStream
            .foreachBatch(upserter.upsert)
            .outputMode("update")
            .option("checkpointLocation", checkpoint_path)
            .queryName(query_name)
        )
        
        self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
        
        if once:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
    
    def _await_queries(self, once):
        if once:
            for stream in self.spark.streams.active:
                stream.awaitTermination()
    
    def upsert(self, once=True, processing_time="5 seconds"):
        start = int(time.time())
        print(f"\n🚀 启动 Silver 层 CDC Upsert...")

        # 运行 Upsert 任务
        self.upsert_reddit_posts_sl(once, processing_time)
        
        # 等待流结束
        self._await_queries(once)
        print(f"✅ Silver 层更新完成，耗时: {int(time.time()) - start} 秒")