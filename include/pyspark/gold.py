import os
import time
from pathlib import Path
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# --- 基础 Upserter 类 ---
class Upserter:
    def __init__(self, merge_query, temp_view_name):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name

    def upsert(self, df_micro_batch, batch_id):
        df_micro_batch.createOrReplaceTempView(self.temp_view_name)
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)

# --- CDC 专用 Upserter 类 ---
class CDCUpserter:
    def __init__(self, merge_query, temp_view_name, id_column, sort_by):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name
        self.id_column = id_column
        self.sort_by = sort_by

    def upsert(self, df_micro_batch, batch_id):
        if df_micro_batch.isEmpty():
            return
        # 1. 窗口去重逻辑
        window = Window.partitionBy(self.id_column).orderBy(F.col(self.sort_by).desc())
        
        # 2. 赋值给 deduped_df，确保使用的是 row_number()
        deduped_df = (df_micro_batch
            .withColumn("_rn", F.row_number().over(window))
            .filter("_rn == 1")
            .drop("_rn")
        )
        # 3. 注册视图
        deduped_df.createOrReplaceTempView(self.temp_view_name)
        # 4. 执行 SQL
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)

class Gold():
    def __init__(self, spark_session, db_setup_manager):
        self.spark = spark_session
        self.manager = db_setup_manager
        self.db_name = "reddit_db"
        self.checkpoint_base = f"s3a://{self.manager.bucket}/_checkpoints"
        print(f"💾 Gold Checkpoint 根路径: {self.checkpoint_base}")

    def upsert_gold_layers(self, once=True, processing_time="15 seconds"):
        # 1. 获取物理路径
        sl_path = self.manager._get_table_location("reddit_posts_sl", "s3a")
        fact_path = self.manager._get_table_location("fact_posts_gl", "s3a")
        dim_authors_path = self.manager._get_table_location("dim_authors_gl", "s3a")
        dim_flairs_path = self.manager._get_table_location("dim_flairs_gl", "s3a")
        dim_domains_path = self.manager._get_table_location("dim_domains_gl", "s3a")

        # 2. 初始化所有 Upserter (现在都定义在文件内部了)
        fact_upserter = CDCUpserter(
            f"""
            MERGE INTO delta.`{fact_path}` a 
            USING fact_batch b ON a.post_id = b.post_id 
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
            """,
            "fact_batch", "post_id", "extracted_time"
        )

        author_upserter = Upserter(f"MERGE INTO delta.`{dim_authors_path}` a USING author_batch b ON a.author = b.author WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *", "author_batch")
        flair_upserter = Upserter(f"MERGE INTO delta.`{dim_flairs_path}` a USING flair_batch b ON a.flair = b.flair WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *", "flair_batch")
        domain_upserter = Upserter(f"MERGE INTO delta.`{dim_domains_path}` a USING domain_batch b ON a.domain = b.domain WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *", "domain_batch")

        # 3. Micro-batch 处理器
        def gold_batch_processor(df_micro_batch, batch_id):
            if df_micro_batch.isEmpty():
                return
            
            # --- 转换逻辑 ---
            processed_df = (df_micro_batch
                .withColumn("format", 
                    F.when(F.col("is_self") == True, "text")
                     .when(F.col("is_video") == True, "video")
                     .otherwise("Others"))
                .withColumn("update_time", F.current_timestamp())
            )

            # A. 事实表 (使用 CDC Upserter 内部去重)
            fact_df = processed_df.select(
                "post_id", "title", "author", "score", "upvote_ratio", 
                "comments", "flair", "domain", "format", "url", 
                "created_utc", "selftext", "extracted_time", "update_time"
            )
            fact_upserter.upsert(fact_df, batch_id)

            # B. 维度表 (手动调用 dropDuplicates 确保简单去重)
            author_df = (processed_df.select("author", "update_time")
                         .filter("author IS NOT NULL")
                         .dropDuplicates(["author"]))
            author_upserter.upsert(author_df, batch_id)

            flair_df = (processed_df.select("flair", "update_time")
                        .filter("flair IS NOT NULL")
                        .dropDuplicates(["flair"]))
            flair_upserter.upsert(flair_df, batch_id)

            domain_df = (processed_df.select("domain", "update_time")
                         .filter("domain IS NOT NULL")
                         .dropDuplicates(["domain"]))
            domain_upserter.upsert(domain_df, batch_id)

        # 4. 读取 Silver 流
        df_stream = (self.spark.readStream
            .format("delta")
            .option("readChangeData", "true") # 关键：开启读取变更数据
            .option("startingVersion", 0)     # 或者从某个版本开始
            .load(sl_path)
        )

        return self._write_stream_update_gold(df_stream, gold_batch_processor, "gold_layer", "gold_upsert_stream", "gold_p1", once, processing_time)

    def _write_stream_update_gold(self, df, processor_func, path, query_name, pool, once, processing_time):
        checkpoint_path = f"{self.checkpoint_base}/{path}"
        stream_writer = (df.writeStream
            .foreachBatch(processor_func)
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
        print(f"\n🚀 执行 Gold 层 Upsert (自包含模式) ...")
        self.upsert_gold_layers(once, processing_time)
        self._await_queries(once)
        print(f"✅ 完成 Gold 层加工，耗时 {int(time.time()) - start} 秒")