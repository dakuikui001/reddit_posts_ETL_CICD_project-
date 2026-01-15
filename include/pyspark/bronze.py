import great_expectations_common as gec
import os
from pathlib import Path
from pyspark.sql import functions as F

class Bronze():
    def __init__(self, spark_session, db_setup_manager):
        self.spark = spark_session
        self.manager = db_setup_manager
        
        # 1. 路径自适应
        if os.environ.get('AIRFLOW_HOME'):
            # Airflow 容器环境
            root = Path("/usr/local/airflow/include")
        else:
            # 本地环境
            root = Path(__file__).resolve().parent.parent

        # 数据源路径：明确到具体的主题文件夹
        self.raw_data_base = root / "data"
        
        # Checkpoint 路径建议改为 MinIO，确保持久化
        # 也可以保留本地，但要确保宿主机挂载了该卷
        self.checkpoint_base = f"s3a://{self.manager.bucket}/_checkpoints"

        print(f"📂 Bronze 数据基准路径: {self.raw_data_base}")
        print(f"💾 Checkpoint 根路径: {self.checkpoint_base}")

    def consume_reddit_posts_bz(self, once=True, processing_time="5 seconds"):
        # 明确子路径
        data_path = str(self.raw_data_base)
        
        schema = '''
                post_id STRING, 
                title STRING, 
                author STRING, 
                score INT,
                upvote_ratio DOUBLE, 
                comments INT, 
                flair STRING, 
                is_video STRING, 
                is_self STRING, 
                domain STRING, 
                url STRING,
                created_utc STRING, 
                selftext STRING,
                extracted_time TIMESTAMP
        '''
        
        # 检查本地路径是否存在，防止 readStream 立即报错
        if not os.path.exists(data_path):
            os.makedirs(data_path, exist_ok=True)
            print(f"⚠️ Warning: 路径 {data_path} 为空，已自动创建。")

        df_stream = (self.spark.readStream
                        .format("csv")
                        .schema(schema)
                        .option("header", "true")
                        .option("recursiveFileLookup", "true") 
                        .option("pathGlobFilter", "*.csv")
                        .option("maxFilesPerTrigger", 10) 
                        .load(data_path)
                        .withColumn("load_time", F.current_timestamp())
                    )
        
        # 传入 table_name = "reddit_posts_bz"
        return self._write_stream_append(
            df_stream, 
            "reddit_posts_bz", 
            "reddit_posts_bz_ingestion_stream", 
            "bronze_p1", 
            once, 
            processing_time
        )

    def _write_stream_append(self, df, table_name, query_name, pool, once, processing_time):
        # 构造 Checkpoint 路径
        checkpoint_path = f"{self.checkpoint_base}/{table_name}"
        
        # 内部定义 Batch 处理逻辑，确保序列化安全
        manager_instance = self.manager # 局部引用
        def batch_processor(micro_df, batch_id):
            gec.validate_and_insert_process_batch(
                micro_df, 
                batch_id, 
                table_name, 
                manager_instance
            )

        stream_writer = (df.writeStream
            .foreachBatch(batch_processor)
            .option("checkpointLocation", checkpoint_path)
            .queryName(query_name)
        ) 

        self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
        
        if once:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()

    def consume(self, once=True, processing_time="5 seconds"):
        import time
        start = time.time()
        print(f"\n🚀 Starting bronze layer consumption ...")
        
        # 获取 Active Stream
        stream = self.consume_reddit_posts_bz(once, processing_time)
        
        if once:
            # 这种方式比遍历所有 active streams 更精准
            stream.awaitTermination()
                
        print(f"✅ Completed bronze layer consumption in {int(time.time() - start)} seconds")