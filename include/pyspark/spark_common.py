import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession

class MinIOSparkManager:
    def __init__(self, endpoint=None, access_key=None, secret_key=None, bucket=None):
        # 1. 自动加载 .env 变量
        load_dotenv()
        
        # 优先级：显式传入参数 > 环境变量 > 默认值
        self.endpoint = endpoint or os.getenv("AWS_ENDPOINT_URL")
        self.access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket = bucket or os.getenv("MINIO_BUCKET")
        
        # 2. 依赖包配置 (Spark 3.5.x + Delta 3.2.1)
        self.delta_version = "3.2.1" 
        self.hadoop_aws_version = "3.3.4" 
        
        self.packages = [
            f"io.delta:delta-spark_2.12:{self.delta_version}",
            f"org.apache.hadoop:hadoop-aws:{self.hadoop_aws_version}"
        ]

    def create_session(self, app_name="RedditAnalysisProject"):
        """创建支持 Delta Lake 和 MinIO 的 Spark Session"""
        # 确保 Python 解释器一致性
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        
        builder = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", ",".join(self.packages)) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", self.endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", self.access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.caseSensitive", "true") \
            .config("spark.hadoop.fs.s3a.attempts.maximum", "10") \
            .config("spark.hadoop.fs.s3a.paging.maximum", "5000") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
            
        return builder.getOrCreate()

    def write_delta(self, df, table_name, mode="append"):
        target_path = f"s3a://{self.bucket}/delta/{table_name}"
        try:
            df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(target_path)
            return target_path
        except Exception as e:
            print(f"❌ 写入失败: {e}")
            return None

    def read_delta(self, spark, table_name):
        """从 MinIO 读取 Delta 表"""
        target_path = f"s3a://{self.bucket}/delta/{table_name}"
        return spark.read.format("delta").load(target_path)

    def write_stream_delta(self, df, table_name, checkpoint_dir):
        """流式写入 (ReadStream -> WriteStream)"""
        target_path = f"s3a://{self.bucket}/delta/{table_name}"
        checkpoint_path = f"s3a://{self.bucket}/checkpoints/{checkpoint_dir}"
        
        return df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .start(target_path)