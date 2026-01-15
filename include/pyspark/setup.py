import os
import time
from pathlib import Path
from dotenv import load_dotenv
from trino.dbapi import connect
from spark_common import MinIOSparkManager 

class LakehouseSetupManager():
    def __init__(self, spark_session, endpoint=None, access_key=None, secret_key=None, 
                 bucket=None, trino_host=None, trino_port=None):
        
        # 1. 自动加载环境变量
        load_dotenv()
        
        # 2. 基础配置优先级：显式传入 > 环境变量 > 默认值
        self.spark = spark_session
        self.endpoint = endpoint or os.getenv("AWS_ENDPOINT_URL")
        self.access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket = bucket or os.getenv("MINIO_BUCKET")
        
        # Trino 配置 (Astro 环境下 host 通常为 'trino')
        self.trino_host = trino_host or os.getenv("TRINO_HOST") # Astro 环境默认为 trino
        self.trino_port = int(trino_port or os.getenv("TRINO_PORT")) # 容器内部通常是 8080
        
        self.db_name = "reddit_db"
        
        # 初始化 MinIO Manager (复用参数)
        self.manager = MinIOSparkManager(
            endpoint=self.endpoint, 
            access_key=self.access_key, 
            secret_key=self.secret_key, 
            bucket=self.bucket
        )
        
        self.trino_config = {
            "host": self.trino_host,
            "port": self.trino_port,
            "user": "admin",
            "catalog": "delta"
        }
        
        # Checkpoint 路径建议也放入 S3 统一管理，避免本地路径权限问题
        self.checkpoint_base = f"s3a://{self.bucket}/_checkpoints"

    # --- 辅助方法 ---
    def _get_table_location(self, table_name, protocol="s3a"):
        """生成物理路径。Spark 用 s3a://, Trino 用 s3://"""
        return f"{protocol}://{self.bucket}/{self.db_name}/{table_name}"

    def execute_trino_raw_sql(self, sql):
        """执行 Trino DDL，增加连接超时处理"""
        try:
            with connect(**self.trino_config) as conn:
                cur = conn.cursor()
                cur.execute(sql)
                # fetchone 防止某些驱动需要显式消耗结果集
                return True
        except Exception as e:
            print(f"⚠️ Trino SQL 提示 (非致命): {e}")
            return False

    # --- 主流程 ---
    def setup(self):
        print(f"🏗️ 正在从零开始初始化 Lakehouse...")
        # 1. 确保数据库存在
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")

        table_definitions = {
            "reddit_posts_bz": "post_id STRING, title STRING, author STRING, score INT, upvote_ratio DOUBLE, comments INT, flair STRING, is_video STRING, is_self STRING, domain STRING, url STRING, created_utc STRING, selftext STRING, extracted_time TIMESTAMP, load_time TIMESTAMP",
            "reddit_posts_sl": "post_id STRING, title STRING, author STRING, score INT, upvote_ratio DOUBLE, comments INT, flair STRING, is_video BOOLEAN, is_self BOOLEAN, domain STRING, url STRING, created_utc TIMESTAMP, selftext STRING, extracted_time TIMESTAMP, load_time TIMESTAMP, update_time TIMESTAMP",
            "fact_posts_gl": "post_id STRING, title STRING, author STRING, score INT, upvote_ratio DOUBLE, comments INT, flair STRING, domain STRING, format STRING, url STRING, created_utc TIMESTAMP, selftext STRING, extracted_time TIMESTAMP, update_time TIMESTAMP",
            "dim_authors_gl": "author STRING, update_time TIMESTAMP",
            "dim_flairs_gl": "flair STRING, update_time TIMESTAMP",
            "dim_domains_gl": "domain STRING, update_time TIMESTAMP",
            "data_quality_quarantine": "table_name STRING, gx_batch_id STRING, violated_rules STRING, raw_data STRING, ingestion_time TIMESTAMP"
        }

        # 3. 物理建表循环
        for t, schema_sql in table_definitions.items():
            location = self._get_table_location(t, "s3a")
            
            # 先删掉旧的元数据
            self.spark.sql(f"DROP TABLE IF EXISTS {self.db_name}.{t}")

            # 如果是 Silver 表，直接在创建时开启 CDC
            tbl_props = ""
            if t == "reddit_posts_sl":
                tbl_props = "TBLPROPERTIES (delta.enableChangeDataFeed = true)"

            # 使用 正确的数据库名.表名
            create_sql = f"""
                CREATE TABLE {self.db_name}.{t} ({schema_sql}) 
                USING DELTA 
                LOCATION '{location}'
                {tbl_props}
            """
            self.spark.sql(create_sql)
            print(f"✅ Spark 物理表已就绪: {self.db_name}.{t} {'(CDC 已开启)' if tbl_props else ''}")

        # 4. Trino 注册 (注意：Trino 里我们手动加上 delta 前缀)
        from trino.dbapi import connect
        with connect(**self.trino_config) as conn:
            cur = conn.cursor()
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS delta.{self.db_name}")
            for t in table_definitions.keys():
                cur.execute(f"DROP TABLE IF EXISTS delta.{self.db_name}.{t}")
                location_trino = self._get_table_location(t, "s3")
                cur.execute(f"CALL delta.system.register_table('{self.db_name}', '{t}', '{location_trino}')")
                print(f"✅ Trino 注册成功: delta.{self.db_name}.{t}")

        print("🚀 所有表初始化完成！")

    def cleanup(self):
        print(f"🧹 执行全链路清理...")
        tables = ["reddit_posts_bz", "reddit_posts_sl", "fact_posts_gl", 
                  "dim_authors_gl", "dim_flairs_gl", "dim_domains_gl", "data_quality_quarantine"]
        
        # 1. 清理 Trino
        for t in tables:
            self.execute_trino_raw_sql(f"DROP TABLE IF EXISTS delta.{self.db_name}.{t}")
        self.execute_trino_raw_sql(f"DROP SCHEMA IF EXISTS delta.{self.db_name}")

        # 2. 清理 Spark
        for t in tables:
            self.spark.sql(f"DROP TABLE IF EXISTS {self.db_name}.{t}")
        self.spark.sql(f"DROP DATABASE IF EXISTS {self.db_name} CASCADE")

        # 3. 物理删除 (MinIO 上的数据 + Checkpoints)
        db_path = f"s3a://{self.bucket}/{self.db_name}"
        self._delete_s3_path(db_path)
        self._delete_s3_path(self.checkpoint_base)
        
        print("✨ 环境清理完成。")

    def _delete_s3_path(self, s3_path):
        """底层 Hadoop API 删除 S3 路径"""
        try:
            sc = self.spark.sparkContext
            Path_class = sc._gateway.jvm.org.apache.hadoop.fs.Path
            FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
            conf = sc._jsc.hadoopConfiguration()
            
            uri = sc._gateway.jvm.java.net.URI(s3_path)
            fs = FileSystem.get(uri, conf)
            fs.delete(Path_class(s3_path), True)
            print(f"✅ 物理路径已删除: {s3_path}")
        except Exception as e:
            print(f"ℹ️ 物理路径跳过 (可能已空): {s3_path}")

    def validate(self):
        print("\n🔍 正在验证 Lakehouse 状态:")
        for table in ["reddit_posts_bz", "reddit_posts_sl", "fact_posts_gl"]:
            exists = self.spark.catalog.tableExists(f"{self.db_name}.{table}")
            print(f"{'✅' if exists else '❌'} {table}")