import sys
import os

# 获取当前正在运行的这个文件 (.py 或 .ipynb) 的绝对路径
if '__file__' in locals():
    current_dir = os.path.dirname(os.path.abspath(__file__))
else:
    # 适配 Jupyter Notebook 环境
    current_dir = os.getcwd()

# 将当前目录加入系统路径，确保能 import 同级的 bronze, silver, pyspark_common 等
if current_dir not in sys.path:
    sys.path.append(current_dir)

# 如果你的 great_expectations_common.py 在 include/pyspark 下
# 但你想引用 include/ 下的其他东西，可以再往上一层
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)


import great_expectations as gx
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import traceback
import os
import json
import threading
import gc
from datetime import datetime

# ==========================================
# 1. 基础配置 (适配 MinIO / Delta Lake)
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
BASE_PATH = os.path.join(current_dir, "gx_configs/expectations/")
DB_NAME = "reddit_db"
QUARANTINE_TABLE = "data_quality_quarantine"

_SHARED_GX_CONTEXT = None
_CACHED_SUITES_JSON = {}
gx_lock = threading.RLock() 

# ==========================================
# 2. 配置预加载与上下文管理
# ==========================================
def preload_all_suites():
    global _CACHED_SUITES_JSON
    if not os.path.exists(BASE_PATH):
        print(f"⚠️ Warning: GX Expectations path not found: {BASE_PATH}")
        return
    files = [f for f in os.listdir(BASE_PATH) if f.endswith(".json")]
    for f in files:
        suite_name = f.replace(".json", "")
        try:
            with open(os.path.join(BASE_PATH, f), "r", encoding='utf-8') as file:
                suite_dict = json.load(file)
                suite_dict.pop("name", None)
                suite_dict.pop("data_context_id", None)
                _CACHED_SUITES_JSON[suite_name] = suite_dict
            print(f"✅ Preloaded GX Suite: {suite_name}")
        except Exception as e:
            print(f"❌ Load error {f}: {e}")

preload_all_suites()

def get_gx_context():
    global _SHARED_GX_CONTEXT
    with gx_lock:
        if _SHARED_GX_CONTEXT is None:
            _SHARED_GX_CONTEXT = gx.get_context(mode="ephemeral")
        return _SHARED_GX_CONTEXT

def load_suite_simple(context, suite_name):
    try:
        return context.suites.get(name=suite_name)
    except Exception:
        if suite_name in _CACHED_SUITES_JSON:
            suite_data = _CACHED_SUITES_JSON[suite_name]
            new_suite = gx.ExpectationSuite(
                name=suite_name, 
                expectations=suite_data.get("expectations", [])
            )
            return context.suites.add(new_suite)
        else:
            raise FileNotFoundError(f"Suite {suite_name} not found in cache.")

# ==========================================
# 3. 核心写入函数 (适配 Delta Lake)
# ==========================================
def delta_lake_insert(df, table_name, db_setup_manager):
    """适配 MinIO 的 Delta 写入方法"""
    try:
        # 获取 s3a 物理路径
        target_path = db_setup_manager._get_table_location(table_name, "s3a")
        
        # 使用 Delta 格式追加写入
        df.write.format("delta") \
          .mode("append") \
          .save(target_path)
    except Exception as e:
        print(f"❌ Delta Write Error [{table_name}]: {e}")
        raise e

# ==========================================
# 4. 验证与分流处理
# ==========================================
def validate_and_insert_process_batch(df, batch_id, table_name, db_setup_manager): 
    """
    适配 MinIO Lakehouse 的批处理验证
    db_setup_manager: LakehouseSetupManager 实例
    """
    spark_internal = df.sparkSession
    if df.limit(1).count() == 0: return

    temp_id_col = "_dq_batch_id"
    ds_name = f"ds_{table_name}_{batch_id}"
    val_def_name = f"val_{table_name}_{batch_id}"
    
    # 1. 生成唯一 ID
    df_with_id = df.withColumn(temp_id_col, F.monotonically_increasing_id()).persist()
    result = None 

    # --- GX 验证流 ---
    with gx_lock:
        try:
            context = get_gx_context()
            # 清理旧定义
            for n in [val_def_name, ds_name]:
                try: 
                    context.validation_definitions.delete(n) if "val" in n else context.data_sources.delete(n)
                except: pass

            datasource = context.data_sources.add_spark(name=ds_name)
            asset = datasource.add_dataframe_asset(name=f"asset_{batch_id}")
            batch_def = asset.add_batch_definition_whole_dataframe(name="batch_def")
            suite = load_suite_simple(context, f"{table_name}_suite")
            
            val_definition = context.validation_definitions.add(
                gx.ValidationDefinition(name=val_def_name, data=batch_def, suite=suite)
            )

            print(f"🚀 Batch {batch_id}: Running GX for {table_name}...")
            result = val_definition.run(
                batch_parameters={"dataframe": df_with_id},
                result_format={"result_format": "COMPLETE", "unexpected_index_column_names": [temp_id_col]}
            )
        except Exception as e:
            print(f"❌ GX Error: {str(e)}")
            # 报错保底：直接写入
            delta_lake_insert(df_with_id.drop(temp_id_col), table_name, db_setup_manager)
            return 
        finally:
            gc.collect()

    # --- 分流写入 MinIO ---
    try:
        curr_time = F.current_timestamp()
        
        if result and result.success:
            print(f"✅ Batch {batch_id}: Passed.")
            delta_lake_insert(df_with_id.drop(temp_id_col), table_name, db_setup_manager)
        
        elif result:
            errors = []
            for r in result.results:
                if not r.success:
                    conf = r.expectation_config
                    col = conf.kwargs.get("column", "Table")
                    rule = r.expectation_config.type
                    ids = r.result.get("unexpected_index_list")
                    if ids:
                        for row_id_dict in ids:
                            val = row_id_dict.get(temp_id_col)
                            if val is not None:
                                errors.append((val, f"[{col}] {rule}"))
            
            # 情况 B1: 表级错误
            if not errors: 
                bad_df = df_with_id.withColumn("violated_rules", F.lit("Table-level Error")) \
                    .withColumn("raw_data", F.to_json(F.struct([c for c in df.columns]))) \
                    .withColumn("ingestion_time", curr_time) \
                    .select(F.lit(table_name).alias("table_name"), 
                            F.lit(str(batch_id)).alias("gx_batch_id"),
                            "violated_rules", "raw_data", "ingestion_time")
                delta_lake_insert(bad_df, QUARANTINE_TABLE, db_setup_manager)
                return

            # 情况 B2: 行级错误隔离
            error_schema = StructType([
                StructField(temp_id_col, LongType(), True), 
                StructField("violated_rule", StringType(), True)
            ])
            error_info_df = spark_internal.createDataFrame(errors, schema=error_schema) \
                .groupBy(temp_id_col).agg(F.concat_ws("; ", F.collect_list("violated_rule")).alias("violated_rules"))

            bad_row_ids = [e[0] for e in errors]
            
            # 隔离脏数据 (匹配 reddit_db.data_quality_quarantine 结构)
            bad_df = df_with_id.filter(F.col(temp_id_col).isin(bad_row_ids)) \
                .join(error_info_df, on=temp_id_col, how="left") \
                .withColumn("raw_data", F.to_json(F.struct([c for c in df.columns]))) \
                .withColumn("ingestion_time", curr_time) \
                .select(F.lit(table_name).alias("table_name"), 
                        F.lit(str(batch_id)).alias("gx_batch_id"),
                        "violated_rules", "raw_data", "ingestion_time")
            
            delta_lake_insert(bad_df, QUARANTINE_TABLE, db_setup_manager)
            
            # 写入好数据
            good_df = df_with_id.filter(~F.col(temp_id_col).isin(bad_row_ids)).drop(temp_id_col)
            if good_df.limit(1).count() > 0:
                delta_lake_insert(good_df, table_name, db_setup_manager)
            
            print(f"⚠️ Batch {batch_id}: FAILED. {len(set(bad_row_ids))} rows quarantined.")

    except Exception as e:
        print(f"❌ Final Write Error: {str(e)}")
        delta_lake_insert(df_with_id.drop(temp_id_col), table_name, db_setup_manager)
    finally:
        if df_with_id.is_cached: df_with_id.unpersist()
        gc.collect()