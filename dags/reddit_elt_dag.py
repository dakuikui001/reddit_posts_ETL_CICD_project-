from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

# 设置时区
local_tz = pendulum.timezone("Asia/Singapore")

# 定义路径变量（对应 Astro 的目录结构）
INCLUDE_DIR = "/usr/local/airflow/include"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='reddit_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    schedule_interval='0 8 * * *', # 每天早上 8 点
    catchup=False,
    tags=['reddit', 'spark', 'lakehouse']
) as dag:

    # 1. 运行爬虫 Notebook
    # 假设 scrapy.ipynb 就在 include/ 目录下
    run_scrapy = BashOperator(
        task_id='run_scrapy_notebook',
        bash_command=(
            f"cd {INCLUDE_DIR} && "
            "papermill scrapy.ipynb out_scrapy.ipynb "
            "--log-output --no-progress-bar"
        ),
        execution_timeout=timedelta(minutes=10)
    )

    # 2. 运行 Spark ETL Notebook
    # 假设你的模块和 notebook 都在 include/pyspark/ 目录下
    # 关键：我们需要把 include/pyspark 加入 PYTHONPATH 才能让 Notebook import 成功的模块
    run_spark_main = BashOperator(
        task_id='run_spark_etl_notebook',
        bash_command=(
            f"export PYTHONPATH=$PYTHONPATH:{INCLUDE_DIR}/pyspark && "
            f"cd {INCLUDE_DIR}/pyspark && "
            "papermill run.ipynb out_run.ipynb "
            "--log-output --no-progress-bar"
        ),
        # Spark 任务通常较重，建议设置更长的超时
        execution_timeout=timedelta(minutes=20)
    )

    # 定义依赖关系
    run_scrapy >> run_spark_main