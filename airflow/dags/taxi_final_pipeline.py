import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = "/home/vishal/de_project"
SCRIPT_PATH = os.path.join(BASE_DIR, "scripts/incremental_batch_pipeline.py")

REPORT_SCRIPT = os.path.join(BASE_DIR, "scripts/query_lakehouse.py")

VENV_PYTHON = "/home/vishal/de_project/airflow_env/bin/python3"

default_args = {
    'owner': 'vishal',
    'start_date': datetime(2026, 2, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'taxi_incremental_final_v4',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['spark', 'minio', 'incremental']
)

SYSTEM_PYTHON = "/usr/bin/python3" 

run_spark_pipeline = BashOperator(
    task_id='run_spark_task',
    bash_command=f"{SYSTEM_PYTHON} {SCRIPT_PATH}",
    cwd=BASE_DIR,
    env={
        'MINIO_ACCESS_KEY': 'admin',
        'MINIO_SECRET_KEY': 'password',
        'MINIO_ENDPOINT': 'http://127.0.0.1:9000',
        'PATH': os.environ.get('PATH') 
    },
    dag=dag,
)

run_reports = BashOperator(
    task_id='generate_business_reports',
    bash_command=f"{VENV_PYTHON} {REPORT_SCRIPT} summary",
    cwd=BASE_DIR,
    env={
        'MINIO_ACCESS_KEY': 'admin',
        'MINIO_SECRET_KEY': 'password',
        'MINIO_ENDPOINT': 'http://127.0.0.1:9000',
        'HOME': '/home/vishal',
        'PATH': os.environ.get('PATH')
    },
    dag=dag,
)

run_spark_pipeline >> run_reports