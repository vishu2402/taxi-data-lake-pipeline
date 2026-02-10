import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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
    catchup=False
)

SYSTEM_PYTHON = "/usr/bin/python3" 
SCRIPT_PATH = "/home/vishal/de_project/scripts/incremental_batch_pipeline.py"

run_spark_pipeline = BashOperator(
    task_id='run_spark_task',
    bash_command=f"{SYSTEM_PYTHON} {SCRIPT_PATH}",
    cwd='/home/vishal/de_project',
    dag=dag,
)