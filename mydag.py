from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

from predictit_api_extract import extract_data
from predict_api_raw_store import fetch_from_s3_bucket

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,1,10),
    'email':['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'mydag',
    default_args=default_args,
    description="Extract raw data from predictitAPI"
)

dbt_command = "dbt run --profiles-dir /home/venkat/.dbt/ --project-dir /home/venkat/airflow/my_dags/dbt_scripts/transform_predictit/"


run_dag1 = PythonOperator(task_id='extract_raw_data',python_callable=extract_data,dag=dag)
run_dag2 = PythonOperator(task_id="raw_file_db",python_callable=fetch_from_s3_bucket,dag=dag)
run_dag3 = BashOperator(task_id="transform_dbt",bash_command=dbt_command,dag=dag)

run_dag1 >> run_dag2 >> run_dag3