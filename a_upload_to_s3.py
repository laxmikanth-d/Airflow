import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 12, 5),
    'retries': 0,
    'email_on_retry': False,
    'email_on_failure': False,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=1),
    'email': 'lax@mail.com'
}

def upload_to_s3(**context):
    print(f'Current airflow working directory is {os.getcwd()}')
    s3_hook = S3Hook(aws_conn_id = 'AWSS3Operations')
    conn = s3_hook.get_conn()
    environment = 'dev'
    job_id = context['dag_run'].dag_id
    print(f'job_id: {job_id}')
    run_id = context['dag_run'].run_id
    print(f'run_id: {run_id}')
    s3_hook.load_file(filename="s3test.txt",key=f'{environment}/{job_id}/{run_id}/test_file.txt',bucket_name="airflowtest09292021",replace=True)
        
    

with DAG('s3upload', default_args=default_args, schedule_interval="@once", tags=['lax']) as dag:

    t1 = PythonOperator(
        task_id='copy_file_to_s3',
        python_callable=upload_to_s3,
        provide_context = True
    )
