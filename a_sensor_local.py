from airflow import DAG
from datetime import datetime, timedelta

import os
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner':'airflow',
    'start_date': datetime(2021,12,7),
    'email_on_retry': False,
    'email_on_failure': False,
    'retries': 0,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=1),
    'email': 'lax@gmai.com',
}

def upload_to_s3():
    print(f'Current airflow working directory is {os.getcwd()}')
    s3 = S3Hook(aws_conn_id = 'AWSS3Operations')

    for file in os.scandir('/Users/laxmac/airflow-tutorial/all_input_files'):
        if file.is_file():
            print(file.path)
            s3.load_file(filename=file.path,
                        key=file.name,
                        bucket_name='jsonfiles10052021',
                        replace=True
            )
    

with DAG('file_sensor_local', default_args=default_args, schedule_interval='@once', tags=['lax']) as dag:

    t1 = FileSensor(
        task_id='wait_for_file',
        fs_conn_id='A_localfile',
        filepath='*.json',
        poke_interval = 5,
        timeout=200,
    )

    t2 = PythonOperator(
        task_id='copy_file_to_s3',
        python_callable=upload_to_s3,
        provide_context = True
    )


    t1 >> t2