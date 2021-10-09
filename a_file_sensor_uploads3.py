from datetime import datetime, timedelta

import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner': 'Lax',
    'start_date': datetime(2021, 9, 1),
    'email': ['lax@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
}


def load_json_to_s3():

    s3 = S3Hook(aws_conn_id='AWSS3Operations')    

    for file in os.scandir('./input_json_files'):
        if file.is_file():
            print(file.path)
            s3.load_file(filename=file.path,
                         key=file.name,
                         bucket_name='jsonfiles10052021',
                         replace=True
                         )
    
    
def sense_json_files():
    sensor = FileSensor(
                fs_conn_id='Sensor_Json_Files',
                filepath='*.*'
                )



with DAG('a_file_sensor_uploads3',
         default_args=default_args,
         schedule_interval="@once",
         tags=['lax']
         ) as dag:

    load_files = PythonOperator(
        task_id='load_json_to_s3',
        python_callable=load_json_to_s3,
        provide_context=True
    )

    json_file_sensor = FileSensor(
        task_id = "json_file_sensor",
        fs_conn_id='Sensor_Json_Files',
        filepath='*.json',
        poke_interval=5,
        timeout=200
    )

    json_file_sensor >> load_files
