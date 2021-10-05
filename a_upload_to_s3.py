import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3_bucket import S3CreateBucketOperator
from datetime import datetime, timedelta
# from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


default_args = {
    'owner': 'Lax',
    'start_date': datetime(2021,9,1),
    'retry_delay': timedelta(minutes=5)
}


def sysexecutable():
    print('Executables')
    print(sys.executable)

def get_conn():
    s3hook = S3Hook(aws_conn_id='AWSS3Operations')
    conn = s3hook.get_conn()
    return str(conn)

def upload_to_s3(**context):
    s3hook = S3Hook(aws_conn_id='AWSS3Operations')    
    conn = s3hook.get_conn()
    
    environment = 'dev'
    job_id = context['dag_run'].dag_id
    run_id = context['dag_run'].run_id
  

    # C:\Users\ldhage\AppData\Local\Packages\CanonicalGroupLimited.Ubuntu20.04onWindows_79rhkp1fndgsc\LocalState\rootfs\home\lax

    s3hook.load_file(
        filename='airflow_test_write.txt',
        key=f'{environment}/{job_id}/{run_id}/test_file.txt',
        bucket_name='airflowtest09292021',
        replace=True
    )

    #'https://s3-us-west-2.amazonaws.com/airflowtest09292021')

def delete_bucket():
    s3Hook = S3Hook(aws_conn_id='AWSS3Operations')    
    res = s3Hook.delete_bucket("airflowtest09292021")
    
    
with DAG('upload_to_s3', 
        default_args=default_args, 
        schedule_interval="@once",
        tags=['lax']) as dag:
   

    t1 = PythonOperator(
        task_id='create_bucket',
        python_callable=upload_to_s3,
        provide_context = True
    )
  
    t1
