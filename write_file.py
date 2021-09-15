from airflow import DAG
from datetime import datetime, timedelta
import os
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'email': 'lax@airflow.com',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}


def file_write_method():
    with open('test1.txt', 'w') as f:
        # Path here is /home/lax
        f.write('Laxmikanth weldone')
        f.close()
    cwd = os.getcwd()
    return cwd


with DAG(
    'write_file',
    default_args=default_args,
    description='Write file',
    start_date=days_ago(1),
    tags=['lax']
) as dag:

    fw = PythonOperator(
        task_id='file_write',
        python_callable=file_write_method
    )

fw
