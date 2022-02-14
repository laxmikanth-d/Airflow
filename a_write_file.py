from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
import os

from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'email':'lax@gmail.com',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

def file_write_method():
    with open('test.txt', 'w') as f:
        f.write('Weldone')
    cwd = os.getcwd()
    return cwd


with DAG(
    'write_file',
    default_args = default_args,
    description='Write file to cwd',
    start_date = days_ago(1),
    tags=['lax'],
) as dag:

    fw = PythonOperator(
        task_id = 'file_write',
        python_callable = file_write_method,
    )

fw