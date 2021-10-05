import sys

from airflow import DAG
from datetime import timedelta

from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import boto3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['lax@gmail.com', 'lax1@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def sysexecutable():
    print('Executables')
    print(sys.executable)

with DAG(
    'hello_world',
    default_args=default_args,
    description='Hello world description',
    start_date=days_ago(20),
    tags=['lax']
) as dag:

    t1 = BashOperator(
        task_id='print_msg',
        bash_command="echo 'hello_world'"
    )

    t2 = BashOperator(
        task_id='print_msg_again',
        bash_command="echo 'hello_world again'"
    )

    t3 = PythonOperator(
        task_id='check_system_env',
        python_callable=sysexecutable
    )

    t3