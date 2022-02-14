from airflow import DAG
from datetime import datetime, timedelta

from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 12, 1),
    'depends_on_past': False,
    'email': ['lax@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hello_world',
    default_args=default_args,
    description='Hello world',
    start_date=days_ago(2),
    tags=['lax']
) as dag:

    t1 = BashOperator(
        task_id='print_message',
        bash_command="echo 'hello world's"
    )

    t1
    