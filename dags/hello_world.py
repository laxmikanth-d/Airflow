from airflow import DAG
from datetime import datetime, timedelta

from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['lax@gmail.com', 'lax1@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
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

    t1 << t2
