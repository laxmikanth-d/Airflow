from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.custom_hello import HelloOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 3, 8),
    'depends_on_past': False,
    'email': ['lax@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('custom_hello', default_args=default_args,
         description='custom hello', start_date=days_ago(2), tags=['lax']) as dag:
    hello_task = HelloOperator(task_id="sample-task", name="foo-bar")

    hello_task