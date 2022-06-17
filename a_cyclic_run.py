from airflow import DAG
from pendulum import datetime
from airflow.operators.email import EmailOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'owner': 'Lax',
    'start_date': datetime(2022, 6, 15)
}

with DAG(
        'a_cyclic_run'
        , default_args=default_args
        , catchup=False
        , schedule_interval='@daily'
        , tags=['lax']
        ) as dag:

    start = EmailOperator(
        task_id = 'email_user',
        to='xxxx@gmail.com',
        subject='Cyclic Airflow DAG Test',
        html_content='<html><body><h1>Hello from Cyclic DAG</h1></body></html>'
    )

    trigger_back = TriggerDagRunOperator(
        task_id = 'trigger_back',
        trigger_dag_id='a_cyclic_run',
        conf={'message': 'Hello World'}
    )

    start >> trigger_back