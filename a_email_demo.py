from airflow import DAG 
from datetime import datetime
from airflow.operators.email import EmailOperator

default_args = {
    'owner':'Lax',
    'email_on_failure': False,
    'depends_on_past': False,
    'email_on_retry': False,
    'start_date': datetime(2021, 12, 10),
    'retries': 0,
}

with DAG('send_email_notification', default_args = default_args, schedule_interval='@daily', tags=['lax']) as dag:

    send_email = EmailOperator(
        task_id='send_email_notification',
        to = 'dayapule.saiswetha@gmail.com',
        subject='Test email from Airflow',
        html_content='<html><h1>Congratulations</h1></html>'
    )

    send_email