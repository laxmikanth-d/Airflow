from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 12, 1),
    'retries': 0,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=1),
    'email': 'dayapule.saiswetha@gmail.com',
}


def choose_best_model_():
    accuracy = 3

    if accuracy > 5:
        return 'accurate'
    else:
        return 'inaccurate'


with DAG(
    'branching',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['lax'],
) as dag:

    choose_best_model = BranchPythonOperator(
        task_id='choose_best_model',
        python_callable=choose_best_model_,
        do_xcom_push=False,
    )

    accurate = DummyOperator(
        task_id='accurate'
    )

    inaccurate = DummyOperator(
        task_id='inaccurate'
    )

    storing = DummyOperator(
        task_id='storing',
        trigger_rule='none_failed_or_skipped'
    )

    choose_best_model >> [accurate, inaccurate] >> storing
