from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime


default_args = {
    'start_date': datetime(2021,1,1)
}


def _choose_best_model():
    accuracy = 6
    if accuracy > 5:
        return 'accurate'
    elif accuracy <= 5:
        return 'inaccurate'
   

with DAG('branching', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    choose_best_model = BranchPythonOperator(
        task_id='choose_best_model',
        python_callable = _choose_best_model,
        do_xcom_push = False
    )

    accurate = DummyOperator(        
        task_id='accurate'
    )

    inaccurate = DummyOperator(
        task_id='inaccurate'
    )

    storing = DummyOperator(
        task_id = 'storing',
        trigger_rule = 'none_failed_or_skipped'
    )

    choose_best_model >> [accurate, inaccurate] >> storing
    