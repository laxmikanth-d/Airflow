from datetime import datetime, timedelta

import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


default_args = {
    'owner': 'Lax',
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2021,10,9),
    'retry_delay': timedelta(minutes=5)
}


def decide_branch():
    ran = random.randint(1,10)

    print(f'Random number generated is {ran}.')

    if ran >= 1 and ran < 4:
        return 'hello_1_4'
    elif ran >= 4 and ran < 8:
        return 'hello_4_8'
    else:
        return 'hello_8_10'


def greet_good_bye():
    print('Bye bye.')



with DAG('branching',default_args=default_args, catchup=False, tags=['lax']
, schedule_interval='@once') as dag:

    decider = PythonOperator(
        task_id = 'decider',
        python_callable=decide_branch
    )

    hello_1_4 = DummyOperator(
        task_id = 'hello_1_4'
    )

    hello_4_8 = DummyOperator(
        task_id = 'hello_4_8'
    )

    hello_8_10 = DummyOperator(
        task_id = 'hello_8_10'
    )

    good_bye = PythonOperator(
        task_id = 'good_bye',
        python_callable=greet_good_bye,
        trigger_rule='none_failed_or_skipped'
    )



    decider >> [hello_1_4, hello_4_8, hello_8_10] >> good_bye