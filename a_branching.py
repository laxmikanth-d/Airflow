from datetime import datetime, timedelta

import random

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator


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


def hello_1_4():
    print('In method: hello_1_4')


def hello_4_8():
    print('In method: hello_4_8')


def hello_8_10():
    print('In method: hello_8_10')    


with DAG('branching',default_args=default_args, catchup=False, tags=['lax']
, schedule_interval='@once') as dag:

    decider = BranchPythonOperator(
        task_id = 'decider',
        python_callable=decide_branch,
        do_xcom_push = False
    )

    hello_1_4 = PythonOperator(
        task_id = 'hello_1_4',
        python_callable=hello_1_4
    )

    hello_4_8 = PythonOperator(
        task_id = 'hello_4_8',
        python_callable=hello_4_8
    )

    hello_8_10 = PythonOperator(
        task_id = 'hello_8_10',
        python_callable=hello_8_10
    )

    good_bye = PythonOperator(
        task_id = 'good_bye',
        python_callable=greet_good_bye,
        trigger_rule='one_success'
    )


    decider >> [hello_1_4, hello_4_8, hello_8_10] >> good_bye