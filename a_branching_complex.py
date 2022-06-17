from random import randint
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG
from pendulum import datetime

default_args = {
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
    "owner": "Lax",
    "start_date": datetime(2022, 6, 15)
}

def start():
    rand_val = randint(1, 100)

    if rand_val < 50:
        return 'p1'
    else:
        return 'p2'

def p1():
    print('In p1')
    rand_val = randint(1, 10)
    if rand_val < 5:
        return 'p11'
    else:
        return 'p12'

def p11():
    print('In p11')
    return 'p1_end'

def p12():
    print('In p12')
    return 'p1_end'

def p1_end():
    print('In p1_end')
    return 'end'

def p2():
    print('In p2')
    rand_val = randint(1, 10)
    if rand_val < 5:
        return 'p21'
    else:
        return 'p22'

def p21():
    print('In p21')
    return 'p2_end'

def p22():
    print('In p22')
    return 'p2_end'

def p2_end():
    print('In p2_end')
    return 'end'

def end():
    print('In end')
    return 'finally the end'

with DAG("a_branching_complex", default_args=default_args, catchup=False, schedule_interval="@once", tags=["lax"]) as dag:

    start = BranchPythonOperator(
        task_id="start",
        python_callable=start,
        provide_context=True
    )

    p1 = BranchPythonOperator(
        task_id="p1",
        python_callable=p1,
    )

    p11 = BranchPythonOperator(
        task_id="p11",
        python_callable=p11,
    )

    p12 = BranchPythonOperator(
        task_id="p12",
        python_callable=p12,
    )

    p1_end = BranchPythonOperator(
        task_id="p1_end",
        python_callable=p1_end,
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED
    )

    p2 = BranchPythonOperator(
        task_id="p2",
        python_callable=p2,
    )

    p21 = BranchPythonOperator(
        task_id="p21",
        python_callable=p21,
    )

    p22 = BranchPythonOperator(
        task_id="p22",
        python_callable=p22,
    )

    p2_end = BranchPythonOperator(
        task_id="p2_end",
        python_callable=p2_end,
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED
    )

    end = PythonOperator(
        task_id="end",
        python_callable=end,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED
    )

    start >> [p1, p2]
    p1 >> [p11, p12] >> p1_end
    p2 >> [p21, p22] >> p2_end
    [p1_end, p2_end] >> end
