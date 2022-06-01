from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Lax',
    'start_date': datetime(2022,5,29),
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 0
}

with DAG("a_bashoperator_demo", default_args = default_args, catchup=False, schedule_interval="@once", tags=["lax"]) as dag:

    t1 = BashOperator (
        task_id = 'bash_demo',
        bash_command='./data/hello.sh'
    )

    t1