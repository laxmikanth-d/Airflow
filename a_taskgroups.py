from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Lax',
    'start_date': datetime(2021, 12, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 0
}

with DAG('taskgroup_demo', default_args=default_args, schedule_interval='@once', tags=['lax']) as dag:

    task_1 = BashOperator(
        task_id='task_1',
        bash_command='sleep 3'
    )

    with TaskGroup('processing_tasks') as processing_tasks:
        task_2 = BashOperator(
            task_id='task_2',
            bash_command='sleep 3'
        )

        with TaskGroup('spark_tasks') as spark_tasks:    
            task_3 = BashOperator(
                task_id='task_3',
                bash_command='sleep 3'
            )

        with TaskGroup('flink_tasks') as flink_tasks:    
            task_3 = BashOperator(
                task_id='task_3',
                bash_command='sleep 3'
            )

    task_4 = BashOperator(
        task_id='task_4',
        bash_command='sleep 3'
    )

    task_1 >> processing_tasks >> task_4

