from airflow import DAG
from datetime import datetime
import json
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'lax',
    'start_date': datetime(2022,5,19),
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 0
}

def load_jsonfile(**context):
        with open("/Users/laxmac/Documents/airflow-tutorial/airflow_home/dags/data/data_1.json") as f:
            data = json.load(f)
            print(context['ti'])
        context['ti'].xcom_push(key='orders', value=data)

# airflow.operators.trigger_dagrun: To continually run the dag again and again

with DAG('employee_import',schedule_interval='@daily', tags=['lax'], default_args=default_args,catchup=False) as dag:
    
    load_json = PythonOperator(
        task_id = 'load_json',
        python_callable = load_jsonfile,
        provide_context = True
    )

    load_json
