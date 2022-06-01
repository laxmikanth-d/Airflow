from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests

default_args = {
    'owner': 'Lax',
    'start_date': datetime(2022,5,29),
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 0
}

def postgres_reader(**context):
    print("------------------------------")
    print("Hello")
    print(str(context))
    pg_hook = PostgresHook(postgre_conn_id='postgres_default', schema='demodb')
    pg_conn = pg_hook.get_conn()
    request = "SELECT * FROM return_order"
    crsr = pg_conn.cursor()
    crsr.execute(request)

    results = crsr.fetchall()

    for r in results:
        if r[2] == False:
            print(f'{r[0]} -- {r[1]} -- {r[2]}')
            data_obj = {
                'r0': r[0],
                'r1': r[1],
                'r2': r[2]
            }
            res = requests.post('https://reqres.in/api/users/2',data = data_obj)
            if res.status_code == 201:
                print('Returned status is 201')
                cursr = pg_conn.cursor()
                cursr.execute(f'UPDATE return_order SET processed = True WHERE id = {r[0]}')
                pg_conn.commit()
                cursr.close() 

    print("------------------------------")


with DAG("dbupdate_httpcalls", default_args = default_args, catchup=False, schedule_interval="@once", tags=["lax"]) as dag:

    t1 = PythonOperator (
        task_id = 'read_db',
        python_callable = postgres_reader,
        provide_context = True
    )

    t1
