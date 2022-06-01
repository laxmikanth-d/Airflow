from datetime import datetime
from airflow.decorators import dag, task
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

pg_hook = PostgresHook(postgre_conn_id='postgres_default', schema='demodb')
pg_conn = pg_hook.get_conn()

@dag(schedule_interval = '@daily', tags=['lax'], default_args=default_args, catchup = False)
def dbupdate_httpcalls():

    @task
    def read_db() -> list[tuple]:
        request = "SELECT * FROM return_order"
        crsr = pg_conn.cursor()
        crsr.execute(request)
        orders = crsr.fetchall()

        return orders

    @task
    def make_http(orders: list[tuple]):
        for order in orders:
            if order[2] == False:
                print(f'{order[0]} -- {order[1]} -- {order[2]}')
                data_obj = {
                    'id': order[0],
                    'response': order[1],
                    'processed': order[2]
                }
                res = requests.post('https://reqres.in/api/users/2',data = data_obj)
                if res.status_code == 201:
                    print('Returned status is 201')
                    cursr = pg_conn.cursor()
                    cursr.execute(f'UPDATE return_order SET processed = True WHERE id = {order[0]}')
                    pg_conn.commit()
                    cursr.close()

    make_http(read_db())

dag = dbupdate_httpcalls()
