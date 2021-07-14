from datetime import datetime, timedelta
from decimal import Decimal

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator


def purchase_quantity() -> int:
    return 10

def get_portfolio_cost() -> Decimal: 
    stock_price = 100
    quantity = 100
    total_cost = (stock_price * quantity)
    print(f'Total cost of stocks is {total_cost}')
    return total_cost


# ti stands for Task Instance object
def buy_no_buy_decision(ti):
    stock_price = 0
    if purchase_quantity() > 0:
        stock_price = 10000
        ti.xcom_push(key='stock_price', value=stock_price)
        return 'buy_stock'
    else:
        ti.xcom_push(key='stock_price', value=stock_price)
        return 'no_buy_stock'
    

def buy_stock(ti):
    price = ti.xcom_pull(key='stock_price', task_ids='buy_no_buy_decision')
    price = int(price) + 10
    print(price)


def no_buy_stock(ti):
    price =ti.xcom_pull(key='stock_price', task_ids='buy_no_buy_decision')
    price = int(price) + 10
    print(price)


default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "email@email.com",
    "retries":1,
    "retry_delay": timedelta(minutes=5)
}


with DAG(dag_id="stocks_dag",start_date=datetime(2021,7,13), schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    portfolio_cost = PythonOperator(
        task_id="portfolio_cost",
        python_callable = get_portfolio_cost,
        do_xcom_push = False
    )
    
    buy_no_buy_decision = BranchPythonOperator(
        task_id="buy_no_buy_decision",
        python_callable = buy_no_buy_decision,
        do_xcom_push = False
    )

    buy_stock = PythonOperator(
    task_id = 'buy_stock',
    python_callable = buy_stock,
    do_xcom_push = False
    )

    no_buy_stock = PythonOperator(        
    task_id = 'no_buy_stock',
    python_callable = no_buy_stock,
    do_xcom_push = False
    )

    buy_decision_taken = DummyOperator(
        task_id = 'buy_decision_taken',
        trigger_rule = 'none_failed_or_skipped',
        do_xcom_push = False
    )


    buy_no_buy_decision >> [buy_stock, no_buy_stock] >> buy_decision_taken