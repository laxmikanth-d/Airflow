from datetime import datetime, timedelta, timezone
from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import bindparam, func

default_args = {
    'owner': 'Lax',
    'start_date': datetime(2022,6,15),
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 0
}

#  https://stackoverflow.com/questions/46707132/how-to-delete-xcom-objects-once-the-dag-finishes-its-run-in-airflow

@provide_session
def cleanup_xcom(session=None):
    # Prints all the XCom executed 1 minute(s) back
    ts_limit = datetime.now(timezone.utc) - timedelta(minutes=1)
    print(f'ts_limit is {ts_limit}')
    query = session.query(XCom).filter(XCom.timestamp < bindparam('id'))
    print('=============================')
    print(query.params(id = ts_limit).all())
    print('-----------------------------')
    # Delete all the XCom executed 1 minute(s) back
    query.params(id = ts_limit).delete()
    # Verify if all the XCom executed 1 minute(s) back are deleted.
    print(query.params(id = ts_limit).all())
    print('*****************************')
    
with DAG("a_xcom_cleanup"
        , default_args=default_args
        , catchup=False
        , schedule_interval="@once"
        , tags=["lax"]
        ) as dag:

    
    clean_xcom = PythonOperator(
        task_id="clean_xcom",
        python_callable = cleanup_xcom,
        provide_context=True
    )

    clean_xcom