from os import path
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.sftp.sensors.sftp import SFTPSensor

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 12, 24),
    'email_on_retry': False,
    'email_on_failure': True,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=1),
    'email': 'dayapule.saiswethaxxx@gmail.com'
}

sftp_default_conn = 'sftp_conn_id'

with DAG('sftpSensor', default_args=default_args, tags=['lax'], schedule_interval="@once") as dag:

    t1 = SFTPSensor(
        task_id='sftp_sense',
        sftp_conn_id='sftp_conn_id',
        path = '/Users/')
