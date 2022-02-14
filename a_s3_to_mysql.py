from airflow import DAG
import os
import json
import csv
from datetime import datetime, timedelta
from pathlib import Path
from airflow.utils.db import provide_session
from airflow.models.xcom import XCom

from airflow.operators.python import PythonOperator, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.mysql.hooks.mysql import MySqlHook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 12, 18),
    'email_on_retry': False,
    'email_on_failure': True,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=1),
    'email': 'dayapule.saiswethaxxx@gmail.com'
}

S3_SOURCE_BUCKET_NAME = 'jsonfiles10052021'
FILES_TYPE = '*.json'
AWS_CONNECTION_ID = 'AWSS3Operations'
JSON_LOCAL_PATH = '/Users/laxmac/airflow-tutorial/s3_downloads/'
CSV_LOCAL_PATH = '/Users/laxmac/airflow-tutorial/s3_downloads_csv/'


def download_s3_file():
    s3_hook = S3Hook(aws_conn_id=AWS_CONNECTION_ID)

    files = s3_hook.list_keys(bucket_name=S3_SOURCE_BUCKET_NAME, prefix="emp")

    for f in files:
        print(f'Found file {f}. Downloading now.....')
        temp_file = s3_hook.download_file(
            key=f, bucket_name=S3_SOURCE_BUCKET_NAME, local_path=JSON_LOCAL_PATH)
        print(
            f'Success downloading file {temp_file}. Original file name is {f}')
        os.rename(temp_file, os.path.join(JSON_LOCAL_PATH, f))

    return files


def json_csv_converter():

    for filename in os.listdir(JSON_LOCAL_PATH):
        print(f'Current json local path: {JSON_LOCAL_PATH}')
        if filename.endswith(".json"):
            full_file = os.path.join(JSON_LOCAL_PATH, filename)
            with open(full_file) as json_file:
               data = json.load(json_file)

            csv_filename = os.path.join(
                CSV_LOCAL_PATH, f"{filename.split('.')[0]}.csv")
            print(f'CSV file name will be {csv_filename}')
            csv_file = open(csv_filename, 'w')

            csv_writer = csv.writer(csv_file)

            count = 0

            for emp in data:
                if count == 0:
                    header = emp.keys()
                    csv_writer.writerow(header)
                    count += 1
                csv_writer.writerow(emp.values())

            csv_file.close()


def load_mysql():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_id', scheme='home')

    for filename in os.listdir(CSV_LOCAL_PATH):
        if filename.endswith(".csv"):
            full_file = os.path.join(CSV_LOCAL_PATH, filename)
            mysql_hook.bulk_load_custom(
                'Employee', full_file, 'IGNORE', "FIELDS TERMINATED BY ','")

@provide_session
def xcom_cleanup(session=None, **context):
    dag = context["dag"]
    dag_id = dag._dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


with DAG('s3toMysql', default_args=default_args, tags=['lax'], schedule_interval="@once") as dag:

    t1 = S3KeySensor(
        task_id='s3key_sensor',
        bucket_name=S3_SOURCE_BUCKET_NAME,
        bucket_key=FILES_TYPE,
        wildcard_match=True,
        aws_conn_id=AWS_CONNECTION_ID,
    )

    t2 = PythonOperator(
        task_id='downloadS3File',
        python_callable=download_s3_file,
    )

    t3 = PythonOperator(
        task_id='create_csv',
        python_callable=json_csv_converter,
    )

    t4 = PythonOperator(
        task_id='load_msql',
        python_callable=load_mysql,
    )

    t5 = PythonOperator(
        task_id='xcom_cleanup',
        python_callable=xcom_cleanup,
        provide_context=True,
    )

    t1 >> t2 >> t3 >> t4 >> t5
