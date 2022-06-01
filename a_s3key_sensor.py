from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

default_args = {
    'owner': 'Lax',
    'start_date': datetime(2022, 5, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 0
}

with DAG('s3key_sensor', default_args=default_args, schedule_interval="@once", tags=['lax']) as dag:

   t1 = S3KeySensor(
       task_id='s3key_sensor',
       bucket_name='input-files-json',
       bucket_key="*.txt",
       wildcard_match=True,
       aws_conn_id='aws_default'
   ) 

   t1