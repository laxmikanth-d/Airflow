from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor


default_args = {
    'owner': 'Lax',
    'start_date': datetime(2021,10,8),
    'email_on_failure': False,
    'email': ['laxmikanth.dhage@gmail.com'],
    'email_on_retry': False
}


with DAG('a_s3key_sensor', default_args=default_args, schedule_interval='@once', tags=['lax']) as dag:

    sense_file_in_s3 = S3KeySensor(
        task_id='sense_file_in_s3',
        bucket_key='*.json',
        bucket_name='jsonfiles10052021',
        aws_conn_id='AWSS3Operations',
        wildcard_match=True,
        verify=False
    )

    sense_file_in_s3