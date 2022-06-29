from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from confluent_kafka import Consumer
from sqlalchemy import true


default_args = {
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'owner':'Lax',
    'start_date': datetime(2022,6,25)
}

def kafka_consumer():
    config_params = {
        'bootstrap.servers':'pkc-pgq85.us-west-2.aws.confluent.cloud:9092',
        'security.protocol':'SASL_SSL',
        'sasl.mechanisms':'PLAIN',
        'sasl.username':'I5U2FEUQI62HEJBX',
        'sasl.password':'Hp04bmFsEUlBX2gNFAhB2MZNj9mmx/vcAcIc9tTk7YIWUDoStyquCW6OY+ehgwRi',
        'group.id':'python_example_group_1'
    }

    consumer = Consumer(config_params)
    topic = "first"
    consumer.subscribe([topic])
    
    while True:
        msg = consumer.poll(1)
        
        if msg is None:
            print('Waiting...')
        elif msg.error():
            print(f'Error: {msg.error()}')
        else:
            print(f"Consumed event from topic {msg.topic()}: value = {msg.value().decode('utf-8')}")
    

with DAG(
    'a_kafka_consumer'
    , default_args=default_args
    , tags=['lax']
    , schedule_interval='@daily'
    , catchup=False
) as dag:

    start = PythonOperator(
        task_id = 'start',
        python_callable=kafka_consumer
    )

    trigger_back = TriggerDagRunOperator(
        task_id='trigger_back',
        trigger_dag_id='a_kafka_consumer'
    )

    start >> trigger_back