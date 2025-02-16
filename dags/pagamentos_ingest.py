from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta
from sqlalchemy import create_engine
from configparser import ConfigParser
import pandas as pd
import boto3
import os

SUBJECT = 'tb_pagamentos'
REF = datetime.now().strftime('%Y%m%d')
TS_PROC = datetime.now().strftime('%Y%m%d%H%M%S')

def _create_engine():
    config = ConfigParser()
    config_dir = os.getcwd() + 'config.cfg'
    config.read(config_dir)
    user = config.get('DB','user')
    password = config.get('DB','password')
    host = config.get('DB','host')
    database = config.get('DB','database')

    connection_string = f'postgresql://{user}:{password}@{host}/{database}'
    
    engine = create_engine(connection_string)

    return engine.connect()

def _extract_data(**kwargs):
    filename = f'{SUBJECT}_{REF}_{TS_PROC}'
    query = f'SELECT * FROM public.{SUBJECT}'
    df = pd.read_sql(query, _create_engine())
    df.to_csv(f'/tmp/{filename}.csv', index=False)

    kwargs['ti'].xcom_push(key='filename',value=filename)

def _ingest_data_to_s3(**kwargs):
    filename = kwargs['ti'].xcom_pull(key='filename')
    s3_client = boto3.client('s3')
    bucket_name = 'maicon-donza-lake-586794485137'
    prefix = f'00_ingestion/{SUBJECT}/{filename}.csv'
    try:
        with open(f'/tmp/{filename}.csv', 'rb') as f:
            s3_client.put_object(Bucket=bucket_name, Key=prefix, Body=f)

    except Exception as e:
        print(f'Erro!{e}')

default_args = {
    'owner': 'Maicon',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['maiconsntg@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

with DAG(
    dag_id='pagamentos_ingest',
    start_date=datetime(2025,1,22),
    schedule_interval='0 5 * * *',
    catchup=False
) as dag:
    start_execution = DummyOperator(
        task_id='start_execution'
    )

    extract_db_data = PythonOperator(
        task_id='extract_db_data',
        python_callable=_extract_data
    )

    ingest_data_to_s3 = PythonOperator(
        task_id='ingest_data_to_s3',
        python_callable=_ingest_data_to_s3
    )

    finish_execution = DummyOperator(
        task_id='finish_execution'
    )

    start_execution >> extract_db_data >> ingest_data_to_s3 >> finish_execution