import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from dags.utils.oracle_connection import get_oracle_data
from dags.utils.cassandra_connection import load_to_cassandra

def migrate_data():
    # Extract data from Oracle
    oracle_data = get_oracle_data()
    
    # Load data into Cassandra
    load_to_cassandra(oracle_data)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

dag = DAG(
    'oracle_to_cassandra_migration',
    default_args=default_args,
    description='A simple DAG to migrate data from Oracle to Cassandra',
    schedule_interval='@daily',
)

migrate_task = PythonOperator(
    task_id='migrate_data_task',
    python_callable=migrate_data,
    dag=dag,
)

migrate_task