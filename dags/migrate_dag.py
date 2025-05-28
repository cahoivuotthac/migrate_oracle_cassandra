
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG 
from airflow.operators.python import PythonOperator
import sys 
import os 

# Add scripts directory to path to import modules
sys.path.append('/opt/airflow/scripts')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from extract_oracle_data import extract_replicated_data
from transform import transform_data
from setup_connections import setup_connections 
from load_to_cassandra import load_user_data, load_product_data, load_attr_product_data, load_cat_product_data

default_args = {
	'owner': 'hienfaang',
	'email_on_failure': True, 
	'email_on_retry': False,
	'email': 'thuyhienphanthi2004@gmail.com',
	'retries': 3,
	'retry_delay': timedelta(minutes=5),
	'start_date': datetime(2025, 5, 28)
}

dag = DAG(
	dag_id='oracle_to_cassandra_migration',
	default_args=default_args,
	description='ETL pipeline from Oracle to Cassandra',
	schedule_interval=None, 
	catchup=False,
	tags=['migration', 'oracle', 'cassandra']
)

setup_conn_task = PythonOperator(
	task_id='setup_connections',
	python_callable=setup_connections,
	dag=dag
)

# Task 1: Extract data from Oracle
extract_task = PythonOperator(
	task_id='extract_data_from_oracle',
	python_callable=extract_replicated_data,
	dag=dag 
)

# Task 2: Transform the extracted data 
def transform_data_task(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract_data_from_oracle')
    
    print(f"Extracted data type: {type(extracted_data)}")
    
    if isinstance(extracted_data, dict):
        user_data = extracted_data.get('user_data')
        product_data = extracted_data.get('product_data')
        attr_product_data = extracted_data.get('attr_product_data')
        cat_product_data = extracted_data.get('cat_product_data')
        
        print(f"User data shape: {user_data.shape if hasattr(user_data, 'shape') else 'No shape'}")
        print(f"Product data shape: {product_data.shape if hasattr(product_data, 'shape') else 'No shape'}")
        print(f"Attr data shape: {attr_product_data.shape if hasattr(attr_product_data, 'shape') else 'No shape'}")
        print(f"Cat data shape: {cat_product_data.shape if hasattr(cat_product_data, 'shape') else 'No shape'}")
        
    elif isinstance(extracted_data, (tuple, list)) and len(extracted_data) == 4:
        user_data, product_data, attr_product_data, cat_product_data = extracted_data
    else:
        print(f"Unexpected extracted data format: {type(extracted_data)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Transform each dataset individually
    from transform import transform_data as transform_replicated_data
    
    print("Transforming user data...")
    transformed_user_data = transform_replicated_data(user_data)
    
    print("Transforming product data...")
    transformed_product_data = transform_replicated_data(product_data)
    
    print("Transforming attribute data...")
    transformed_attr_product_data = transform_replicated_data(attr_product_data)
    
    print("Transforming category data...")
    transformed_cat_product_data = transform_replicated_data(cat_product_data)
    
    return transformed_user_data, transformed_product_data, transformed_attr_product_data, transformed_cat_product_data

transform_task = PythonOperator(
	task_id='transform_data',
	python_callable=transform_data_task,
	provide_context=True,
	dag=dag 
)

# Task 3: Load the transformed data to Cassandra
def load_data(**kwargs):
    ti = kwargs['ti']
    
    transformed_data = ti.xcom_pull(task_ids='transform_data')
    
    if isinstance(transformed_data, tuple) and len(transformed_data) == 4:
        transformed_user_data, transformed_product_data, transformed_attr_product_data, transformed_cat_product_data = transformed_data
        
        print(f"User data type: {type(transformed_user_data)}")
        print(f"Product data type: {type(transformed_product_data)}")
        print(f"Attribute data type: {type(transformed_attr_product_data)}")
        print(f"Category data type: {type(transformed_cat_product_data)}")
        
        print("Loading user data...")
        load_user_data(transformed_user_data)
        
        print("Loading product data...")
        load_product_data(transformed_product_data)
        
        print("Loading attribute data...")
        load_attr_product_data(transformed_attr_product_data)
        
        print("Loading category data...")
        load_cat_product_data(transformed_cat_product_data)
        
        print("All data loaded successfully to Cassandra")
  
    else:
        print(f"Unexpected data format from transform task: {type(transformed_data)}")
	
load_task = PythonOperator(
	task_id='load_data_to_cassandra',
	python_callable=load_data,
	provide_context=True,
	dag=dag
)

# Set the task dependencies
setup_conn_task >> extract_task >> transform_task >> load_task