
from datetime import datetime, timedelta

import pandas as pd
import sys 
import os 

from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import BranchPythonOperator

# Add scripts directory to path to import modules
sys.path.append('/opt/airflow/scripts')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from extract_oracle_data import extract_invoice_data, extract_revenue_data, extract_warehouse_data, extract_customer_data
from transform import transform_invoice_data, transform_revenue_data, transform_warehouse_data, transform_customer_data
from setup_connections import setup_connections 

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

setup_connections = PythonOperator(
	task_id='setup_db_connections',
	python_callable=setup_connections,
	dag=dag
)

# Task 1: Extract data from Oracle
extract_invoice_data = PythonOperator(
	task_id='extract_invoice_data',
	python_callable=extract_invoice_data,
	dag=dag 
)

extract_revenue_data = PythonOperator(
	task_id='extract_revenue_data',
	python_callable=extract_revenue_data,
	dag=dag 
)

extract_warehouse_data = PythonOperator(
	task_id='extract_warehouse_data',
	python_callable=extract_warehouse_data,
	dag=dag 
)

extract_customer_data = PythonOperator(
	task_id='extract_customer_data',
	python_callable=extract_customer_data,
	dag=dag 
)

# def validate_data_task(**context):
#     # Get data from previous task
#     extracted_data = context['task_instance'].xcom_pull(task_ids='extract_data_task')
	
#     if not extracted_data:
#         raise AirflowSkipException("No data extracted - stopping DAG")
	
#     # Check if any dataset is empty
#     empty_datasets = [key for key, data in extracted_data.items() if data.empty]
	
#     if empty_datasets:
#         raise AirflowSkipException(f"Empty datasets found: {empty_datasets} - stopping DAG")
	
#     return 'continue_processing_task'

# validate_nonempty_data_task = BranchPythonOperator(
#     task_id='validate_nonempty_extracted_data',
#     python_callable=validate_data_task,
#     dag=dag
# )

# Task 2: Transform the extracted data 
def transform_invoice_data(**kwargs):
	ti = kwargs['ti']
   
	input_data = ti.xcom_pull(task_ids='extract_invoice_data') 
	print(f"Extracted data type: {type(input_data)}")
	print("Transforming data...")
	
	return {
		'invoice_data': transform_invoice_data(input_data)
	}
	
transform_invoice_data = PythonOperator(
	task_id='transform_invoice_data',
	python_callable=transform_invoice_data,
	provide_context=True,
	dag=dag 
)

def transform_revenue_data(**kwargs):
	ti = kwargs['ti']
   
	input_data = ti.xcom_pull(task_ids='extract_revenue_data') 
	print(f"Extracted data type: {type(input_data)}")
	print("Transforming data...")
	
	return {
		'revenue_data': transform_revenue_data(input_data)
	}
	
transform_revenue_data = PythonOperator(
	task_id='transform_revenue_data',
	python_callable=transform_revenue_data,
	provide_context=True,
	dag=dag 
)

def transform_warehouse_data(**kwargs):
	ti = kwargs['ti']
   
	input_data = ti.xcom_pull(task_ids='extract_warehouse_data') 
	print(f"Extracted data type: {type(input_data)}")
	print("Transforming data...")
	
	return {
		'warehouse_data': transform_warehouse_data(input_data)
	}
	
transform_warehouse_data = PythonOperator(
	task_id='transform_warehouse_data',
	python_callable=transform_warehouse_data,
	provide_context=True,
	dag=dag 
)

def transform_customer_data(**kwargs):
	ti = kwargs['ti']
   
	input_data = ti.xcom_pull(task_ids='extract_customer_data') 
	print(f"Extracted data type: {type(input_data)}")
	print("Transforming data...")
	
	return {
		'customer_data': transform_customer_data(input_data)
	}
	
transform_customer_data = PythonOperator(
	task_id='transform_customer_data',
	python_callable=transform_customer_data,
	provide_context=True,
	dag=dag 
)

# Task 3: Load the transformed data to Cassandra
def load_data(**kwargs):
	ti = kwargs['ti']
	
	invoice_data = ti.xcom_pull(task_ids='transform_invoice_data')
	revenue_data = ti.xcom_pull(task_ids='transform_revenue_data')
	warehouse_data = ti.xcom_pull(task_ids='transform_warehouse_data')
	customer_data = ti.xcom_pull(task_ids='transform_customer_data')
 
	from load_to_cassandra import (
		load_invoice_details_data_optimized,
		load_revenue_data_optimized,
		load_wh_data_optimized,
		load_cus_data_optimized
	)
	
	print("Loading chi_tiet_hoa_don_theo_ma_kh data ...")
	if invoice_data and 'invoice_data' in invoice_data:
		load_invoice_details_data_optimized(invoice_data['invoice_data'])
	
	print("Loading doanh_thu_moi_ngay_theo_ma_cn data...")
	if revenue_data and 'revenue_data' in revenue_data:
		load_revenue_data_optimized(revenue_data['revenue_data'])
	
	print("Loading kho_sp_theo_ma_cn data...")
	if warehouse_data and 'warehouse_data' in warehouse_data:
		load_wh_data_optimized(warehouse_data['warehouse_data'])
	
	print("Loading sl_khach_hang_moi_ngay_theo_ma_cn data...")
	if customer_data and 'customer_data' in customer_data:
		load_cus_data_optimized(customer_data['customer_data'])
	
	print("All data is loaded successfully to Cassandra")
  
load_data_to_cassandra= PythonOperator(
	task_id='load_data_to_cassandra',
	python_callable=load_data,
	provide_context=True,
	dag=dag
)

# Set the task dependencies
setup_connections >> [extract_invoice_data, extract_revenue_data, extract_warehouse_data, extract_customer_data] \

extract_invoice_data >> transform_invoice_data 
extract_revenue_data >> transform_revenue_data
extract_warehouse_data >> transform_warehouse_data
extract_customer_data >> transform_customer_data

[transform_invoice_data, transform_revenue_data, transform_warehouse_data, transform_customer_data] >> load_data_to_cassandra