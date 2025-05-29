
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

from extract_oracle_data import extract_replicated_data, extract_branch_data
from transform import transform_replicated_data, transform_branch_data
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

setup_conn_task = PythonOperator(
	task_id='setup_db_connections',
	python_callable=setup_connections,
	dag=dag
)

# Task 1: Extract data from Oracle
extract_replicated_task = PythonOperator(
	task_id='01_extract_replicated_data_from_oracle',
	python_callable=extract_replicated_data,
	dag=dag 
)

extract_branch_task = PythonOperator(
	task_id='02_extract_branch_data_from_oracle',
	python_callable=extract_branch_data,
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
def transform_data_task(**kwargs):
    ti = kwargs['ti']
    replicated_data = ti.xcom_pull(task_ids='01_extract_replicated_data_from_oracle')
    branch_data = ti.xcom_pull(task_ids='02_extract_branch_data_from_oracle')
    
    print(f"Extracted replicated data type: {type(replicated_data)}")
    print(f"Extracted branch data type: {type(branch_data)}")
     
    if isinstance(replicated_data, dict):
        user_data = replicated_data.get('user_data')
        product_data = replicated_data.get('product_data')
        attr_product_data = replicated_data.get('attr_product_data')
        cat_product_data = replicated_data.get('cat_product_data')
        
    elif isinstance(replicated_data, (tuple, list)) and len(replicated_data) == 4:
        user_data, product_data, attr_product_data, cat_product_data = replicated_data
    else:
        print(f"Unexpected replicated data format: {type(replicated_data)}")
        user_data = product_data = attr_product_data = cat_product_data = pd.DataFrame()
    
    if not isinstance(branch_data, dict):
        print(f"Unexpected branch data format: {type(branch_data)}")
        branch_data = {}
    
    print("Transforming user data...")
    transformed_user_data = transform_replicated_data(user_data)
    
    print("Transforming product data...")
    transformed_product_data = transform_replicated_data(product_data)
    
    print("Transforming attribute data...")
    transformed_attr_product_data = transform_replicated_data(attr_product_data)
    
    print("Transforming category data...")
    transformed_cat_product_data = transform_replicated_data(cat_product_data)
    
    print("Transforming branch data...")
    transformed_branch_data = transform_branch_data(branch_data)
    
    return {
        'user_data': transformed_user_data,
        'product_data': transformed_product_data,
        'attr_product_data': transformed_attr_product_data,
        'cat_product_data': transformed_cat_product_data,
        'invoice_data': transformed_branch_data.get('invoice_data', pd.DataFrame()),
        'revenue_data': transformed_branch_data.get('revenue_data', pd.DataFrame()),
        'warehouse_data': transformed_branch_data.get('warehouse_data', pd.DataFrame()),
        'cus_data': transformed_branch_data.get('cus_data', pd.DataFrame())
    }

transform_task = PythonOperator(
	task_id='transform_data',
	python_callable=transform_data_task,
	provide_context=True,
	dag=dag 
)

# Task 3: Load the transformed data to Cassandra
def load_replicated_data(**kwargs):
	ti = kwargs['ti']
	
	transformed_data = ti.xcom_pull(task_ids='transform_data')
	
	if isinstance(transformed_data, dict):
	   
		from load_to_cassandra import (
			load_user_data_optimized, 
			load_product_data_optimized, 
			load_attr_product_data_optimized, 
			load_cat_product_data_optimized
		)
		
		print("Loading khachang data ...")
		load_user_data_optimized(transformed_data.get('user_data'))
		
		print("Loading sanpham data ...")
		load_product_data_optimized(transformed_data.get('product_data'))
		
		print("Loading thuoctinh_sanpham data ...")
		load_attr_product_data_optimized(transformed_data.get('attr_product_data'))
		
		print("Loading danhmuc_sanpham data ...")
		load_cat_product_data_optimized(transformed_data.get('cat_product_data'))
		
		print("All data is loaded successfully to Cassandra")
  
	else:
		print(f"Unexpected data format from transform task: {type(transformed_data)}")

def load_branch_data(**kwargs):
	ti = kwargs['ti']
	
	transformed_data = ti.xcom_pull(task_ids='transform_data')
	
	if isinstance(transformed_data, dict):
	   
		from load_to_cassandra import (
			load_invoice_details_data_optimized,
			load_revenue_data_optimized,
			load_wh_data_optimized,
			load_cus_data_optimized
		)
		
		print("Loading chi_tiet_hoa_don_theo_ma_kh data ...")
		load_invoice_details_data_optimized(transformed_data.get('invoice_data'))
		
		print("Loading doanh_thu_moi_ngay_theo_ma_cn data...")
		load_revenue_data_optimized(transformed_data.get('revenue_data'))
		
		print("Loading kho_sp_theo_ma_cn data...")
		load_wh_data_optimized(transformed_data.get('warehouse_data'))
		
		print("Loading sl_khach_hang_moi_ngay_theo_ma_cn data...")
		load_cus_data_optimized(transformed_data.get('cus_data'))
		
		print("All data is loaded successfully to Cassandra")
  
	else:
		print(f"Unexpected data format from transform task: {type(transformed_data)}")
  	
load_replicated_task = PythonOperator(
	task_id='01_load_replicated_data_to_cassandra',
	python_callable=load_replicated_data,
	provide_context=True,
	dag=dag
)

load_branch_task = PythonOperator(
	task_id='02_load_branch_data_to_cassandra',
	python_callable=load_branch_data,
	provide_context=True,
	dag=dag
)

# Set the task dependencies
setup_conn_task >> [extract_replicated_task, extract_branch_task] >> transform_task >> [load_replicated_task, load_branch_task]