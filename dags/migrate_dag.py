
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

from extract_oracle_data import extract_invoice_data, extract_revenue_branch_data, extract_warehouse_data, extract_customer_data, extract_doanh_thu_sp_quy_cn, extract_doanh_thu_thang_nv_cn
from transform import transform_invoice_data, transform_revenue_branch_data, transform_warehouse_data, transform_customer_data, transform_revenue_sp_quy_cn, transform_revenue_thang_nv_cn
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

setup_connections_op = PythonOperator(
	task_id='setup_db_connections',
	python_callable=setup_connections,
	dag=dag
)

# Task 1: Extract data from Oracle
extract_invoice_data_op = PythonOperator(
	task_id='extract_invoice_data',
	python_callable=extract_invoice_data,
	dag=dag 
)

extract_revenue_brnach_data_op = PythonOperator(
	task_id='extract_revenue_branch_data',
	python_callable=extract_revenue_branch_data,
	dag=dag 
)

extract_warehouse_data_op = PythonOperator(
	task_id='extract_warehouse_data',
	python_callable=extract_warehouse_data,
	dag=dag 
)

extract_customer_data_op = PythonOperator(
	task_id='extract_customer_data',
	python_callable=extract_customer_data,
	dag=dag 
)

extract_revenue_product_data_op = PythonOperator(
	task_id='extract_revenue_sp_data',
	python_callable=extract_doanh_thu_sp_quy_cn,
	dag=dag 
)

extract_revenue_employee_data_op = PythonOperator(
	task_id='extract_revenue_nv_data',
	python_callable=extract_doanh_thu_thang_nv_cn,
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
def transformed_invoice_data(**kwargs):
	ti = kwargs['ti']
   
	input_data = ti.xcom_pull(task_ids='extract_invoice_data') 
	print(f"Extracted data type: {type(input_data)}")
	print("Transforming data...")
	
	return {
		'invoice_data': transform_invoice_data(input_data)
	}
	
transform_invoice_data_op = PythonOperator(
	task_id='transform_invoice_data',
	python_callable=transformed_invoice_data,
	provide_context=True,
	dag=dag 
)

def transformed_revenue_data(**kwargs):
	ti = kwargs['ti']
   
	input_data = ti.xcom_pull(task_ids='extract_revenue_data') 
	print(f"Extracted data type: {type(input_data)}")
	print("Transforming data...")
	
	return {
		'revenue_data': transform_revenue_branch_data(input_data)
	}
	
transform_revenue_branch_data_op = PythonOperator(
	task_id='transform_revenue_branch_data',
	python_callable=transformed_revenue_data,
	provide_context=True,
	dag=dag 
)

def transformed_warehouse_data(**kwargs):
	ti = kwargs['ti']
   
	input_data = ti.xcom_pull(task_ids='extract_warehouse_data') 
	print(f"Extracted data type: {type(input_data)}")
	print("Transforming data...")
	
	return {
		'warehouse_data': transform_warehouse_data(input_data)
	}
	
transform_warehouse_data_op = PythonOperator(
	task_id='transform_warehouse_data',
	python_callable=transformed_warehouse_data,
	provide_context=True,
	dag=dag 
)

def transformed_customer_data(**kwargs):
	ti = kwargs['ti']
   
	input_data = ti.xcom_pull(task_ids='extract_customer_data') 
	print(f"Extracted data type: {type(input_data)}")
	print("Transforming data...")
	
	return {
		'customer_data': transform_customer_data(input_data)
	}
	
transform_customer_data_op = PythonOperator(
	task_id='transform_customer_data',
	python_callable=transformed_customer_data,
	provide_context=True,
	dag=dag 
)

def transformed_revenue_product_data(**kwargs):
    ti = kwargs['ti']
   
    input_data = ti.xcom_pull(task_ids='extract_revenue_sp_data') 
    print(f"Extracted data type: {type(input_data)}")
    print(f"Extracted data keys: {input_data.keys() if input_data else 'None'}")
    print("Transforming data...")
    
    if input_data and 'doanh_thu_sp_quy_cn_data' in input_data:
        df = input_data['doanh_thu_sp_quy_cn_data']
        print(f"DataFrame shape: {df.shape if hasattr(df, 'shape') else 'Not a DataFrame'}")
        
        if hasattr(df, 'empty') and not df.empty:
            transformed_df = transform_revenue_sp_quy_cn(df)
            return {
                'doanh_thu_sp_quy_cn_data': transformed_df
            }
        else:
            print("DataFrame is empty")
            return {'doanh_thu_sp_quy_cn_data': {}}
    else:
        print("No input data found for revenue product transformation")
        print(f"Available keys: {list(input_data.keys()) if input_data else 'None'}")
        return {'doanh_thu_sp_quy_cn_data': {}}
	
transform_revenue_product_data_op = PythonOperator(
	task_id='transform_revenue_sp_data',
	python_callable=transformed_revenue_product_data,
	provide_context=True,
	dag=dag 
)

def transformed_revenue_employee_data(**kwargs):
    ti = kwargs['ti']
   
    input_data = ti.xcom_pull(task_ids='extract_revenue_nv_data') 
    print(f"Extracted data type: {type(input_data)}")
    print(f"Extracted data keys: {input_data.keys() if input_data else 'None'}")
    print("Transforming data...")
    
    # Fixed: Check for the correct key that extraction returns
    if input_data and 'doanh_thu_thang_nv_cn_data' in input_data:
        df = input_data['doanh_thu_thang_nv_cn_data']
        print(f"DataFrame shape: {df.shape if hasattr(df, 'shape') else 'Not a DataFrame'}")
        
        if hasattr(df, 'empty') and not df.empty:
            transformed_df = transform_revenue_thang_nv_cn(df)
            return {
                'doanh_thu_thang_nv_cn_data': transformed_df
            }
        else:
            print("DataFrame is empty")
            return {'doanh_thu_thang_nv_cn_data': {}}
    else:
        print("No input data found for revenue employee transformation")
        print(f"Available keys: {list(input_data.keys()) if input_data else 'None'}")
        return {'doanh_thu_thang_nv_cn_data': {}}
    
transform_revenue_employee_data_op = PythonOperator(
    task_id='transform_revenue_nv_data',
    python_callable=transformed_revenue_employee_data,
    provide_context=True,
    dag=dag 
)

# Task 3: Load the transformed data to Cassandra
def load_invoice_data(**kwargs):
    ti = kwargs['ti']
    invoice_data = ti.xcom_pull(task_ids='transform_invoice_data')
    
    print("Loading chi_tiet_hoa_don_theo_ma_kh data ...")
    if invoice_data and 'invoice_data' in invoice_data:
        from load_to_cassandra import load_invoice_details_data_optimized
        load_invoice_details_data_optimized(invoice_data['invoice_data'].get('invoice_data'))
        print("Invoice data loaded successfully to Cassandra")
    else:
        print("No invoice data to load")
  
load_invoice_data_op = PythonOperator(
    task_id='load_invoice_data',
    python_callable=load_invoice_data,
    provide_context=True,
    dag=dag
)

def load_revenue_data(**kwargs):
    ti = kwargs['ti']
    revenue_data = ti.xcom_pull(task_ids='transform_revenue_branch_data')
    
    print("Loading doanh_thu_moi_ngay_theo_ma_cn data...")
    if revenue_data and 'revenue_data' in revenue_data:
        from load_to_cassandra import load_revenue_data_optimized
        load_revenue_data_optimized(revenue_data['revenue_data'].get('revenue_data'))
        print("Revenue data loaded successfully to Cassandra")
    else:
        print("No revenue data to load")
  
load_revenue_branch_data_op = PythonOperator(
    task_id='load_revenue_branch_data',
    python_callable=load_revenue_data,
    provide_context=True,
    dag=dag
)

def load_warehouse_data(**kwargs):
    ti = kwargs['ti']
    warehouse_data = ti.xcom_pull(task_ids='transform_warehouse_data')
    
    print("Loading kho_sp_theo_ma_cn data...")
    if warehouse_data and 'warehouse_data' in warehouse_data:
        from load_to_cassandra import load_wh_data_optimized
        load_wh_data_optimized(warehouse_data['warehouse_data'].get('warehouse_data'))
        print("Warehouse data loaded successfully to Cassandra")
    else:
        print("No warehouse data to load")
  
load_warehouse_data_op = PythonOperator(
    task_id='load_warehouse_data',
    python_callable=load_warehouse_data,
    provide_context=True,
    dag=dag
)

def load_customer_data(**kwargs):
    ti = kwargs['ti']
    customer_data = ti.xcom_pull(task_ids='transform_customer_data')
    
    print("Loading sl_khach_hang_moi_ngay_theo_ma_cn data...")
    if customer_data and 'customer_data' in customer_data:
        from load_to_cassandra import load_cus_data_optimized
        load_cus_data_optimized(customer_data['customer_data'].get('customer_data'))
        print("Customer data loaded successfully to Cassandra")
    else:
        print("No customer data to load")
  
load_customer_data_op = PythonOperator(
    task_id='load_customer_data',
    python_callable=load_customer_data,
    provide_context=True,
    dag=dag
)

def load_revenue_employee_data(**kwargs):
    ti = kwargs['ti']
    revenue_nv_data = ti.xcom_pull(task_ids='transform_revenue_nv_data')
    
    print("Loading 'doanh_thu_thang_nv_cn_data' ...")
    print(f"Retrieved data: {revenue_nv_data}")  # Debug print
    
    if revenue_nv_data and 'doanh_thu_thang_nv_cn_data' in revenue_nv_data:
        from load_to_cassandra import load_doanhthu_nv_data_optimized
        transformed_data = revenue_nv_data['doanh_thu_thang_nv_cn_data']
        print(f"Transformed data: {transformed_data}")  # Debug print
        
        if 'doanh_thu_thang_nv_cn' in transformed_data:
            load_doanhthu_nv_data_optimized(transformed_data['doanh_thu_thang_nv_cn'])
            print("'doanh_thu_thang_nv_cn' data loaded successfully to Cassandra")
        else:
            print("No 'doanh_thu_thang_nv_cn' data in transformed result")
    else:
        print("No 'doanh_thu_thang_nv_cn_data' data to load")
  
load_revenue_nv_op = PythonOperator(
    task_id='load_revenue_nv_data',
    python_callable=load_revenue_employee_data,
    provide_context=True,
    dag=dag
)

def load_revenue_product_data(**kwargs):
    ti = kwargs['ti']
    revenue_sp_data = ti.xcom_pull(task_ids='transform_revenue_sp_data')
    
    print("Loading 'doanh_thu_sp_quy_cn' data...")
    print(f"Retrieved data: {revenue_sp_data}")  # Debug print
    
    if revenue_sp_data and 'doanh_thu_sp_quy_cn_data' in revenue_sp_data:
        from load_to_cassandra import load_doanhthu_sp_data_optimized
        transformed_data = revenue_sp_data['doanh_thu_sp_quy_cn_data']
        print(f"Transformed data: {transformed_data}")  # Debug print
        
        if 'doanh_thu_sp_quy_cn' in transformed_data:
            load_doanhthu_sp_data_optimized(transformed_data['doanh_thu_sp_quy_cn'])
            print("'doanh_thu_sp_quy_cn' data loaded successfully to Cassandra")
        else:
            print("No 'doanh_thu_sp_quy_cn' data in transformed result")
    else:
        print("No 'doanh_thu_sp_quy_cn' data to load")
  
load_revenue_product_op = PythonOperator(
    task_id='load_revenue_sp_data',
    python_callable=load_revenue_product_data,
    provide_context=True,
    dag=dag
)

def verify_completion(**kwargs):
    print("All data has been successfully loaded to Cassandra")
    return True

verification_op = PythonOperator(
    task_id='verification',
    python_callable=verify_completion,
    dag=dag
)

# Update task dependencies
setup_connections_op >> [extract_invoice_data_op, extract_revenue_brnach_data_op, extract_warehouse_data_op, extract_customer_data_op, extract_revenue_product_data_op, extract_revenue_employee_data_op]

extract_invoice_data_op >> transform_invoice_data_op >> load_invoice_data_op
extract_revenue_brnach_data_op >> transform_revenue_branch_data_op >> load_revenue_branch_data_op
extract_warehouse_data_op >> transform_warehouse_data_op >> load_warehouse_data_op
extract_customer_data_op >> transform_customer_data_op >> load_customer_data_op
extract_revenue_product_data_op >> transform_revenue_product_data_op >> load_revenue_product_op
extract_revenue_employee_data_op >> transform_revenue_employee_data_op >> load_revenue_nv_op

[load_invoice_data_op, load_revenue_branch_data_op, load_warehouse_data_op, load_customer_data_op, load_revenue_product_op, load_revenue_nv_op] >> verification_op