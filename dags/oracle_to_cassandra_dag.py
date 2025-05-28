from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys 
import os 

sys.path.append('/opt/airflow/scripts')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from extract_oracle_data import extract_oracle_data, transform_to_cassandra, load_to_cassandra

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='oracle_to_cassandra_migration',
    default_args=default_args,
    description='Migrate ALL data from Oracle to Cassandra at once',
    schedule_interval=None,
    catchup=False,
    tags=['migration', 'oracle', 'cassandra']
)
def oracle_to_cassandra_pipeline():
    
    @task
    def complete_migration():
        """
        Complete migration process - extract, transform, and load all data
        """
        print("🚀 Starting complete data migration...")
        
        # Step 1: Extract ALL data from Oracle
        print("📊 Step 1: Extracting data from Oracle...")
        oracle_data = extract_oracle_data()
        if oracle_data is None:
            print("❌ No data extracted from Oracle - oracle_data is None")
            raise ValueError("Oracle extraction returned None")
        
        if len(oracle_data) == 0:
            print("⚠️ No rows extracted from Oracle - table might be empty")
            return "No data to migrate"
        
        # Step 2: Transform data for Cassandra
        print("🔄 Step 2: Transforming data...")
        transformed_data = transform_to_cassandra(oracle_data)
        print(f"✅ Transformed {len(transformed_data)} rows")
        
        # Step 3: Load to Cassandra
        print("💾 Step 3: Loading to Cassandra...")
        load_to_cassandra(transformed_data)
        
        print("🎉 Complete data migration finished successfully!")
        return f"Migration completed: {len(transformed_data)} rows processed"
    
    # Execute the migration
    complete_migration()

# Create the DAG instance
dag_instance = oracle_to_cassandra_pipeline()