from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine

class OracleToCassandraOperator(BaseOperator):
    @apply_defaults
    def __init__(self, oracle_conn_id, cassandra_conn_id, query, table, *args, **kwargs):
        super(OracleToCassandraOperator, self).__init__(*args, **kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.cassandra_conn_id = cassandra_conn_id
        self.query = query
        self.table = table

    def execute(self, context):
        # Establish Oracle connection
        oracle_conn = BaseHook.get_connection(self.oracle_conn_id)
        oracle_engine = create_engine(oracle_conn.get_uri())
        
        # Extract data from Oracle
        self.log.info("Extracting data from Oracle...")
        df = pd.read_sql(self.query, oracle_engine)
        
        # Transform data if necessary (placeholder for transformation logic)
        self.log.info("Transforming data...")
        # Example transformation: df['new_column'] = df['existing_column'] * 2
        
        # Establish Cassandra connection
        cassandra_conn = BaseHook.get_connection(self.cassandra_conn_id)
        cassandra_engine = create_engine(cassandra_conn.get_uri())
        
        # Load data into Cassandra
        self.log.info("Loading data into Cassandra...")
        df.to_sql(self.table, cassandra_engine, if_exists='append', index=False)
        
        self.log.info("Data migration completed successfully!")