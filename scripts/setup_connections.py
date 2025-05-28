import os
import yaml
from airflow import settings
from airflow.models import Connection
from airflow.utils.db import provide_session

@provide_session
def create_connection(conn_id, conn_type, host, schema, login, password, port, session=None):
	existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
	
	if existing_conn:
		print(f"Connection {conn_id} already exists. Skipping creation.")
		return
	
	conn = Connection(
		conn_id=conn_id,
		conn_type=conn_type,
		host=host,
		schema=schema,
		login=login,
		password=password,
		port=port
	)
	session.add(conn)
	session.commit()
	print(f"Connection {conn_id} created.")

def setup_connections():
	config_path = os.path.join(os.path.dirname(__file__), '..', 'dags/config', 'connections.yaml')
	with open(config_path, 'r') as file:
		config = yaml.safe_load(file)
		
	oracle_config = config['connections']['oracle_db']
	cassandra_config = config['connections']['cassandra_db']
 
	create_connection(
		conn_id=oracle_config['conn_id'],
		conn_type=oracle_config['conn_type'],
		host=oracle_config['host'],
		schema=oracle_config['schema'],
		login=oracle_config['login'],
		password=oracle_config['password'],
		port=oracle_config['port']
	)

	# create_connection(
	# 	conn_id=cassandra_config['conn_id'],
	# 	host='cassandra',
	# 	schema=cassandra_config['schema'],
	# 	login='',
	# 	password='',
	# 	port=cassandra_config['port']
	# )

if __name__ == "__main__":
	setup_connections()