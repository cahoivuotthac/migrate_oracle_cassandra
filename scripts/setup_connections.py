import os
from airflow import settings
from airflow.models import Connection
from airflow.utils.db import provide_session

@provide_session
def create_connection(conn_id, conn_type, host, schema, login, password, port, session=None):
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
    # Oracle connection
    create_connection(
        conn_id='oracle_conn',
        conn_type='oracle',
        host='26.93.36.133',
        schema='orcl',
        login='BTL1',
        password='password',
        port=1521
    )

    # Cassandra connection
    create_connection(
        conn_id='cassandra_conn',
        conn_type='cassandra',
        host='localhost',
        schema='etl_data',
        login='',
        password='',
        port=9042
    )

if __name__ == "__main__":
    setup_connections()