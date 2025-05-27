from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def get_oracle_connection(user, password, host, port, service_name):
    """Establish a connection to the Oracle database."""
    try:
        dsn = f"oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service_name}"
        engine = create_engine(dsn)
        Session = sessionmaker(bind=engine)
        session = Session()
        return session
    except Exception as e:
        raise Exception(f"Error connecting to Oracle database: {e}")

def execute_query(session, query):
    """Execute a query on the Oracle database."""
    try:
        result = session.execute(query)
        return result.fetchall()
    except Exception as e:
        raise Exception(f"Error executing query: {e}")

def close_connection(session):
    """Close the Oracle database connection."""
    try:
        session.close()
    except Exception as e:
        raise Exception(f"Error closing the Oracle connection: {e}")