from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def create_cassandra_connection(host='localhost', port=9042, username=None, password=None):
    """
    Establish a connection to the Cassandra database.

    :param host: Cassandra host
    :param port: Cassandra port
    :param username: Username for authentication
    :param password: Password for authentication
    :return: Cassandra session
    """
    auth_provider = PlainTextAuthProvider(username, password) if username and password else None
    cluster = Cluster([host], port=port, auth_provider=auth_provider)
    session = cluster.connect()
    return session

def close_cassandra_connection(session):
    """
    Close the Cassandra session.

    :param session: Cassandra session to close
    """
    if session:
        session.shutdown()