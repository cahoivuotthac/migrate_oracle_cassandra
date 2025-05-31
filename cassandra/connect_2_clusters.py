from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import socket

def test_connection(host, port):
	try:
		sock = socket.create_connection((host, port), timeout=5)
		sock.close()
		return True
	except Exception as e:
		return f"Failed: {e}"

def connect_to_cluster(cluster_ip, keyspace_name):
	# auth_provider = PlainTextAuthProvider(username='congphan', password='password')
	try:
		# Test connection to teammate's machine
		print(test_connection(cluster_ip, 9042))
  
		cluster1 = Cluster(
			contact_points=[cluster_ip],
			# auth_provider=auth_provider,
			port=9042,
			protocol_version=4,
			connect_timeout=10,  # seconds
			control_connection_timeout=10  # seconds
		)

		session1 = cluster1.connect()
		print("Connected to cluster_1 successfully!")
		
		print("\nAttempting to connect to BTL2_data keyspace...")
		session1 = cluster1.connect(keyspace_name)
		print(f"Connected to {keyspace_name} successfully!")

		return cluster1, session1
	except Exception as e: 
		print(f"Connecting to another cluster failed: {e}")