from cassandra.connect_2_clusters import connect_to_cluster

cluster_ip = '26.103.246.194'
keyspace_name = 'btl2_data'

try: 
	cluster, session = connect_to_cluster(cluster_ip, keyspace_name)
	
	query = """
		SELECT * FROM doanh_thu_moi_ngay_theo_ma_cn
		WHERE ma_chi_nhanh = 1;
 	"""
  
	rows = session.execute(query)
	if not rows:
		print("No results found")
	else:
		for row in rows:
			print(row)
   
	print("======================================")

except Exception as e: 
	print(f"Error when doing queries: {e}")
 
finally:
    # Always close connections
    if 'cluster' in locals():
        cluster.shutdown()
