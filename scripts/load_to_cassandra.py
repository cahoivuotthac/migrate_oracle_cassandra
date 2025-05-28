import pandas as pd
from cassandra.cluster import Cluster
import traceback

def get_cassandra_hook():
	try: 
		cluster = Cluster(
			['cassandra'],  # Use container name
			port=9042,
			protocol_version=4,
			connect_timeout=20,
			control_connection_timeout=20
		)
		return cluster 
	
	except Exception as e:
		print(f"Error when getting Cassandra hook: {e}")
		return None
  
try:
	cluster = get_cassandra_hook()
	if cluster:
		session = cluster.connect()
		session.execute("USE etl_data")
	else:
		session = None
except Exception as e:
	print(f"Error connecting to Cassandra: {e}")
	session = None

def load_user_data(user_data):	
	try:
		print(f"user_data type: {type(user_data)}")
  
		if not session:
			print("No Cassandra session available")
			return

		insert_user_cql = """
			INSERT INTO khachhang (
				ma_khach_hang, email, ho_ten, sdt, dia_chi, gioi_tinh, ngay_sinh
			) VALUES (?, ?, ?, ?, ?, ?, ?)
		"""

		count = 0
		for _, row in user_data.iterrows():
			try:
				session.execute(insert_user_cql, (
					row['ma_khach_hang'],
					row['email'],
					row['ho_ten'],
					row['sdt'],
					row['dia_chi'],
					row['gioi_tinh'],
					row['ngay_sinh']
				))
				count += 1
	
			except Exception as e:
				print(f"Error inserting user: {e}")
				print(f"Problematic row: {row.to_dict()}")

		print(f"Successfully loaded {count} users to Cassandra")
		
	except Exception as e:
		print(f"Error loading users to Cassandra: {e}")
		traceback.print_exc()
  
def load_product_data(product_data):
	print(f"product_data of type {type(product_data)}")
	
	if product_data.empty:
		print("Empty product data, nothing to load")
		return
		
	if not session:
		print("No Cassandra session available")
		return

	try:
		print(f"Product data columns: {product_data.columns.tolist()}")
		
		insert_product_cql = """
			INSERT INTO sanpham (
				ma_san_pham, ten_san_pham, the_loai, gia
			) VALUES (?, ?, ?, ?)
		"""
		
		count = 0
		for _, row in product_data.iterrows():
			try:
				
				session.execute(insert_product_cql, [
					row['ma_san_pham'],
					row['ten_san_pham'],
					row['the_loai'],
					row['gia']
				])
				count += 1
			except Exception as e:
				print(f"Error inserting product: {e}")
		
		print(f"Successfully loaded {count} products to Cassandra")
	except Exception as e:
		print(f"Error loading products to Cassandra: {e}")
		traceback.print_exc()
  
def load_attr_product_data(attr_product_data):
	print(f"attr_product_data of type {type(attr_product_data)}")
	
	if attr_product_data.empty:
		print("Empty attribute data, nothing to load")
		return
		
	if not session:
		print("No Cassandra session available")
		return
 
	try:
		print(f"Attribute data columns: {attr_product_data.columns.tolist()}")
		
		insert_attr_cql = """
			INSERT INTO thuoctinh_sanpham (
				ma_san_pham, ten_thuoc_tinh, gia_tri_thuoc_tinh
			) VALUES (?, ?, ?)
		"""
		
		count = 0
		for _, row in attr_product_data.iterrows():
			try:
				
				session.execute(insert_attr_cql, [
					row['ma_san_pham'],
					row['ten_thuoc_tinh'],
					row['gia_tri_thuoc_tinh']
				])
				count += 1
			except Exception as e:
				print(f"Error inserting attribute: {e}")
		
		print(f"Successfully loaded {count} attributes to Cassandra")
	except Exception as e:
		print(f"Error loading attributes to Cassandra: {e}")
		traceback.print_exc()
  
def load_cat_product_data(cat_product_data):
	print(f"cat_product_data of type {type(cat_product_data)}")
	
	if cat_product_data.empty:
		print("Empty category data, nothing to load")
		return
		
	if not session:
		print("No Cassandra session available")
		return
 
	try:
		print(f"Category data columns: {cat_product_data.columns.tolist()}")
		
		insert_cat_cql = """
			INSERT INTO danhmuc_sanpham (
				ma_san_pham, ten_danh_muc
			) VALUES (?, ?)
		"""
  
		count = 0
		for _, row in cat_product_data.iterrows():
			try:
				
				session.execute(insert_cat_cql, [
					row['ma_san_pham'],
					row['ten_danh_muc']
				])
				count += 1
			except Exception as e:
				print(f"Error inserting category: {e}")
		
		print(f"Successfully loaded {count} categories to Cassandra")
	except Exception as e:
		print(f"Error loading categories to Cassandra: {e}")
		traceback.print_exc()