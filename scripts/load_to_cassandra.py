from contextlib import contextmanager
import pandas as pd
from cassandra.cluster import Cluster
import traceback
from cassandra.concurrent import execute_concurrent_with_args

BATCH_SIZE=1000

def get_cassandra_cluster():
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
 
@contextmanager
def get_cassandra_session():
	"""Context manager for Cassandra session"""
	cluster = None
	session = None
	try:
		cluster = get_cassandra_cluster()
		if cluster:
			session = cluster.connect()
			session.execute("USE BTL2_data")
			session.default_timeout = 60  # Increase timeout for large operations
			yield session
		else:
			yield None
	except Exception as e:
		print(f"Error with Cassandra session: {e}")
		yield None
	finally:
		if session:
			session.shutdown()
		if cluster:
			cluster.shutdown()

def load_data_in_batches(params_list, session, prepared, concurrency=25):
	try: 
		total_batches = len(params_list) // BATCH_SIZE + (1 if len(params_list) % BATCH_SIZE else 0)
				
		for i in range(0, len(params_list), BATCH_SIZE):
			batch_params = params_list[i:i + BATCH_SIZE]
			print(f"Processing batch {i//BATCH_SIZE + 1}/{total_batches}")
			
			results = execute_concurrent_with_args(
				session, 
				prepared, 
				batch_params,
				concurrency,
				raise_on_first_error=False
			)
			
			errors = []
			for j, (success, result) in enumerate(results):
				if not success:
					batch_index = i + j
					error_msg = str(result) if result else "Unknown error"
					print(f"Error at record {batch_index}: {error_msg}")
					if batch_index < len(params_list):
						print(f"Failed parameters: {params_list[batch_index]}")
					errors.append(result)
			
			if errors:
				print(f"Errors in batch: {len(errors)}")
				# Print first few errors for debugging
				for idx, error in enumerate(errors[:3]):  # Show first 3 errors
					print(f"Error {idx + 1}: {error}")
		
		print(f"Successfully processed {len(params_list)} categories to Cassandra")
	except Exception as e: 
		print(f"Error: {e}")

def load_user_data_optimized(user_data):
	try:
		print(f"Input data type: {type(user_data)}")
		
		if user_data.empty:
			print("Empty user data, nothing to load")
			return

		with get_cassandra_session() as session:
			if not session:
				print("No Cassandra session available")
				return

			insert_user_cql = """
				INSERT INTO BTL2_data.khachhang (
					ma_khach_hang, email, ho_ten, sdt, dia_chi, gioi_tinh, ngay_sinh
				) VALUES (?, ?, ?, ?, ?, ?, ?)
			"""
			prepared = session.prepare(insert_user_cql)
			
			# Prepare data for batch execution
			parameters_list = []
			for _, row in user_data.iterrows():
				params = [
					row['ma_khach_hang'],
					row['email'],
					row['ho_ten'],
					row['sdt'],
					row['dia_chi'],
					row['gioi_tinh'],
					row['ngay_sinh']
				]
				parameters_list.append(params)
			
			load_data_in_batches(parameters_list, session, prepared)	

			print(f"Successfully processed {len(parameters_list)} users to Cassandra")
		
	except Exception as e:
		print(f"Error loading users to Cassandra: {e}")
		traceback.print_exc()

def load_product_data_optimized(product_data):
	print(f"product_data of type {type(product_data)}")
	print(f"product_data shape: {product_data.shape if hasattr(product_data, 'shape') else 'No shape'}")
	
	if product_data.empty:
		print("Empty product data, nothing to load")
		return

	try:
		with get_cassandra_session() as session:
			if not session:
				print("No Cassandra session available")
				return

			insert_product_cql = """
				INSERT INTO BTL2_data.sanpham (
					ma_san_pham, ten_san_pham, the_loai, gia
				) VALUES (?, ?, ?, ?)
			"""
			prepared = session.prepare(insert_product_cql)
			
			parameters_list = []
			for _, row in product_data.iterrows():
				params = [
					row['ma_san_pham'],
					row['ten_san_pham'],
					row['the_loai'],
					row['gia']
				]
				parameters_list.append(params)
			
			load_data_in_batches(parameters_list, session, prepared)	
			
			print(f"Successfully processed {len(parameters_list)} products to Cassandra")
	except Exception as e:
		print(f"Error loading products to Cassandra: {e}")
		traceback.print_exc()

def load_attr_product_data_optimized(attr_product_data):
	print(f"attr_product_data of type {type(attr_product_data)}")
	print(f"attr_product_data shape: {attr_product_data.shape if hasattr(attr_product_data, 'shape') else 'No shape'}")
	
	if attr_product_data.empty:
		print("Empty attribute data, nothing to load")
		return

	try:
		with get_cassandra_session() as session:
			if not session:
				print("No Cassandra session available")
				return

			insert_attr_cql = """
				INSERT INTO BTL2_data.thuoctinh_sanpham (
					ma_san_pham, ten_thuoc_tinh, gia_tri_thuoc_tinh
				) VALUES (?, ?, ?)
			"""
			prepared = session.prepare(insert_attr_cql)
			
			parameters_list = []
			for _, row in attr_product_data.iterrows():
				params = [
					row['ma_san_pham'],
					row['ten_thuoc_tinh'],
					row['gia_tri_thuoc_tinh']
				]
				parameters_list.append(params)
			
			load_data_in_batches(parameters_list, session, prepared)		
			
			print(f"Successfully processed {len(parameters_list)} attributes to Cassandra")
	except Exception as e:
		print(f"Error loading attributes to Cassandra: {e}")
		traceback.print_exc()

def load_cat_product_data_optimized(cat_product_data):
	print(f"cat_product_data of type {type(cat_product_data)}")
	print(f"cat_product_data shape: {cat_product_data.shape if hasattr(cat_product_data, 'shape') else 'No shape'}")
	
	if cat_product_data.empty:
		print("Empty category data, nothing to load")
		return

	try:
		with get_cassandra_session() as session:
			if not session:
				print("No Cassandra session available")
				return

			insert_cat_cql = """
				INSERT INTO BTL2_data.danhmuc_sanpham (
					ma_san_pham, ten_danh_muc
				) VALUES (?, ?)
			"""
			prepared = session.prepare(insert_cat_cql)
			
			parameters_list = []
			for _, row in cat_product_data.iterrows():
				params = [
					row['ma_san_pham'],
					row['ten_danh_muc']
				]
				parameters_list.append(params)
			
			load_data_in_batches(parameters_list, session, prepared)	
			
			print(f"Successfully processed {len(parameters_list)} categories to Cassandra")
	except Exception as e:
		print(f"Error loading categories to Cassandra: {e}")
		traceback.print_exc()

def load_invoice_details_data_optimized(invoice_data):
	print(f"invoice_data of type {type(invoice_data)}")
	if not isinstance(invoice_data, pd.DataFrame):
		print(f"Expected DataFrame, got {type(invoice_data)}")
		return
	
	if invoice_data.empty:
		print('Empty input data, nothing to load')
		return 
	
	try:
		with get_cassandra_session() as session:
			if not session:
				print("No Cassandra session available")
				return

			prepared_stmt = session.prepare("""
				INSERT INTO BTL2_data.chi_tiet_hoa_don_theo_ma_kh (
					ma_khach_hang, ma_hoa_don, ma_san_pham, so_luong,
					thanh_tien, tong_tien, ngay_tao, phuong_thuc_thanh_toan, ma_nhan_vien
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			""")

			params_list = []
			for _, row in invoice_data.iterrows():
				params = [
					row['ma_khach_hang'],
					row['ma_hoa_don'],
					row['ma_san_pham'],
					row['so_luong'],
					row['thanh_tien'],
					row['tong_tien'], 
					pd.to_datetime(row['ngay_tao']).to_pydatetime(),
					row['phuong_thuc_thanh_toan'],
					row['ma_nhan_vien']
				]
				params_list.append(params)

			load_data_in_batches(params_list, session, prepared_stmt)
		
	except Exception as e:
		print(f"Error loading categories to Cassandra: {e}")
		traceback.print_exc()
  
def load_revenue_data_optimized(revenue_data):
	print(f"revenue_data of type {type(revenue_data)}")
	if not isinstance(revenue_data, pd.DataFrame):
		print(f"Expected DataFrame, got {type(revenue_data)}")
		return

	if revenue_data.empty:
		print('Empty input data, nothing to load')
		return 
	
	try:
		with get_cassandra_session() as session:
			if not session:
				print("No Cassandra session available")
				return

			prepared_stmt = session.prepare("""
				INSERT INTO BTL2_data.doanh_thu_moi_ngay_theo_ma_cn (
					ma_chi_nhanh,
					ngay,
					tong_tien
				) VALUES (?, ?, ?)
			""")

			params_list = []
			for _, row in revenue_data.iterrows():
				params = [
					row['ma_chi_nhanh'],
					row['ngay'],
					row['tong_tien']
				]
				params_list.append(params)

			load_data_in_batches(params_list, session, prepared_stmt)
		
	except Exception as e:
		print(f"Error loading to Cassandra: {e}")
		traceback.print_exc()
  
def load_wh_data_optimized(wh_data):
	print(f"wh_data of type {type(wh_data)}")
	if not isinstance(wh_data, pd.DataFrame):
		print(f"Expected DataFrame, got {type(wh_data)}")
		return
	
	if wh_data.empty:
		print('Empty input data, nothing to load')
		return 
	
	try:
		with get_cassandra_session() as session:
			if not session:
				print("No Cassandra session available")
				return

			prepared_stmt = session.prepare("""
				INSERT INTO BTL2_data.kho_sp_theo_ma_cn (
					ma_chi_nhanh,
					ma_san_pham,
					ten_san_pham,
					tinh_trang,
					tong_so_luong_danh_gia,
					tong_so_luong_da_ban,
					tong_so_luong_ton_kho
				) VALUES (?, ?, ?, ?, ?, ?, ?)
			""")

			params_list = []
			for _, row in wh_data.iterrows():
				params = [
					row['ma_chi_nhanh'],
					row['ma_san_pham'],
					row['ten_san_pham'],
					row['tinh_trang'],
					row['tong_so_luong_danh_gia'],
					row['tong_so_luong_da_ban'],
					row['tong_so_luong_ton_kho']
				]
				params_list.append(params)

			load_data_in_batches(params_list, session, prepared_stmt)
		
	except Exception as e:
		print(f"Error loading to Cassandra: {e}")
		traceback.print_exc()
 
def load_cus_data_optimized(cus_data):
	print(f"user_data of type {type(cus_data)}")
	if not isinstance(cus_data, pd.DataFrame):
		print(f"Expected DataFrame, got {type(cus_data)}")
		return
	
	if cus_data.empty:
		print('Empty input data, nothing to load')
		return 
	
	try:
		with get_cassandra_session() as session:
			if not session:
				print("No Cassandra session available")
				return

			prepared_stmt = session.prepare("""
				INSERT INTO BTL2_data.sl_khach_hang_moi_ngay_theo_ma_cn (
					ma_chi_nhanh,
					ngay,
					so_luong_khach_hang
				) VALUES (?, ?, ?)
			""")

			params_list = []
			for _, row in cus_data.iterrows():
				params = [
					row['ma_chi_nhanh'],
					row['ngay'],
					row['so_luong_khach_hang']
				]
				params_list.append(params)

			load_data_in_batches(params_list, session, prepared_stmt)
		
	except Exception as e:
		print(f"Error loading to Cassandra: {e}")
		traceback.print_exc()
  
def load_doanhthu_sp_data_optimized(dt_sp_data):
	print(f"user_data of type {type(dt_sp_data)}")
	if not isinstance(dt_sp_data, pd.DataFrame):
		print(f"Expected DataFrame, got {type(dt_sp_data)}")
		return
	
	if dt_sp_data.empty:
		print('Empty input data, nothing to load')
		return 
	
	try:
		with get_cassandra_session() as session:
			if not session:
				print("No Cassandra session available")
				return

			prepared_stmt = session.prepare("""
				INSERT INTO BTL2_data.doanh_thu_sp_quy_cn (
				  ma_chi_nhanh, 
				  ma_san_pham, 
				  nam, 
				  quy, 
				  tong_doanh_thu
				) VALUES (?, ?, ?, ?, ?);
			""")

			params_list = []
			for _, row in dt_sp_data.iterrows():
				params = [
					row['ma_chi_nhanh'],
					row['ma_san_pham'],
					row['nam'],
					row['quy'],
					row['tong_doanh_thu']
				]
				params_list.append(params)

			load_data_in_batches(params_list, session, prepared_stmt)
		
	except Exception as e:
		print(f"Error loading to Cassandra: {e}")
		traceback.print_exc()  

def load_doanhthu_nv_data_optimized(dt_nv_data):
	print(f"user_data of type {type(dt_nv_data)}")
	if not isinstance(dt_nv_data, pd.DataFrame):
		print(f"Expected DataFrame, got {type(dt_nv_data)}")
		return
	
	if dt_nv_data.empty:
		print('Empty input data, nothing to load')
		return 
	
	try:
		with get_cassandra_session() as session:
			if not session:
				print("No Cassandra session available")
				return

			prepared_stmt = session.prepare("""
				INSERT INTO BTL2_data.doanh_thu_thang_nv_cn (
                  ma_chi_nhanh,
                  ma_nhan_vien,
                  nam,
                  thang,
                  tong_doanh_thu
                ) VALUES (?, ?, ?, ?, ?)
			""")

			params_list = []
			for _, row in dt_nv_data.iterrows():
				params = [
					row['ma_chi_nhanh'],
					row['ma_nhan_vien'],
					row['nam'],
					row['thang'],
					row['tong_doanh_thu']
				]
				params_list.append(params)

			load_data_in_batches(params_list, session, prepared_stmt)
		
	except Exception as e:
		print(f"Error loading to Cassandra: {e}")
		traceback.print_exc()  