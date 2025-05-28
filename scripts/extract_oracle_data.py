from airflow.hooks.base import BaseHook 
from airflow.decorators import task 
import pandas as pd 
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import cx_Oracle 
import os 
import oracledb 

def extract_oracle_data():
    """Extract ALL data from Oracle at once"""
    
    try:
        print("🔄 Starting Oracle data extraction...")
        
        # Use oracledb (thin mode - no client required)
        connection = oracledb.connect(
            user='BTL1',
            password='password',
            dsn='26.93.36.133:1521/orcl'
        )
        print("✅ Connected to Oracle successfully")
        
        # Test the table first
        cursor = connection.cursor()
        
        # Try different ways to access the table
        table_queries = [
            'SELECT COUNT(*) FROM "BTL1"."KhoSanPham_QLBanHang"',
            'SELECT COUNT(*) FROM BTL1.KhoSanPham_QLBanHang',
            'SELECT COUNT(*) FROM "KhoSanPham_QLBanHang"',
            'SELECT COUNT(*) FROM KhoSanPham_QLBanHang'
        ]
        
        working_query = None
        for test_query in table_queries:
            try:
                print(f"📊 Testing query: {test_query}")
                cursor.execute(test_query)
                count = cursor.fetchone()[0]
                print(f"✅ Query works! Row count: {count}")
                working_query = test_query
                break
            except Exception as e:
                print(f"❌ Query failed: {e}")
                continue
        
        if working_query is None:
            print("❌ No working query found for the table")
            return pd.DataFrame()
            
        # Use the working table reference for the main query
        table_name = working_query.split(' FROM ')[1]
        
        query = f"""
        SELECT 
            "MaChiNhanh",
            "MaSanPham", 
            "TinhTrang",
            "NgayCapNhat",
            "TongSoLuongDaBan",
            "TongSoLuongDanhGia",
            "TongSoLuongSao"
        FROM {table_name}
        ORDER BY "MaChiNhanh", "MaSanPham"
        """
        
        print(f"📊 Executing main query: {query}")
        data = pd.read_sql(query, connection)
        print(f"✅ Successfully extracted {len(data)} rows from Oracle")
        
        if len(data) > 0:
            print(f"📊 Columns: {data.columns.tolist()}")
            print(f"📝 Sample data: {data.head(2).to_dict('records')}")
        
        return data
            
    except Exception as e:
        print(f"❌ Error extracting Oracle data: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    finally:
        if 'connection' in locals():
            connection.close()
            print("🔐 Oracle connection closed")
 
def transform_to_cassandra(oracle_data_chunk):
	try:
		transformed_data = oracle_data_chunk.copy()

		transformed_data.columns = transformed_data.columns.str.lower()
		cols_mapping = {
			'machinhanh': 'ma_chi_nhanh',
			'masanpham': 'ma_san_pham', 
			'tinhtrang': 'tinh_trang',
			'ngaycapnhat': 'ngay_cap_nhat',
			'tongsoluongdaban': 'tong_so_luong_da_ban',
			'tongsoluongdanhgia': 'tong_so_luong_danh_gia',
			'tongsoluongsao': 'tong_so_luong_sao'
		}
  
		transformed_data.rename(columns=cols_mapping, inplace=True) 
  
		if 'ngay_cap_nhat' in transformed_data.columns:
			transformed_data['ngay_cap_nhat'] = pd.to_datetime(
				transformed_data['ngay_cap_nhat'],
				errors='coerce' # invalid parsing will be set as NaN
			)

		numeric_columns = ['tong_so_luong_da_ban', 'tong_so_luong_danh_gia', 'tong_so_luong_sao']
		for col in numeric_columns:
			if col in transformed_data.columns:
				transformed_data[col] = pd.to_numeric(transformed_data[col], errors='coerce')
				transformed_data[col] = transformed_data[col].fillna(0).astype('int64')
		
		# Ensure string columns are properly formatted
		string_columns = ['ma_chi_nhanh', 'ma_san_pham', 'tinh_trang']
		for col in string_columns:
			if col in transformed_data.columns:
				transformed_data[col] = transformed_data[col].astype(str).str.strip()
		
		return transformed_data
	
	except Exception as e:
		print(f"Error: {e}")

def load_to_cassandra(transformed_data_chunk):
    """
    Load transformed data chunk into Cassandra
    """
    try:
        # Use direct connection to cassandra container (simpler approach)
        cluster = Cluster(
            ['cassandra'],  # Use container name
            port=9042,
            # Optional: Add cluster configuration
            protocol_version=4,
            connect_timeout=20,
            control_connection_timeout=20
        )
        session = cluster.connect()
        
        # Create keyspace if it doesn't exist
        keyspace_name = "etl_data"
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
            WITH REPLICATION = {{
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }}
        """)
        
        # Use your keyspace
        session.execute("USE etl_data")
        # Create table if it doesn't exist
        session.execute("""
            CREATE TABLE IF NOT EXISTS ban_hang_by_chi_nhanh (
                ma_chi_nhanh text,
                ma_san_pham text,
                tinh_trang text,
                ngay_cap_nhat timestamp,
                tong_so_luong_da_ban bigint,
                tong_so_luong_danh_gia bigint,
                tong_so_luong_sao bigint,
                PRIMARY KEY (ma_chi_nhanh, ma_san_pham)
            )
        """)
        
        # Prepare insert statement
        insert_query = """
        INSERT INTO ban_hang_by_chi_nhanh (
            ma_chi_nhanh, ma_san_pham, tinh_trang, ngay_cap_nhat,
            tong_so_luong_da_ban, tong_so_luong_danh_gia, tong_so_luong_sao
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        prepared_stmt = session.prepare(insert_query)
        
        # Insert data row by row
        for _, row in transformed_data_chunk.iterrows():
            session.execute(prepared_stmt, (
                row['ma_chi_nhanh'],
                row['ma_san_pham'],
                row['tinh_trang'],
                row['ngay_cap_nhat'],
                int(row['tong_so_luong_da_ban']),
                int(row['tong_so_luong_danh_gia']),
                int(row['tong_so_luong_sao'])
            ))
        
        print(f"Successfully inserted {len(transformed_data_chunk)} rows into Cassandra")
        
        # Close connection
        cluster.shutdown()
        
    except Exception as e:
        print(f"Error loading to Cassandra: {e}")
        raise e