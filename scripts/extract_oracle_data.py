import pandas as pd
import json
from datetime import datetime
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.configuration import conf

# Enable pickle for XCom serialization
conf.set('core', 'enable_xcom_pickling', 'True')

def get_oracle_hook():
    try: 
        return OracleHook(oracle_conn_id='oracle_BTL2')
    except Exception as e:
        print(f"Error when getting oracle hook: {e}")

oracle_hook = get_oracle_hook()

def execute_query(query):
    try: 
        data = pd.read_sql(query, oracle_hook.get_conn())
        print(f"Successfully extracted {len(data)} rows from Oracle")
        if len(data) > 0:
            print(f"Columns: {data.columns.tolist()}")
            print(f"Sample data: {data.head(2).to_dict('records')}")
            
        return data
    except Exception as e: 
        print(f"Error extracting Oracle data: {e}")
        return pd.DataFrame()

def extract_replicated_data():
    user_data = pd.DataFrame()
    product_data = pd.DataFrame()
    attr_product_data = pd.DataFrame()
    cat_product_data = pd.DataFrame()
    
    if oracle_hook:
        user_query = """
            SELECT 
                "MaKhachHang",
                "Email",
                "HoTen",
                "SDT",
                "DiaChi",
                "GioiTinh",
                "NgaySinh"
            FROM "BTL1"."KhachHang"
        """ 

        product_query = """
            SELECT 
                "MaSanPham",
                "TenSanPham",
                "TheLoai",
                "Gia"
            FROM "BTL1"."SanPham"
        """
    
        attr_product_query = """
            SELECT 
                "MaSanPham",
                "TenThuocTinh",
                "GiaTriThuocTinh"
            FROM "BTL1"."ThuocTinh_SanPham"
        """
     
        cat_product_query = """
            SELECT 
                "MaSanPham",
                "TenDanhMuc"
            FROM "BTL1"."DanhMuc_SanPham"
        """
    
        user_data = execute_query(user_query)
        product_data = execute_query(product_query)
        attr_product_data = execute_query(attr_product_query)
        cat_product_data = execute_query(cat_product_query)
  
    result = {
        'user_data': user_data,
        'product_data': product_data,
        'attr_product_data': attr_product_data,
        'cat_product_data': cat_product_data
    }
  
    return result