import pandas as pd
import json
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
        data = pd.read_sql(
            query, 
            oracle_hook.get_conn(),
            # encoding='utf-8'
        )
        
        print(f"Successfully extracted {len(data)} rows from Oracle")
        if len(data) > 0:
            print(f"Columns: {data.columns.tolist()}")
            print(f"Sample data: {data.head(2).to_dict('records')}")
            
        return data
    except Exception as e: 
        print(f"Error extracting Oracle data: {e}")
        return pd.DataFrame()

def extract_replicated_data():
    
    if not oracle_hook:
        print("Oracle hook not available for invoice data")
        return pd.DataFrame()
    
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

def extract_invoice_data():
    if not oracle_hook:
        print("Oracle hook not available for invoice data")
        return pd.DataFrame()
    
    invoice_query = """
        SELECT 
            hd."MaKhachHang",
            cthd."MaHoaDon",
            cthd."MaSanPham",
            cthd."SoLuong",
            cthd."ThanhTien",
            hd."TongTien",
            hd."NgayTao",
            hd."PhuongThucThanhToan",
            hd."MaNhanVien"
        FROM "BTL1"."ChiTietHoaDon" cthd 
            JOIN "BTL1"."HoaDon" hd ON cthd."MaHoaDon" = hd."MaHoaDon"
            JOIN "BTL1"."KhachHang" kh ON hd."MaKhachHang" = kh."MaKhachHang"
        ORDER BY cthd."MaHoaDon", hd."NgayTao" DESC
    """
    
    print("Extracting invoice data...")
    return execute_query(invoice_query)

def extract_revenue_data():
    if not oracle_hook:
        print("Oracle hook not available for revenue data")
        return pd.DataFrame()
    
    revenue_query = """
        SELECT 
            cn."MaChiNhanh",
            TRUNC(hd."NgayTao") AS "Ngay",
            SUM(hd."TongTien") AS "TongTien"
        FROM "BTL1"."HoaDon" hd 
            JOIN "BTL1"."NhanVien" nv ON hd."MaNhanVien" = nv."MaNhanVien"
            JOIN "BTL1"."ChiNhanh" cn ON cn."MaChiNhanh" = nv."MaChiNhanh"
        GROUP BY cn."MaChiNhanh", TRUNC(hd."NgayTao")
        ORDER BY TRUNC(hd."NgayTao") DESC
    """
    
    print("Extracting revenue data...")
    return execute_query(revenue_query)

def extract_warehouse_data():
    if not oracle_hook:
        print("Oracle hook not available for warehouse data")
        return pd.DataFrame()
    
    warehouse_query = """
        SELECT 
            kspq."MaChiNhanh",
            kspq."MaSanPham",
            sp."TenSanPham",
            kspqh."TinhTrang",
            kspqh."TongSoLuongDanhGia",
            kspqh."TongSoLuongDaBan",
            kspq."SoLuong"
        FROM "BTL1"."KhoSanPham_QLBanHang" kspqh, 
             "BTL1"."KhoSanPham_QLKho" kspq, 
             "BTL1"."SanPham" sp
        WHERE  kspqh."MaSanPham" = kspq."MaSanPham" 
          AND sp."MaSanPham" =  kspq."MaSanPham"
        ORDER BY  kspq."MaSanPham",  kspq."SoLuong"
    """
    
    print("Extracting warehouse data...")
    return execute_query(warehouse_query)

def extract_customer_data():
    if not oracle_hook:
        print("Oracle hook not available for customer data")
        return pd.DataFrame()
    
    cus_query = """
        SELECT 
          nv."MaChiNhanh",
          TRUNC(hd."NgayTao") AS Ngay,
          COUNT(DISTINCT hd."MaKhachHang") AS SoLuongKhachHang
        FROM "BTL1"."NhanVien" nv
          JOIN "BTL1"."HoaDon" hd ON nv."MaNhanVien" = hd."MaNhanVien"
        GROUP BY nv."MaChiNhanh", TRUNC(hd."NgayTao")
        ORDER BY Ngay DESC
    """
    
    print("Extracting customer data...")
    return execute_query(cus_query)

def extract_branch_data():
    try: 
        print("Starting separated data extraction...")
        
        result = {
            'invoice_data': extract_invoice_data(),
            'revenue_data': extract_revenue_data(),
            'warehouse_data': extract_warehouse_data(),
            'cus_data': extract_customer_data()
        }
        
        print("All data extraction completed successfully")
        return result
    
    except Exception as e: 
        print(f"Error during branch daa extraction: {e}")
        return {
            'invoice_data': pd.DataFrame(),
            'revenue_data': pd.DataFrame(),
            'warehouse_data': pd.DataFrame(),
            'cus_data': pd.DataFrame()
        }
