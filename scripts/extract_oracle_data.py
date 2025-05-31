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
            oracle_hook.get_conn() 
        )
        
        print(f"Successfully extracted {len(data)} rows from Oracle")
        if len(data) > 0:
            print(f"Columns: {data.columns.tolist()}")
            print(f"Sample data: {data.head(2).to_dict('records')}")
            
        return data
    except Exception as e: 
        print(f"Error extracting Oracle data: {e}")
        return pd.DataFrame()

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
    df = execute_query(invoice_query)
    return {
        'invoice_data': df
    }

def extract_revenue_branch_data():
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
    df = execute_query(revenue_query)
    return {
        'revenue_branch_data': df
    }

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
    df = execute_query(warehouse_query)
    return {
        'warehouse_data': df
    }

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
    df = execute_query(cus_query)
    return {
        'customer_data': df
    }
    
def extract_doanh_thu_sp_quy_cn():
    if not oracle_hook:
        print("Oracle hook not available for customer data")
        return pd.DataFrame()
    
    cus_query = """
        SELECT
          KQL."MaChiNhanh",
          SP."MaSanPham",
          EXTRACT(YEAR FROM HD."NgayTao") AS Nam,
          TO_NUMBER(TO_CHAR(HD."NgayTao", 'Q')) AS Quy,
          SUM(CT."ThanhTien") AS DoanhThu
        FROM
          "HoaDon" HD
          JOIN "ChiTietHoaDon" CT ON HD."MaHoaDon" = CT."MaHoaDon"
          JOIN "SanPham" SP ON SP."MaSanPham" = CT."MaSanPham"
          JOIN "NhanVien" NV ON NV."MaNhanVien" = HD."MaNhanVien"
          JOIN "ChiNhanh" KQL ON NV."MaChiNhanh" = KQL."MaChiNhanh"
        GROUP BY
          KQL."MaChiNhanh",
          SP."MaSanPham",
          EXTRACT(YEAR FROM HD."NgayTao"),
          TO_NUMBER(TO_CHAR(HD."NgayTao", 'Q'))
        ORDER BY
          KQL."MaChiNhanh", SP."MaSanPham", Nam, Quy
    """  
    
    print("Extracting doanh_thu_sp_quy_cn data...")
    df = execute_query(cus_query)
    return {
        'doanh_thu_sp_quy_cn_data': df
    }
    
def extract_doanh_thu_thang_nv_cn():
    if not oracle_hook:
        print("Oracle hook not available for customer data")
        return pd.DataFrame()
    
    cus_query = """
        SELECT
          NV."MaChiNhanh",
          NV."MaNhanVien",
          EXTRACT(YEAR FROM HD."NgayTao") AS Nam,
          EXTRACT(MONTH FROM HD."NgayTao") AS Thang,
          SUM(CT."ThanhTien") AS DoanhThu
        FROM
          "HoaDon" HD
          JOIN "ChiTietHoaDon" CT ON HD."MaHoaDon" = CT."MaHoaDon"
          JOIN "NhanVien" NV ON HD."MaNhanVien" = NV."MaNhanVien"
        GROUP BY
          NV."MaChiNhanh",
          NV."MaNhanVien",
          EXTRACT(YEAR FROM HD."NgayTao"),
          EXTRACT(MONTH FROM HD."NgayTao")
        ORDER BY
          NV."MaChiNhanh", NV."MaNhanVien", Nam, Thang
    """  
    
    print("Extracting doanh_thu_thang_nv_cn data...")
    df = execute_query(cus_query)
    return {
        'doanh_thu_thang_nv_cn_data': df
    }