import pandas as pd
import json
import traceback 

def transform_data(input_data, k_name, num_cols, str_cols, cols_mapping):
    try: 
        if not input_data or not isinstance(input_data, dict):
            print("Invalid separated data received")
            return {}
        transformed_df = {}
        
def transform_branch_data(separated_data):
    try:
        if not separated_data or not isinstance(separated_data, dict):
            print("Invalid separated data received")
            return {}
        
        transformed_result = {}
        
        # Transform invoice data
        if 'invoice_data' in separated_data and not separated_data['invoice_data'].empty:
            invoice_df = separated_data['invoice_data'].copy()
            invoice_df.columns = invoice_df.columns.str.lower()
            
            invoice_mapping = {
                'makhachhang': 'ma_khach_hang',
                'mahoadon': 'ma_hoa_don',
                'masanpham': 'ma_san_pham',
                'soluong': 'so_luong',
                'thanhtien': 'thanh_tien',
                'tongtien': 'tong_tien',
                'ngaytao': 'ngay_tao',
                'phuongthucthanhtoan': 'phuong_thuc_thanh_toan',
                'manhanvien': 'ma_nhan_vien'
            }
            
            invoice_df.rename(columns=invoice_mapping, inplace=True)
            
            if 'ngay_tao' in invoice_df.columns:
                invoice_df['ngay_tao'] = pd.to_datetime(invoice_df['ngay_tao'], errors='coerce')
            
            numeric_cols = [
                'so_luong', 
                'thanh_tien', 
                'tong_tien', 
                'ma_khach_hang',
                'ma_hoa_don'
            ]
            for col in numeric_cols:
                if col in invoice_df.columns:
                    invoice_df[col] = pd.to_numeric(invoice_df[col], errors='coerce').fillna(0)
            
            transformed_result['invoice_data'] = invoice_df
        
        # Transform revenue data
        if 'revenue_data' in separated_data and not separated_data['revenue_data'].empty:
            revenue_df = separated_data['revenue_data'].copy()
            revenue_df.columns = revenue_df.columns.str.lower()
            
            revenue_mapping = {
                'machinhanh': 'ma_chi_nhanh',
                'ngay': 'ngay',
                'tongtien': 'tong_tien'
            }
            
            revenue_df.rename(columns=revenue_mapping, inplace=True)
            
            if 'ngay' in revenue_df.columns:
                revenue_df['ngay'] = pd.to_datetime(revenue_df['ngay'], errors='coerce')
            
            if 'tong_tien' in revenue_df.columns:
                revenue_df['tong_tien'] = pd.to_numeric(revenue_df['tong_tien'], errors='coerce').fillna(0)
            
            transformed_result['revenue_data'] = revenue_df
        
        # Transform warehouse data
        if 'warehouse_data' in separated_data and not separated_data['warehouse_data'].empty:
            warehouse_df = separated_data['warehouse_data'].copy()
            warehouse_df.columns = warehouse_df.columns.str.lower()
            
            warehouse_mapping = {
                'machinhanh': 'ma_chi_nhanh',
                'masanpham': 'ma_san_pham',
                'tensanpham': 'ten_san_pham',
                'tinhtrang': 'tinh_trang',
                'tongsoluongdanhgia': 'tong_so_luong_danh_gia',
                'tongsoluongdaban': 'tong_so_luong_da_ban',
                'soluong': 'tong_so_luong_ton_kho'
            }
            
            warehouse_df.rename(columns=warehouse_mapping, inplace=True)
            
            numeric_cols = ['tong_so_luong_danh_gia', 'tong_so_luong_da_ban', 'tong_so_luong_ton_kho']
            for col in numeric_cols:
                if col in warehouse_df.columns:
                    warehouse_df[col] = pd.to_numeric(warehouse_df[col], errors='coerce').fillna(0)
            
            string_cols = ['ten_san_pham', 'tinh_trang']
            for col in string_cols:
                if col in warehouse_df.columns:
                    warehouse_df[col] = warehouse_df[col].astype(str).str.strip()
            
            transformed_result['warehouse_data'] = warehouse_df
        
        # Transform customer data
        if 'cus_data' in separated_data and not separated_data['cus_data'].empty:
            cus_df = separated_data['cus_data'].copy()
            cus_df.columns = cus_df.columns.str.lower()
            
            cus_mapping = {
                'machinhanh': 'ma_chi_nhanh',
                'ngay': 'ngay',
                'soluongkhachhang': 'so_luong_khach_hang'
            }
            
            cus_df.rename(columns=cus_mapping, inplace=True)
            
            if 'ngay' in cus_df.columns:
                cus_df['ngay'] = pd.to_datetime(cus_df['ngay'], errors='coerce')
            
            if 'so_luong_khach_hang' in cus_df.columns:
                cus_df['so_luong_khach_hang'] = pd.to_numeric(cus_df['so_luong_khach_hang'], errors='coerce').fillna(0)
            
            transformed_result['cus_data'] = cus_df
        
        print(f"Transformed separated data with {len(transformed_result)} datasets")
        return transformed_result
    
    except Exception as e:
        print(f"Error transforming separated data: {e}")
        import traceback
        traceback.print_exc()
        return {}