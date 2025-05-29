import pandas as pd
import json
import traceback 

def transform_replicated_data(data):
    try:
        if data is None:
            print("Empty data received")
            return pd.DataFrame() 

        if isinstance(data, str):
            print(f"Converting JSON string to DataFrame. String length: {len(data)}")
            print(f"First 100 chars: {data[:100] if data else 'Empty string'}")
            
            if not data or not data.strip():
                print("Empty or whitespace-only string received")
                return pd.DataFrame()
                
            try:
                data_dict = json.loads(data)
                df = pd.DataFrame(data_dict)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
                print(f"Raw data received: '{data}'")
                return pd.DataFrame()
            
        elif isinstance(data, pd.DataFrame):
            print("Data is already a DataFrame")
            df = data.copy()
            
        elif isinstance(data, list):
            print("Converting list to DataFrame")
            df = pd.DataFrame(data)
            
        else:
            print(f"Unexpected data type: {type(data)}")
            print(f"Data content: {data}")
            return pd.DataFrame()

        # Check if df is empty after creation
        if df.empty:
            print("Empty DataFrame created")
            return df
        
        print(f"Transforming DataFrame with columns: {df.columns.tolist()}")
        df.columns = df.columns.str.lower()
        
        cols_mapping = {
            # KhachHang table  
            'makhachhang': 'ma_khach_hang',
            'email': 'email',
            'hoten': 'ho_ten',
            'sdt': 'sdt',
            'diachi': 'dia_chi',
            'gioitinh': 'gioi_tinh',
            'ngaysinh': 'ngay_sinh',

            # SanPham table
            'masanpham': 'ma_san_pham',
            'tensanpham': 'ten_san_pham',
            'theloai': 'the_loai',
            'gia': 'gia',

            # ThuocTinh_SanPham table
            'tenthuoctinh': 'ten_thuoc_tinh',
            'giatrithuoctinh': 'gia_tri_thuoc_tinh',

            # DanhMuc_SanPham table
            'tendanhmuc': 'ten_danh_muc'
        }
        
        df.rename(columns=cols_mapping, inplace=True)
        
        if 'ngay_sinh' in df.columns:
            df['ngay_sinh'] = pd.to_datetime(df['ngay_sinh'], errors='coerce')
        
        # Convert numeric columns
        if 'gia' in df.columns:
            print("Converting gia to numeric")
            try:
                df['gia'] = pd.to_numeric(df['gia'], errors='coerce')
                df['gia'] = df['gia'].fillna(0).astype('int64')
            except Exception as e:
                print(f"Error converting gia column: {e}")

        if 'the_loai' in df.columns:
            print("Converting theloai to numeric")
            try:
                df['the_loai'] = pd.to_numeric(df['the_loai'], errors='coerce')
                df['the_loai'] = df['the_loai'].fillna(0).astype('int32')
            except Exception as e:
                print(f"Error converting theloai column: {e}")

        string_columns = ['email', 'ho_ten', 'sdt', 'dia_chi', 'gioi_tinh', 
                         'ma_san_pham', 'ten_san_pham', 'ten_thuoc_tinh', 
                         'gia_tri_thuoc_tinh', 'ten_danh_muc']
                         
        for col in string_columns:
            if col in df.columns:
                try:
                    df[col] = df[col].astype(str).str.strip()
                except Exception as e:
                    print(f"Error converting string column {col}: {e}")
                    
        return df

    except Exception as e:
        print(f"Error in transformation: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    
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