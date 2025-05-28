import pandas as pd
import json
import traceback 

def transform_data(data):
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
            # KhachHang table - keep names matching Cassandra schema
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