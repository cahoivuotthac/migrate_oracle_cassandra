import pandas as pd
import json
import traceback 

def transform_data(df):
    try:
        if df is None or df.empty:
            print("Empty DataFrame received")
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
            df['ngaysinh'] = pd.to_datetime(df['ngaysinh'], errors='coerce')
        
        # Convert numeric columns
        if 'gia' in df.columns:
            print("Converting gia to numeric")
            try:
                df['gia'] = pd.to_numeric(df['gia'], errors='coerce')
                df['gia'] = df['gia'].fillna(0).astype('int64')
            except Exception as e:
                print(f"Error converting gia column: {e}")

        if 'theloai' in df.columns:
            print("Converting theloai to numeric")
            try:
                df['theloai'] = pd.to_numeric(df['theloai'], errors='coerce')
                df['theloai'] = df['theloai'].fillna(0).astype('int32')
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
        print(f"Error in transform_single_dataframe: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()