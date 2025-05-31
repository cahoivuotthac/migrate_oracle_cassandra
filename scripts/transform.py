import pandas as pd
import traceback 

def transform_data(input_data, k_name, num_cols, str_cols, datetime_cols, cols_mapping):
	try: 
		if input_data is None or not isinstance(input_data, dict):
			print("Invalid separated data received")
			return {}
		
		transformed_df = {}
		
		if k_name in input_data:
			tmp_df = pd.DataFrame(input_data[k_name].copy())
			tmp_df.columns = tmp_df.columns.str.lower()
			
			tmp_df.rename(columns=cols_mapping, inplace=True)
			
			if datetime_cols: 
				for col in datetime_cols:
					if col in tmp_df.columns:
						tmp_df[col] = pd.to_datetime(tmp_df[col], errors='coerce')
						
			if num_cols:
				for col in num_cols:
					if col in tmp_df.columns:
						tmp_df[col] = pd.to_numeric(tmp_df[col], errors='coerce').fillna(0)
			
			if str_cols:        
				for col in str_cols:
					if col in tmp_df.columns:
						tmp_df[col] =  tmp_df[col].astype(str).str.strip()
					
			transformed_df[k_name] = tmp_df
		
		return transformed_df
	
	except Exception as e: 
		print(f"Error transforming {k_name} data: {e}")
		import traceback
		traceback.print_exc()
		return {}

def transform_invoice_data(invoice_df):
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
	
	datetime_cols = ['ngay_tao']
	num_cols = [
		'so_luong', 
		'thanh_tien', 
		'tong_tien', 
		'ma_khach_hang',
		'ma_hoa_don'
	] 
	
	return transform_data(invoice_df, 'invoice_data', num_cols, None, datetime_cols, invoice_mapping)

def transform_revenue_branch_data(revenue_df):
	revenue_mapping = {
		'machinhanh': 'ma_chi_nhanh',
		'ngay': 'ngay',
		'tongtien': 'tong_tien'
	}
	
	datetime_cols = ['ngay']
	num_cols = ['tong_tien']
	
	return transform_data(revenue_df, 'revenue_branch_data', num_cols, None, datetime_cols, revenue_mapping)

def transform_warehouse_data(warehouse_df):
	warehouse_mapping = {
		'machinhanh': 'ma_chi_nhanh',
		'masanpham': 'ma_san_pham',
		'tensanpham': 'ten_san_pham',
		'tinhtrang': 'tinh_trang',
		'tongsoluongdanhgia': 'tong_so_luong_danh_gia',
		'tongsoluongdaban': 'tong_so_luong_da_ban',
		'soluong': 'tong_so_luong_ton_kho'
	}
	
	num_cols = ['tong_so_luong_danh_gia', 'tong_so_luong_da_ban', 'tong_so_luong_ton_kho']
	str_cols = ['ten_san_pham', 'tinh_trang']
	
	return transform_data(warehouse_df, 'warehouse_data', num_cols, str_cols, None, warehouse_mapping)

def transform_customer_data(customer_df):
	customer_mapping = {
		'machinhanh': 'ma_chi_nhanh',
		'ngay': 'ngay',
		'soluongkhachhang': 'so_luong_khach_hang'
	}
	
	datetime_cols = ['ngay']
	num_cols = ['so_luong_khach_hang']
	
	return transform_data(customer_df, 'customer_data', num_cols, None, datetime_cols, customer_mapping)

def transform_revenue_sp_quy_cn(doanhthu_sp_df):
	cols_mapping = {
		'machinhanh': 'ma_chi_nhanh',
		'masanpham': 'ma_san_pham',
		'nam': 'nam',
		'quy': 'quy',
		'doanhthu': 'tong_doanh_thu'
	}
	
	num_cols = ['nam', 'quy', 'ma_chi_nhanh', 'tong_doanh_thu']
	str_cols = ['ma_san_pham']
	
	if isinstance(doanhthu_sp_df, pd.DataFrame):
		input_dict = {'doanh_thu_sp_quy_cn': doanhthu_sp_df}
	else:
		input_dict = doanhthu_sp_df
	
	return transform_data(input_dict, 'doanh_thu_sp_quy_cn', num_cols, str_cols, None, cols_mapping)

def transform_revenue_thang_nv_cn(doanhthu_nv_df):
	cols_mapping = {
		'machinhanh': 'ma_chi_nhanh',
		'manhanvien': 'ma_nhan_vien',
		'nam': 'nam',
		'thang': 'thang',
		'doanhthu': 'tong_doanh_thu'
	}
   
	num_cols = ['nam', 'thang', 'ma_chi_nhanh', 'tong_doanh_thu', 'ma_nhan_vien']

	if isinstance(doanhthu_nv_df, pd.DataFrame):
		input_dict = {'doanh_thu_thang_nv_cn': doanhthu_nv_df}
	else:
		input_dict = doanhthu_nv_df

	return transform_data(input_dict, 'doanh_thu_thang_nv_cn', num_cols, None, None, cols_mapping)