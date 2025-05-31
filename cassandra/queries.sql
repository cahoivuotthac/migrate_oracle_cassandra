--Bảng chi_tiet_hoa_don_theo_ma_kh ✅
-- Lấy toàn bộ hóa đơn của một khách hàng, từ mới nhất đến cũ nhất
SELECT * FROM chi_tiet_hoa_don_theo_ma_kh
WHERE ma_khach_hang = 'KH001';

-- Lọc thêm theo ngày
SELECT * FROM chi_tiet_hoa_don_theo_ma_kh
WHERE ma_khach_hang = 'KH001'
AND ngay_tao >= '2024-01-01' AND ngay_tao <= '2024-12-31';

--bảng doanh_thu_moi_ngay_theo_ma_cn ✅
-- Lấy doanh thu tất cả các ngày của chi nhánh 1, từ mới đến cũ
SELECT * FROM doanh_thu_moi_ngay_theo_ma_cn
WHERE ma_chi_nhanh = 1;

-- Lấy doanh thu của chi nhánh 1 vào ngày cụ thể
SELECT * FROM doanh_thu_moi_ngay_theo_ma_cn
WHERE ma_chi_nhanh = 1 AND ngay = '2024-12-25';

--bảng kho_sp_theo_ma_cn ✅
--Lấy toàn bộ sản phẩm ở chi nhánh 1
SELECT * FROM kho_sp_theo_ma_cn
WHERE ma_chi_nhanh = 1;
--Lấy thông tin sản phẩm cụ thể tại chi nhánh 1
SELECT * FROM kho_sp_theo_ma_cn
WHERE ma_chi_nhanh = 1 AND ma_san_pham = 'CCNPLT0300';
-- Lấy thông tin chi tiết của một bản ghi cụ thể
SELECT * FROM kho_sp_theo_ma_cn
WHERE ma_chi_nhanh = 1 AND ma_san_pham = 'SP001' AND tong_sl_ton_kho = 50;

--bảng sl_khach_hang_moi_ngay_theo_ma_cn ✅
-- Lấy số khách hàng từng ngày của chi nhánh 1
SELECT * FROM sl_khach_hang_moi_ngay_theo_ma_cn
WHERE ma_chi_nhanh = 1;

-- Lấy số khách ngày 2024-05-01 tại chi nhánh 1
SELECT * FROM sl_khach_hang_moi_ngay_theo_ma_cn
WHERE ma_chi_nhanh = 1 AND ngay = '2024-05-01';