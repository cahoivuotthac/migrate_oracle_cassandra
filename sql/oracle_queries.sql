-- For Cassandra's data model tables 
-- chi_tiet_hoa_don_theo_ma_kh
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
FROM BTL1."ChiTietHoaDon" cthd 
  JOIN BTL1."HoaDon" hd ON cthd."MaHoaDon" = hd."MaHoaDon"
  RIGHT JOIN BTL1."KhachHang" kh ON HD."MaKhachHang" = KH."MaKhachHang"
ORDER BY BTL1."MaHoaDon", BTL1."NgayTao" DESC;

-- doanh_thu_moi_ngay_theo_ma_cn 
SELECT 
  cn."MaChiNhanh",
  TRUNC(hd."NgayTao") AS Ngay,
  SUM(hd."TongTien") AS TongTien
FROM "HoaDon" hd 
  RIGHT JOIN "NhanVien" nv ON hd."MaNhanVien" = nv."MaNhanVien"
  JOIN "ChiNhanh" cn ON cn."MaChiNhanh" = nv."MaChiNhanh"
GROUP BY cn."MaChiNhanh", TRUNC(hd."NgayTao")
ORDER BY Ngay DESC;

-- kho_sp_theo_ma_cn
SELECT 
  KSPQ."MaChiNhanh",
  KSPQ."MaSanPham",
  SP."TenSanPham",
  KSPQH."TinhTrang",
  KSPQH."TongSoLuongDanhGia",
  KSPQH."TongSoLuongDaBan",
  KSPQ."SoLuong"
FROM "KhoSanPham_QLBanHang" kspqh, "KhoSanPham_QLKho" kspq, "SanPham" sp
WHERE KSPQ."MaSanPham" = KSPQH."MaSanPham" AND SP."MaSanPham" = KSPQ."MaSanPham"
ORDER BY KSPQ."MaSanPham", KSPQ."SoLuong";

-- sl_khach_hang_moi_ngay_theo_ma_cn
SELECT 
  NV."MaChiNhanh",
  TRUNC(HD."NgayTao") AS Ngay,
  COUNT(DISTINCT HD."MaKhachHang") AS SoLuongKhachHang
FROM "NhanVien" NV
  JOIN "HoaDon" HD ON NV."MaNhanVien" = HD."MaNhanVien"
GROUP BY NV."MaChiNhanh", TRUNC(HD."NgayTao")
ORDER BY Ngay DESC;

--Doanh thu của mỗi sản phẩm tại mỗi chi nhánh, theo từng quý và từng năm
--doanh_thu_sp_quy_cn
SELECT
  KQL."MaChiNhanh",
  SP."MaSanPham",
  EXTRACT(YEAR FROM HD."NgayTao") AS Nam,
  EXTRACT(QUARTER FROM HD."NgayTao") AS Quy,
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
  EXTRACT(QUARTER FROM HD."NgayTao")
ORDER BY
  KQL."MaChiNhanh", SP."MaSanPham", Nam, Quy;

--Doanh thu theo tháng của mỗi nhân viên ở từng chi nhánh
--doanh_thu_thang_nv_cn
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
  NV."MaChiNhanh", NV."MaNhanVien", Nam, Thang;


