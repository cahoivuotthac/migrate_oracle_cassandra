-- For Cassandra's data model tables 
-- chi_tiet_san_pham_theo_ma_sp
-- SELECT 
--     sp."MaSanPham",
--     sp."TenSanPham",
--     sp."Gia",
--     sp."TheLoai",
--     dmsp."TenDanhMuc",
--     ttsp."TenThuocTinh",
--     ttsp."GiaTriThuocTinh"
-- FROM BTL1."SanPham" sp, BTL1."DanhMuc_SanPham" dmsp, BTL1."ThuocTinh_SanPham" ttsp
-- WHERE sp."MaSanPham" = dmsp."MaSanPham" AND sp."MaSanPham" = ttsp."MaSanPham"

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




