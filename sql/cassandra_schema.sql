-- Copy the SQL file to the container
-- docker cp sql\cassandra_schema.sql cassandra:/tmp/cassandra_schema.sql

-- Execute the script
-- docker exec -it cassandra cqlsh -f /tmp/cassandra_schema.sql

CREATE KEYSPACE IF NOT EXISTS BTL2_data
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE BTL2_data;

-- Bảng nhân bản 
-- CREATE TABLE IF NOT EXISTS khachhang (
--   ma_khach_hang int PRIMARY KEY,
--   email text,
--   ho_ten text,
--   sdt text,
--   dia_chi text,
--   ngay_sinh date,
--   gioi_tinh text
-- );

-- CREATE TABLE IF NOT EXISTS sanpham (
--   ma_san_pham text PRIMARY KEY,
--   ten_san_pham text,
--   the_loai int,
--   gia bigint
-- );

-- CREATE TABLE IF NOT EXISTS thuoctinh_sanpham (
--   ma_san_pham text,
--   ten_thuoc_tinh text,
--   gia_tri_thuoc_tinh text,
--   PRIMARY KEY (ma_san_pham, ten_thuoc_tinh)
-- );

-- CREATE TABLE IF NOT EXISTS danhmuc_sanpham (
--   ma_san_pham text,
--   ten_danh_muc text,
--   PRIMARY KEY (ma_san_pham, ten_danh_muc)
-- );

-- Data model tables 
-- CREATE TABLE IF NOT EXISTS chi_tiet_san_pham_theo_ma_sp ( 
--     ma_san_pham TEXT,
--     ten_san_pham TEXT,
--     gia INT,
--     the_loai INT,
--     ten_danh_muc TEXT,
--     ten_thuoc_tinh TEXT,
--     gia_tri_thuoc_tinh TEXT,
--     PRIMARY KEY (ma_san_pham, ten_thuoc_tinh)
-- );

CREATE TABLE IF NOT EXISTS chi_tiet_hoa_don_theo_ma_kh (
    ma_khach_hang int,
    ma_hoa_don int,
    ma_san_pham text,
    so_luong int,
    thanh_tien bigint,
    tong_tien bigint,
    ngay_tao timestamp,
    phuong_thuc_thanh_toan text,
    ma_nhan_vien int,
    PRIMARY KEY ((ma_khach_hang), ngay_tao, ma_hoa_don)
) WITH CLUSTERING ORDER BY (ngay_tao DESC);

CREATE TABLE IF NOT EXISTS doanh_thu_moi_ngay_theo_ma_cn (
    ma_chi_nhanh int,
    ngay date,
    tong_tien bigint,
    PRIMARY KEY ((ma_chi_nhanh), ngay)
) WITH CLUSTERING ORDER BY (ngay DESC);

CREATE TABLE IF NOT EXISTS kho_sp_theo_ma_cn (
    ma_chi_nhanh int,
    ma_san_pham text,
    ten_san_pham text,
    tinh_trang text, 
    tong_so_luong_danh_gia int,
    tong_so_luong_da_ban int,
    tong_so_luong_ton_kho int,
    PRIMARY KEY ((ma_chi_nhanh), ma_san_pham, tong_so_luong_ton_kho)
);

CREATE TABLE IF NOT EXISTS sl_khach_hang_moi_ngay_theo_ma_cn (
  ma_chi_nhanh int,
  ngay date,
  so_luong_khach_hang int,
  PRIMARY KEY ((ma_chi_nhanh), ngay)
) WITH CLUSTERING ORDER BY (ngay DESC); 

