-- Copy the SQL file to the container
-- docker cp sql\cassandra_schema.sql cassandra:/tmp/cassandra_schema.sql

-- Execute the script
-- docker exec -it cassandra cqlsh -f /tmp/cassandra_schema.sql

CREATE KEYSPACE IF NOT EXISTS etl_data
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE etl_data;

-- Bảng nhân bản 
CREATE TABLE IF NOT EXISTS khachhang (
  ma_khach_hang int PRIMARY KEY,
  email text,
  ho_ten text,
  sdt text,
  dia_chi text,
  ngay_sinh date,
  gioi_tinh text
);

CREATE TABLE IF NOT EXISTS sanpham (
  ma_san_pham text PRIMARY KEY,
  ten_san_pham text,
  the_loai int,
  gia bigint
);

CREATE TABLE IF NOT EXISTS thuoctinh_sanpham (
  ma_san_pham text,
  ten_thuoc_tinh text,
  gia_tri_thuoc_tinh text,
  PRIMARY KEY (ma_san_pham, ten_thuoc_tinh)
);

CREATE TABLE IF NOT EXISTS danhmuc_sanpham (
  ma_san_pham text,
  ten_danh_muc text,
  PRIMARY KEY (ma_san_pham, ten_danh_muc)
);

-- Data model tables 
CREATE TABLE IF NOT EXISTS products_by_category (
    ten_danh_muc text,
    ma_san_pham text,
    ten_san_pham text,
    the_loai tinyint,
    gia bigint,
    PRIMARY KEY (ten_danh_muc, ma_san_pham)
);

CREATE TABLE IF NOT EXISTS chi_tiet_hoa_don_by_ma_hoa_don (
    ma_hoa_don text,
    ma_san_pham text,
    ten_san_pham text,
    so_luong int,
    thanh_tien decimal,
    gia decimal,
    the_loai text,
    PRIMARY KEY (ma_hoa_don, ma_san_pham)
);

CREATE TABLE IF NOT EXISTS daily_revenue_by_branch (
    ma_chi_nhanh int,
    ngay date,
    tong_tien bigint,
    PRIMARY KEY ((ma_chi_nhanh), ngay)
);

CREATE TABLE IF NOT EXISTS product_ratings_by_branch (
    ma_chi_nhanh int,
    ma_san_pham text,
    ten_san_pham text,
    tong_danh_gia int,
    tong_sao float,
    avg_sao float,
    PRIMARY KEY ((ma_chi_nhanh), ma_san_pham)
);

CREATE TABLE IF NOT EXISTS top_products_by_branch (
    ma_chi_nhanh int,
    so_luong_da_ban int,
    ma_san_pham text,
    ten_san_pham text,
    PRIMARY KEY ((ma_chi_nhanh), so_luong_da_ban, ma_san_pham)
) WITH CLUSTERING ORDER BY (so_luong_da_ban DESC);
