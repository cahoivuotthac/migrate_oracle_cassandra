-- Copy the SQL file to the container
-- docker cp sql\cassandra_schema.sql cassandra:/tmp/cassandra_schema.sql

-- Execute the script
-- docker exec -it cassandra cqlsh -f /tmp/cassandra_schema.sql

CREATE KEYSPACE IF NOT EXISTS BTL2_data
WITH REPLICATION = {
    'class': 'SimpleStrategy', -- data to be spread across the entire cluster 
    'replication_factor': 1
};

USE BTL2_data;

-- Data model tables 
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

CREATE TABLE doanh_thu_sp_quy_cn (
  ma_chi_nhanh int,
  ma_san_pham text,
  nam int,
  quy int,
  tong_doanh_thu bigint,
  PRIMARY KEY ((ma_chi_nhanh, ma_san_pham), nam, quy)
) WITH CLUSTERING ORDER BY (nam ASC, quy ASC);

CREATE TABLE doanh_thu_thang_nv_cn (
  ma_chi_nhanh int,
  ma_nhan_vien int,
  nam int,
  thang int,
  tong_doanh_thu bigint,
  PRIMARY KEY ((ma_chi_nhanh, ma_nhan_vien), nam, thang)
) WITH CLUSTERING ORDER BY (nam ASC, thang ASC);