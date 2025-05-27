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

CREATE TABLE IF NOT EXISTS ban_hang_by_chi_nhanh (
    ma_chi_nhanh text,
    ma_san_pham text,
    tinh_trang text,
    ngay_cap_nhat timestamp,
    tong_so_luong_da_ban int,
    tong_so_luong_danh_gia int,
    tong_so_luong_sao int,
    PRIMARY KEY (ma_chi_nhanh, ma_san_pham)
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