-- =========================================================================
-- 0. DỌN SẸP CÁC BẢNG CŨ (TRÁNH XUNG ĐỘT CẤU TRÚC LỖI)
-- =========================================================================
DROP TABLE IF EXISTS hive.metadata.products;
DROP TABLE IF EXISTS hive.metadata.stores;
DROP TABLE IF EXISTS hive.metadata.categories;
DROP TABLE IF EXISTS hive.metadata.users;

-- =========================================================================
-- 1. KHỞI TẠO SCHEMA (DATABASE LAYER)
-- =========================================================================
CREATE SCHEMA IF NOT EXISTS hive.metadata
WITH (
    location = 's3a://finhouse-datalake/bronze/metadata/'
);

-- =========================================================================
-- 2. KHỞI TẠO CÁC BẢNG (TABLES) & ĐỒNG BỘ PHÂN VÙNG (PARTITIONS)
-- =========================================================================

----------------------------------------------------------------------------
-- BẢNG 1: NGƯỜI DÙNG (USERS)
----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hive.metadata.users (
    user_id integer,
    username varchar,
    email varchar,
    full_name varchar,
    created_at timestamp, 
    date varchar
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['date'],
    external_location = 's3a://finhouse-datalake/bronze/metadata/users/'
);

CALL hive.system.sync_partition_metadata(
    schema_name => 'metadata', 
    table_name => 'users', 
    mode => 'ADD'
);


----------------------------------------------------------------------------
-- BẢNG 2: DANH MỤC SẢN PHẨM (CATEGORIES)
----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hive.metadata.categories (
    category_id double,
    parent_id double,
    name varchar,
    slug varchar,
    level int,
    date varchar
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['date'],
    external_location = 's3a://finhouse-datalake/bronze/metadata/categories/'
);

CALL hive.system.sync_partition_metadata(
    schema_name => 'metadata', 
    table_name => 'categories', 
    mode => 'ADD'
);


----------------------------------------------------------------------------
-- BẢNG 3: CỬA HÀNG (STORES)
----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hive.metadata.stores (
    store_id bigint,
    owner_id varchar,
    address varchar,
    store_name varchar,
    is_official_store boolean,
    rating double,          -- Chuyển sang double để khớp với kiểu số thực trong Parquet
    created_at timestamp,
    date varchar
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['date'],
    external_location = 's3a://finhouse-datalake/bronze/metadata/stores/'
);

CALL hive.system.sync_partition_metadata(
    schema_name => 'metadata', 
    table_name => 'stores', 
    mode => 'ADD'
);


----------------------------------------------------------------------------
-- BẢNG 4: SẢN PHẨM (PRODUCTS)
----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hive.metadata.products (
    product_id integer,
    store_id bigint,
    category_id bigint,
    name varchar,
    description varchar,
    price double,          -- Đón đầu sửa lỗi bằng kiểu double tương tự cột rating
    stock_quantity int,
    created_at timestamp,
    date varchar
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['date'],
    external_location = 's3a://finhouse-datalake/bronze/metadata/products/'
);

CALL hive.system.sync_partition_metadata(
    schema_name => 'metadata', 
    table_name => 'products', 
    mode => 'ADD'
);

-- =========================================================================
-- 6. BẢNG SỰ KIỆN (FACT_EVENTS)
-- =========================================================================
DROP TABLE IF EXISTS hive.metadata.fact_events;

CREATE TABLE hive.metadata.fact_events (
    event_id VARCHAR,      
    user_id integer,         -- Ánh xạ từ UInt32 sang số nguyên lớn của Trino
    product_id integer,      -- Ánh xạ từ UInt32 sang số nguyên lớn của Trino
    event_type VARCHAR,     
    platform VARCHAR,       
    occurred_at BIGINT,     -- Giữ nguyên Epoch giây (Số nguyên) để tránh lỗi kén kiểu DateTime
    date VARCHAR,           -- Cột phân vùng thư mục cha (date=2026-05-20)
    hour VARCHAR            -- Cột phân vùng thư mục con (hour=02)
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['date', 'hour'],
    external_location = 's3a://finhouse-datalake/bronze/events/fact_events/'
);
-- Tự động quét phân vùng ban đầu nếu có sẵn dữ liệu lịch sử trên MinIO
CALL hive.system.sync_partition_metadata(
    schema_name => 'metadata', 
    table_name => 'fact_events', 
    mode => 'ADD'
);



-- 1. Khởi tạo Schema Silver trỏ về vùng lưu trữ riêng trên MinIO
CREATE SCHEMA IF NOT EXISTS hive.silver
WITH (location = 's3a://finhouse-datalake/silver/');

-- 2. Xóa bảng cũ nếu tồn tại để tránh xung đột cấu trúc khi dev
DROP TABLE IF EXISTS hive.silver.fact_enriched_events;

-- 3. Tạo bảng cấu trúc chuẩn đã được làm giàu (Enriched)
CREATE TABLE hive.silver.fact_enriched_events (
    event_id varchar,
    occurred_at timestamp,
    event_type varchar,
    platform varchar,
    user_id integer,
    username varchar,
    full_name varchar,
    product_id integer,
    product_name varchar,
    product_price double,
    category_name varchar,
    store_name varchar,
    date varchar,
    hour varchar  -- 1. Khai báo cột chính ở đây
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['date', 'hour'], -- 2. Đưa vào mảng phân vùng ở đây
    external_location = 's3a://finhouse-datalake/silver/fact_enriched_events/'
);


CREATE SCHEMA IF NOT EXISTS hive.gold WITH (location = 's3a://finhouse-datalake/gold/');

CREATE TABLE IF NOT EXISTS hive.gold.dm_store_performance (
    store_name varchar,
    category_name varchar,
    total_views bigint,
    total_purchases bigint,
    total_revenue double,
    conversion_rate double,
    date varchar,
    hour varchar
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['date', 'hour'],
    external_location = 's3a://finhouse-datalake/gold/dm_store_performance/'
);