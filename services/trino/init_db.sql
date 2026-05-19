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
    user_id varchar,
    username varchar,
    email varchar,
    full_name varchar,
    created_at timestamp, -- Để mặc định để Trino tự làm tròn từ file Parquet
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

CREATE TABLE IF NOT EXISTS hive.metadata.fact_events (
    event_id varchar,       -- Được cast thành String/Varchar từ hàm toString() trong Airflow
    user_id varchar,
    product_id integer,     -- Kiểu UInt32 của ClickHouse ánh xạ sang tương đương Integer
    event_type varchar,     -- Kiểu LowCardinality(String) sang Trino chỉ cần Varchar
    platform varchar,
    occurred_at integer,  -- Giữ nguyên kiểu thời gian
    date varchar            -- Cột phân vùng (lấy từ thư mục date={{ ds }} do Airflow tạo)
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['date'],
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
    occurred_at timestamp,   -- Kiểu thời gian chuẩn yyyy-mm-dd hh:mm:ss
    event_type varchar,
    platform varchar,
    user_id varchar,
    username varchar,
    full_name varchar,
    product_id integer,
    product_name varchar,
    product_price double,
    category_name varchar,
    store_name varchar,
    date varchar             -- Cột phân vùng chính
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['date'],
    external_location = 's3a://finhouse-datalake/silver/fact_enriched_events/'
);