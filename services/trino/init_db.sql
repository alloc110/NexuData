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
DROP TABLE IF EXISTS hive.metadata.events;

CREATE TABLE hive.metadata.events (
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
    external_location = 's3a://finhouse-datalake/bronze/events/'
);
-- Tự động quét phân vùng ban đầu nếu có sẵn dữ liệu lịch sử trên MinIO
CALL hive.system.sync_partition_metadata(
    schema_name => 'metadata', 
    table_name => 'events', 
    mode => 'ADD'
);



-- 1. Khởi tạo Schema Silver trỏ về vùng lưu trữ riêng trên MinIO
CREATE SCHEMA IF NOT EXISTS hive.silver
WITH (location = 's3a://finhouse-datalake/silver/');

-- 2. Xóa bảng cũ nếu tồn tại để tránh xung đột cấu trúc khi dev
DROP TABLE IF EXISTS hive.silver.wide_table_events;

-- 3. Tạo bảng cấu trúc chuẩn đã được làm giàu (Enriched)
CREATE TABLE hive.silver.wide_table_events (
    event_id VARCHAR,
    occurred_at TIMESTAMP(3),
    event_type VARCHAR,
    platform VARCHAR,
    user_id BIGINT,          -- Chuyển sang BIGINT
    username VARCHAR,
    full_name VARCHAR,
    product_id BIGINT,       -- Chuyển sang BIGINT
    email VARCHAR,           -- Đặt đúng vị trí tường minh
    user_created_at TIMESTAMP(3),
    product_name VARCHAR,
    product_price DOUBLE,
    category_name VARCHAR,
    store_name VARCHAR,
    date VARCHAR,
    hour VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['date', 'hour'],
    external_location = 's3a://finhouse-datalake/silver/wide_table_events/'
);

CREATE SCHEMA IF NOT EXISTS hive.gold WITH (location = 's3a://finhouse-datalake/gold/');


-- 2. Khởi tạo cấu trúc bảng dim_customer_profile
CREATE TABLE IF NOT EXISTS hive.gold.dim_customer_profile (
    customer_id VARCHAR,
    fullname VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP,
    last_active_date DATE,
    total_transactions BIGINT,
    primary_device VARCHAR,
    date VARCHAR,
    hour VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['date', 'hour'],
    external_location = 's3a://finhouse-datalake/gold/dim_customer_profile/'
);


-- 2. Xóa bảng cũ nếu có để làm sạch cấu trúc

-- 3. Tạo bảng Gold Funnel với cấu trúc chuẩn từ Spark đổ về
CREATE TABLE hive.gold.fact_funnel_conversion (
    log_date VARCHAR,
    device_type VARCHAR,
    step_1_visit BIGINT,
    step_2_view_product BIGINT,
    step_3_initiate_checkout BIGINT,
    step_4_purchase_success BIGINT,
    conversion_rate_overall DOUBLE,
    date VARCHAR, -- Cột phân vùng thư mục cha
    hour VARCHAR  -- Cột phân vùng thư mục con
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['date', 'hour'],
    external_location = 's3a://finhouse-datalake/gold/fact_funnel_conversion/'
);


-- 3. Tạo bảng Gold Funnel với cấu trúc chuẩn từ Spark đổ về

-- 3. Tạo bảng Fact RFM chuẩn Data Mart
CREATE TABLE hive.gold.fact_rfm_segments (
    customer_id VARCHAR,
    recency_value INT,
    frequency_value INT,
    monetary_value DOUBLE,             -- Kiểu số thực để lưu tổng số tiền giao dịch
    r_score INT,
    f_score INT,
    m_score INT,
    rfm_cell VARCHAR,                  -- Ví dụ: '555', '112'
    segment_name VARCHAR,              -- Ví dụ: 'Champions', 'Hibernating'
    date VARCHAR,                      -- Cột phân vùng thư mục cha (Airflow Execution Date)
    hour VARCHAR                       -- Cột phân vùng thư mục con (Airflow Execution Hour)
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['date', 'hour'],
    external_location = 's3a://finhouse-datalake/gold/fact_rfm_segments/'
);


-- 3. Khởi tạo cấu trúc bảng Fact Session Performance Daily
CREATE TABLE hive.gold.fact_session_performance (
    log_date VARCHAR,
    total_sessions BIGINT,
    total_users BIGINT,
    bounce_sessions BIGINT,
    bounce_rate DOUBLE,
    avg_events_per_session DOUBLE,
    operating_system VARCHAR,
    date VARCHAR -- Cột Partition Key nằm cuối cùng
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['date'],
    external_location = 's3a://finhouse-datalake/gold/fact_session_performance/'
);