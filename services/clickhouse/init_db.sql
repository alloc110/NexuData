DROP TABLE IF EXISTS finhouse.fact_events;

CREATE TABLE finhouse.fact_events (
    event_id UUID,                -- Giữ nguyên UUID để hứng ID log sự kiện từ App/Kafka
    user_id UInt32,               -- Khớp hoàn toàn với SERIAL (INT) của Postgres
    product_id UInt32,            -- Khớp hoàn toàn với SERIAL (INT) của Postgres
    event_type LowCardinality(String),
    platform LowCardinality(String),
    occurred_at DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (event_type, platform, occurred_at);