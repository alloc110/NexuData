CREATE TABLE IF NOT EXISTS finhouse.fact_events(
    -- Chuyển về String vì Python gửi chuỗi "user_..."
    user_id UInt32, 
    
    -- UUID trong ClickHouse nên dùng kiểu UUID chuẩn để tiết kiệm bộ nhớ
    event_id UUID,
    
    product_id UInt32,
    
    -- Giữ nguyên LowCardinality là rất tốt cho performance
    event_type LowCardinality(String),
    platform LowCardinality(String),
    
    -- DateTime mặc định độ chính xác theo giây
    occurred_at DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
-- Đưa platform vào Order By nếu bạn thường xuyên lọc theo nền tảng
ORDER BY (event_type, platform, occurred_at);