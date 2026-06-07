from datetime import datetime
import json
import logging
import os
import time  # Sửa từ 'from datetime import time' để dùng được time.sleep()
from uuid import UUID

from clickhouse_connect import get_client
from confluent_kafka import Consumer
import dateutil.parser as parser

# ==========================================
# CẤU HÌNH STRUCTURED LOGGING (JSON)
# ==========================================
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": record.levelname,
            "component": "KafkaClickHouseConsumer",
            "message": record.getMessage()
        }
        # Gộp các siêu dữ liệu (metadata) truyền thêm vào JSON gốc
        if hasattr(record, "extra_data"):
            log_data.update(record.extra_data)
        return json.dumps(log_data)

# Thiết lập logger hệ thống
logger = logging.getLogger("ClawMarket_Consumer")
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logger.setLevel(LOG_LEVEL)

# ==========================================
# KẾT NỐI VÀ CẤU HÌNH PIPELINE
# ==========================================
batch_size = 5000
data_batch = []

consumer_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'clickhouse-consumer-v2',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True  # Hoặc False nếu bạn muốn commit thủ công sau khi insert xong
}

consumer = Consumer(consumer_config)

def on_assign(consumer, partitions):
    """Callback xử lý khi Kafka điều phối Rebalance/Assign Partition"""
    partition_ids = []
    for p in partitions:
        p.offset = -2  # OFFSET_BEGINNING
        partition_ids.append(p.partition)
        
    record = logging.LogRecord("KafkaConsumer", logging.INFO, "", 0, "Kafka partitions assigned and offset reset executed", None, None)
    record.extra_data = {
        "kafka_metadata": {
            "assigned_partitions": partition_ids,
            "offset_strategy": "EARLIEST"
        }
    }
    logger.handle(record)
    consumer.assign(partitions) 

# INFO LOG: Đánh dấu khởi động Service
start_record = logging.LogRecord("KafkaConsumer", logging.INFO, "", 0, "Initializing Kafka to ClickHouse consumer pipeline", None, None)
start_record.extra_data = {"config": {"batch_size": batch_size, "group_id": consumer_config['group.id']}}
logger.handle(start_record)

consumer.subscribe(['fact_events'], on_assign=on_assign)

def get_clickhouse_client():
    """Hàm khởi tạo kết nối với cơ chế Retry bền bỉ (Resilient connection)"""
    ch_config = {'host': 'clickhouse', 'port': 8123, 'username': 'admin', 'password': 'admin', 'database': 'finhouse'}
    while True:
        try:
            client = get_client(**ch_config)
            
            record = logging.LogRecord("ClickHouseClient", logging.INFO, "", 0, "Successfully connected to ClickHouse cluster", None, None)
            record.extra_data = {"connection_target": {"host": ch_config['host'], "database": ch_config['database']}}
            logger.handle(record)
            
            return client
        except Exception as e:
            record = logging.LogRecord("ClickHouseClient", logging.WARNING, "", 0, "ClickHouse connection refused. Retrying in 5 seconds...", None, None)
            record.extra_data = {"error_details": str(e), "target_host": ch_config['host']}
            logger.handle(record)
            time.sleep(5)

client = get_clickhouse_client()

# ==========================================
# LUỒNG TIÊU THỤ DỮ LIỆU CHÍNH (MAIN LOOP)
# ==========================================
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            record = logging.LogRecord("KafkaConsumer", logging.ERROR, "", 0, "Kafka broker returned an error event", None, None)
            record.extra_data = {"error_code": str(msg.error())}
            logger.handle(record)
            continue

        try:
            # Parse dữ liệu từ Kafka Message
            event = json.loads(msg.value().decode("utf-8"))      
            data_batch.append([
                event['user_id'],
                UUID(event['event_id']),
                event['product_id'],
                event['event_type'],
                event['platform'],
                parser.parse(event['occurred_at'])
            ])
            
        except Exception as e:
            record = logging.LogRecord("DataParser", logging.ERROR, "", 0, "Failed to parse incoming Kafka message payload", None, None)
            record.extra_data = {
                "error_details": str(e),
                "raw_payload_preview": str(msg.value()[:200]) if msg.value() else None
            }
            logger.handle(record)

        # Thực thi ghi xuống ClickHouse theo cơ chế Batching
        if len(data_batch) >= batch_size:
            try:
                start_time = time.time()
                
                client.insert(
                    'events', 
                    data_batch, 
                    column_names=['user_id', 'event_id', 'product_id', 'event_type', 'platform', 'occurred_at']
                )
                
                duration = time.time() - start_time
                
                # INFO LOG: Báo cáo hiệu năng ghi dữ liệu (Data Ingestion Metrics)
                success_record = logging.LogRecord("ClickHouseSink", logging.INFO, "", 0, "Successfully flushed batch to ClickHouse", None, None)
                success_record.extra_data = {
                    "metrics": {
                        "records_inserted": len(data_batch),
                        "duration_seconds": round(duration, 4),
                        "target_table": "events"
                    }
                }
                logger.handle(success_record)
                
                data_batch = [] # Reset buffer memory
                
            except Exception as e:
                # CRITICAL ERROR LOG: Lỗi ghi DB cần cảnh báo ngay lập tức
                fail_record = logging.LogRecord("ClickHouseSink", logging.ERROR, "", 0, "Database insertion failed", None, None)
                fail_record.extra_data = {
                    "error_details": str(e),
                    "lost_batch_size": len(data_batch),
                    "target_table": "events"
                }
                logger.handle(fail_record)
                # Tùy thuộc vào chiến lược (DLQ hoặc Retry), xử lý data_batch tại đây

except KeyboardInterrupt:
    shutdown_record = logging.LogRecord("System", logging.INFO, "", 0, "KeyboardInterrupt received. Initiating graceful shutdown...", None, None)
    logger.handle(shutdown_record)
finally:
    # Đóng kết nối an toàn
    consumer.close()
    final_record = logging.LogRecord("System", logging.INFO, "", 0, "Kafka consumer closed. Pipeline stopped.", None, None)
    logger.handle(final_record)