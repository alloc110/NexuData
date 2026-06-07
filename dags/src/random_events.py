from confluent_kafka import Producer
import json
import uuid
import random
from datetime import datetime, timedelta
import logging
import os

# ==========================================
# CẤU HÌNH STRUCTURED LOGGING (JSON)
# ==========================================
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": record.levelname,
            "component": "KafkaProducer",
            "message": record.getMessage()
        }
        # Nếu có truyền thêm tham số extra, gộp chung vào JSON gốc
        if hasattr(record, "extra_data"):
            log_data.update(record.extra_data)
        return json.dumps(log_data)

# Thiết lập logger hệ thống
logger = logging.getLogger("ClawMarket_Producer")
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

# Đọc Log Level từ môi trường (Mặc định INFO ở Prod, DEBUG ở Dev)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logger.setLevel(LOG_LEVEL)

# ==========================================
# KAFKA DELIVERY CALLBACK (LOG CHUẨN SENIOR)
# ==========================================
def delivery_report(err, msg):
    """Callback gọi khi Kafka Broker phản hồi kết quả gửi tin nhắn"""
    if err is not None:
        # EXCEPTION LOG: Gặp lỗi nghiêm trọng khi ghi vào cluster
        record = logging.LogRecord("KafkaProducer", logging.ERROR, "", 0, "Message delivery failed", None, None)
        record.extra_data = {
            "error_details": str(err),
            "topic": msg.topic() if msg else "unknown",
            "partition": msg.partition() if msg else -1
        }
        logger.handle(record)
    else:
        # TRACING LOG: Chỉ bật khi cần debug sâu (mặc định INFO sẽ bỏ qua dòng này để tránh tốn CPU/RAM)
        if logger.isEnabledFor(logging.DEBUG):
            record = logging.LogRecord("KafkaProducer", logging.DEBUG, "", 0, "Message delivered successfully", None, None)
            record.extra_data = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode('utf-8') if msg.key() else None
            }
            logger.handle(record)

# ==========================================
# KHỞI TẠO CẤU HÌNH PIPELINE
# ==========================================
producer = Producer({'bootstrap.servers': 'kafka:9092'})

JOURNEY_PATTERNS = [
    # --- 4 Pattern cũ của bạn ---
    ["SEARCH", "VIEW_PRODUCT"],                                    
    ["SEARCH", "VIEW_PRODUCT", "ADD_TO_CART"],                      
    ["SEARCH", "VIEW_PRODUCT", "ADD_TO_CART", "PURCHASE"],           
    ["VIEW_PRODUCT", "ADD_TO_CART", "PURCHASE"],
    
    # --- 4 Pattern mới bổ sung ---
    ["VIEW_PRODUCT", "PURCHASE"],                          
    ["SEARCH", "VIEW_PRODUCT", "PURCHASE"],               
    ["VIEW_PRODUCT", "ADD_TO_CART"],                      
    ["SEARCH", "ADD_TO_CART"] ,       
    
    # --- 2 pattern ---
    ["SEARCH"],
    ["VIEW_PRODUCT"]                      
]


JOURNEY_WEIGHTS = [ 0.20, 0.15, 0.08, 0.07, 0.05, 0.05, 0.10, 0.10, 0.1, 0.1]
platforms = ["WEB", "IOS", "ANDROID"]

size_user = 100000
size_product = 100000
size_events = 100000

start_record = logging.LogRecord("KafkaProducer", logging.INFO, "", 0, "Starting event ingestion job", None, None)
start_record.extra_data = {"config": {"total_events": size_events, "target_topic": "fact_events"}}
logger.handle(start_record)

# ==========================================
# LUỒNG XỬ LÝ CHÍNH (MAIN DATA STREAM)
# ==========================================
events_emitted = 0
while events_emitted < size_events:
    # 1. Khởi tạo thông tin cố định cho một phiên (Session) của User cụ thể trên Product cụ thể
    user_id = random.randint(1, size_user)
    product_id = random.randint(1, size_product)
    platform = random.choice(platforms)
    
    # 2. Bốc ngẫu nhiên một kịch bản hành vi dựa trên trọng số đã cấu hình
    chosen_journey = random.choices(JOURNEY_PATTERNS, weights=JOURNEY_WEIGHTS)[0]
    
    # 3. Duyệt qua từng bước trong kịch bản để sinh Event theo đúng thứ tự logic
    for event_type in chosen_journey:
        if events_emitted >= size_events:
            break  # Dừng lại nếu đã đủ số lượng event cấu hình ban đầu
            
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "product_id": product_id,
            "event_type": event_type,
            "platform": platform,
            # Sử dụng định dạng chuẩn hóa UTC
            "occurred_at": (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))        }
        
        try:
            # Đẩy data vào local queue của librdkafka
            producer.produce(
                "fact_events",
                key=event["event_id"],
                value=json.dumps(event),
                callback=delivery_report
            )
            producer.poll(0) 
            
        except BufferError as ex:
            warn_record = logging.LogRecord("KafkaProducer", logging.WARNING, "", 0, "Local queue full, executing safety flush", None, None)
            warn_record.extra_data = {
                "current_index": events_emitted,
                "failed_event_id": event["event_id"],
                "exception": type(ex).__name__
            }
            logger.handle(warn_record)
            producer.flush()

        events_emitted += 1

        # LOG TIẾN ĐỘ THÔNG MINH
        if events_emitted % (size_events // 10) == 0:
            progress_percentage = int((events_emitted / size_events) * 100)
            progress_record = logging.LogRecord("KafkaProducer", logging.INFO, "", 0, f"Ingestion progress checkpoint: {progress_percentage}%", None, None)
            progress_record.extra_data = {
                "metrics": {
                    "processed_records": events_emitted,
                    "remaining_records": size_events - events_emitted
                }
            }
            logger.handle(progress_record)

# Xả toàn bộ hàng đợi trước khi đóng script
producer.flush()

# INFO LOG: Hoàn tất công việc
finish_record = logging.LogRecord("KafkaProducer", logging.INFO, "", 0, "Ingestion job completed successfully", None, None)
finish_record.extra_data = {"summary": {"total_emitted": events_emitted, "status": "FINISHED"}}
logger.handle(finish_record)