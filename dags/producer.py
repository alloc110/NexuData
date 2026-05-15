from confluent_kafka import Producer
import json
import uuid
import random
from datetime import datetime
from dateutil import parser 

# Cấu hình log đơn giản
def delivery_report(err, msg):
    if err is not None:
        print(f"⚠️ Message delivery failed: {err}")

producer = Producer({'bootstrap.servers': 'kafka:9092'})

EVENT_TYPES = [
    "VIEW_PRODUCT",
    "ADD_TO_CART",
    "PURCHASE",
    "SEARCH",
    "CLICK",
    "LOGIN",
    "LOGOUT",   
    "CHECKOUT"
]
platforms = ["WEB", "IOS", "ANDROID"]
size_user = 100000
size_product = 50000   
size_events = 100000

print(f"🚀 Bắt đầu gửi {size_events} events...")

for i in range(size_events):
    
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, size_user)}",
        "product_id": random.randint(1, size_product),
        "event_type": random.choice(EVENT_TYPES),
        "platform": random.choice(platforms),
        "occurred_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")    
        }
    
    try:
        producer.produce(
            "fact_events",
            key=event["event_id"],
            value=json.dumps(event),
            callback=delivery_report
        )
        # Quan trọng: Giải phóng callback queue để tránh lỗi Queue Full
        producer.poll(0) 
        
    except BufferError:
        print("🔴 Local queue full, waiting for free space...")
        producer.flush()

    # Log tiến độ mỗi 10%
    if (i + 1) % (size_events // 10) == 0:
        print(f"✅ Đã xử lý: {i + 1} events")

producer.flush()
print(f"🏁 FINISH! Sended {size_events} events to Kafka.")