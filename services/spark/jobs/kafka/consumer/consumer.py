from datetime import time
from uuid import UUID

from confluent_kafka import Consumer
from clickhouse_connect import get_client
import json
import logging
import dateutil.parser as parser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'clickhouse-consumer-v2',
    'auto.offset.reset': 'earliest'
})

batch_size = 5000
data_batch = []

def on_assign(consumer, partitions):
    for p in partitions:
        p.offset = -2  # -2 tương đương với OFFSET_BEGINNING (đọc từ đầu)
        logger.info(f"📍 Đã ép Partition {p.partition} về vị trí bắt đầu (Earliest)")
    consumer.assign(partitions) 

logger.info("🚀 Kafka to ClickHouse Consumer started...")
consumer.subscribe(['fact_events'], on_assign=on_assign)
logger.info("✅ Subscribed to topic fact_events")


def get_clickhouse_client():
    while True:
        try:
            client = get_client(host='clickhouse', port=8123, username='admin', password='admin', database='finhouse')
            logger.info("✅ Connected to ClickHouse!")
            return client
        except Exception as e:
            logger.warning(f"Waiting for ClickHouse... {e}")
            time.sleep(5)

client = get_clickhouse_client()
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Kafka error: {msg.error()}")
            continue

        try:
            # Parse dữ liệu
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
            logger.error(f"❌ Lỗi parse dữ liệu: {e}")

        # Ghi theo BATCH
        if len(data_batch) >= batch_size:
            try:
                client.insert('fact_events', data_batch, 
                              column_names=['user_id', 'event_id', 'product_id', 'event_type', 'platform', 'occurred_at'])
                logger.info(f"✅ Đã chèn thành công {len(data_batch)} dòng vào ClickHouse")
                data_batch = [] # Reset batch
            except Exception as e:
                logger.error(f"🔥 Lỗi khi ghi vào ClickHouse: {e}")
                # Ở đây bạn có thể thêm logic retry nếu cần

except KeyboardInterrupt:
    logger.info("Stopping consumer...")
finally:
    consumer.close()