#!/bin/sh

echo "Đang chờ MinIO server khởi động..."
sleep 5

echo "Đang kết nối tới MinIO..."
while ! mc alias set myminio http://minio:9000 admin supersecretpassword 2>/dev/null; do
    echo "MinIO Server hoặc Docker DNS chưa sẵn sàng, đang thử lại sau 2 giây..."
    sleep 2
done
# 1. TẠO CÁC BUCKET CHÍNH
echo "Đang tạo bucket..."

mc mb myminio/finhouse-datalake --ignore-existing

# 2. MẸO: TẠO THƯ MỤC TRONG DATALAKE BẰNG CÁCH GHI FILE ẨN (.keep)
echo "Đang quy hoạch cấu trúc Medallion cho Datalake..."

# Tạo một file rỗng trên container
touch /tmp/.keep

# Đẩy file rỗng này vào các đường dẫn để ép MinIO hiển thị thư mục
# Tầng Bronze
mc cp /tmp/.keep myminio/finhouse-datalake/bronze/metadata/users/
mc cp /tmp/.keep myminio/finhouse-datalake/bronze/metadata/products/
mc cp /tmp/.keep myminio/finhouse-datalake/bronze/metadata/stores/
mc cp /tmp/.keep myminio/finhouse-datalake/bronze/metadata/categories/
mc cp /tmp/.keep myminio/finhouse-datalake/bronze/events/fact_events/

# Tầng Silver
mc cp /tmp/.keep myminio/finhouse-datalake/silver/wide_events_enriched/

# Tầng Gold
mc cp /tmp/.keep myminio/finhouse-datalake/gold/store_daily_performance/
mc cp /tmp/.keep myminio/finhouse-datalake/gold/user_conversion_funnel/

echo "================================================="
echo "Hoàn tất xây dựng móng Data Lakehouse cho Finhouse!"
echo "================================================="

exit 0