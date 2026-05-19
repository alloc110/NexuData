#!/bin/bash

# 1. Khởi động lệnh chạy Trino mặc định của container ở tiến trình nền (background)
/usr/lib/trino/bin/run-trino &

# 2. Vòng lặp kiểm tra xem Trino đã mở cửa nhận kết nối chưa
echo "====> Đang đợi Trino khởi động hoàn tất..."
until trino --execute "SELECT 1" > /dev/null 2>&1; do
    echo "====> Trino đang khởi động, đợi thêm 3 giây..."
    sleep 3
done

echo "====> Trino đã SẴN SÀNG! Tiến hành chạy file init.sql..."

# 3. Chạy file SQL khởi tạo
trino -f /docker-init/init_db.sql

echo "====> Khởi tạo dữ liệu tầng Bronze THÀNH CÔNG!"

# 4. Giữ container tiếp tục sống ở tiến trình chính
wait