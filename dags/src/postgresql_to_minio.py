import os
import pandas as pd
import psycopg2
from datetime import datetime

# ==========================================
# CẤU HÌNH KẾT NỐI (Lấy từ biến môi trường nếu có, hoặc fix cứng)
# ==========================================

# Cấu hình PostgreSQL
PG_HOST = "postgres"
PG_PORT = "5432"
PG_USER = "finhouse"
PG_PASSWORD = "finhouse"
PG_DB = "finhouse"

# Cấu hình MinIO (S3 Compatible)
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "supersecretpassword"
MINIO_BUCKET = "finhouse-datalake"

# Danh sách các bảng cần lấy
TABLES_TO_EXTRACT = ["users", "categories", "stores", "products"]

def get_pg_connection():
    """Tạo kết nối tới PostgreSQL"""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB
    )

def extract_table_to_parquet(table_name: str, execution_date: str):
    """
    Hàm đọc nguyên một bảng từ Postgres và ghi thẳng thành file Parquet trên MinIO
    """
    print(f"🚀 Bắt đầu xử lý bảng: {table_name}")
    
    conn = get_pg_connection()
    
    try:
        # 1. Đọc dữ liệu từ Postgres vào Pandas DataFrame
        # Lưu ý: Nếu bảng siêu to (triệu dòng), nên dùng chunksize. Ở đây ta lấy hết (Full Load)
        query = f"SELECT * FROM {table_name}"
        print(f"   Đang đọc dữ liệu từ DB...")
        df = pd.read_sql(query, conn)
        
        row_count = len(df)
        if row_count == 0:
            print(f"   ⚠️ Bảng {table_name} đang trống, bỏ qua.")
            return

        print(f"   Đã đọc {row_count} dòng. Chuẩn bị đẩy lên MinIO...")
        
        # 2. Xây dựng đường dẫn (Prefix) lưu file trên MinIO theo chuẩn Bronze
        # Định dạng: bronze/metadata/{tên_bảng}/date={YYYY-MM-DD}/data.parquet
        s3_path = f"s3://{MINIO_BUCKET}/bronze/metadata/{table_name}/date={execution_date}/{table_name}_full.parquet"
        
        # 3. Cấu hình S3 filesystem (trỏ về MinIO cục bộ thay vì AWS thật)
        storage_options = {
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {
                "endpoint_url": MINIO_ENDPOINT
            }
        }
        
        # 4. Ghi trực tiếp DataFrame thành file Parquet lên MinIO
        df.to_parquet(
            s3_path,
            engine="pyarrow",
            index=False,
            storage_options=storage_options
        )
        
        print(f"   ✅ Thành công! Đã lưu tại: {s3_path}")
        
    except Exception as e:
        print(f"   ❌ Lỗi khi xử lý bảng {table_name}: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    # Lấy ngày hiện tại (dùng để chia partition thư mục trên MinIO)
    # Nếu chạy trong Airflow, bạn sẽ truyền tham số {{ ds }} vào thay vì lấy today()
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"=== BẮT ĐẦU ĐỒNG BỘ DỮ LIỆU NGÀY: {current_date} ===")
    
    for table in TABLES_TO_EXTRACT:
        extract_table_to_parquet(table, current_date)
        
    print("=== HOÀN TẤT ĐỒNG BỘ! ===")