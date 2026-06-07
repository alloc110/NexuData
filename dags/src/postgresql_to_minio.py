from datetime import datetime
import json
import logging
import os
import time

import pandas as pd
import psycopg2
import s3fs  # Dùng để check file tồn tại trên MinIO/S3
import sys
sys.path.append("/opt/airflow/plugins")
from utils.logger_utils import get_logger

# Khởi tạo logger tập trung theo chuẩn dự án
logger = get_logger("PostgreSQLToMinIO", "MetadataExtractor")

# ==========================================
# CẤU HÌNH KẾT NỐI (Môi trường hoặc Mặc định)
# ==========================================
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "finhouse")
PG_PASSWORD = os.getenv("PG_PASSWORD", "finhouse")
PG_DB = os.getenv("PG_DB", "finhouse")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "supersecretpassword")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "finhouse-datalake")

TABLES_TO_EXTRACT = ["users", "categories", "stores", "products"]

def get_pg_connection():
    """Tạo kết nối tới PostgreSQL"""
    try:
        return psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB
        )
    except Exception as e:
        record = logging.LogRecord("PostgresClient", logging.CRITICAL, "", 0, "Failed to connect to PostgreSQL", None, None)
        record.extra_data = {"error_details": str(e)}
        logger.handle(record)
        raise e

def extract_table_to_parquet(table_name: str, execution_date: str):
    """
    Đọc dữ liệu từ Postgres và ghi vào MinIO. 
    Bỏ qua (Skip) nếu file của ngày hôm đó đã tồn tại (Idempotent Constraint).
    """
    start_time = time.time()
    s3_path = f"s3://{MINIO_BUCKET}/bronze/metadata/{table_name}/date={execution_date}/{table_name}_full.parquet"
    
    # 1. KHỞI TẠO S3 FILESYSTEM ĐỂ KIỂM TRA FILE TRÊN MINIO
    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT}
    )
    
    try:
        # KIỂM TRA ĐIỀU KIỆN: Nếu file đã tồn tại thì THOÁT SỚM (Early Return)
        path_without_s3 = s3_path.replace("s3://", "")
        if fs.exists(path_without_s3):
            skip_record = logging.LogRecord("ExtractorJob", logging.INFO, "", 0, f"Table '{table_name}' already extracted for date {execution_date}. Skipping task.", None, None)
            skip_record.extra_data = {
                "extraction_target": {"table": table_name, "date": execution_date},
                "status": "SKIPPED",
                "storage": {"destination_path": s3_path}
            }
            logger.handle(skip_record)
            return  # Thoát hàm luôn, không tốn tài nguyên chạy xuống dưới

        # 2. TIẾN HÀNH BẮT ĐẦU PIPELINE NẾU CHƯA CÓ FILE
        start_table_record = logging.LogRecord("ExtractorJob", logging.INFO, "", 0, f"Starting extraction for table: {table_name}", None, None)
        start_table_record.extra_data = {"extraction_target": {"table": table_name, "layer": "bronze"}}
        logger.handle(start_table_record)
        
        conn = get_pg_connection()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, conn)
        conn.close()
        
        row_count = len(df)
        if row_count == 0:
            warn_record = logging.LogRecord("ExtractorJob", logging.WARNING, "", 0, f"Table {table_name} contains 0 records. Skipping upload.", None, None)
            logger.handle(warn_record)
            return

        # 3. GHI FILE LÊN MINIO
        storage_options = {
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        }
        
        df.to_parquet(s3_path, engine="pyarrow", index=False, storage_options=storage_options)
        
        duration = time.time() - start_time
        
        # BÁO CÁO THÀNH CÔNG
        success_record = logging.LogRecord("ExtractorJob", logging.INFO, "", 0, f"Successfully extracted and loaded table: {table_name}", None, None)
        success_record.extra_data = {
            "metrics": {"rows_extracted": row_count, "duration_seconds": round(duration, 4)},
            "status": "SUCCESS",
            "storage": {"destination_path": s3_path, "table_name": table_name}
        }
        logger.handle(success_record)
        
    except Exception as e:
        error_record = logging.LogRecord("ExtractorJob", logging.ERROR, "", 0, f"Pipeline execution failed for table: {table_name}", None, None)
        error_record.extra_data = {"table_name": table_name, "error_details": str(e)}
        logger.handle(error_record)

# ==========================================
# LUỒNG CHẠY CHÍNH (MAIN EXECUTION)
# ==========================================
if __name__ == "__main__":
    current_date = datetime.utcnow().strftime("%Y-%m-%d")
    job_start_time = time.time()
    
    init_record = logging.LogRecord("MainPipeline", logging.INFO, "", 0, "Initializing metadata synchronization batch job", None, None)
    init_record.extra_data = {"job_metadata": {"execution_date": current_date, "target_tables": TABLES_TO_EXTRACT}}
    logger.handle(init_record)
    
    for table in TABLES_TO_EXTRACT:
        extract_table_to_parquet(table, current_date)
        
    total_duration = time.time() - job_start_time
    finish_record = logging.LogRecord("MainPipeline", logging.INFO, "", 0, "Metadata synchronization batch job process finished", None, None)
    finish_record.extra_data = {"summary": {"total_duration_seconds": round(total_duration, 4), "execution_date": current_date}}
    logger.handle(finish_record)