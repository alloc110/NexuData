from datetime import datetime
import json
import logging
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# GỌI LOGGER TẬP TRUNG TỪ FILE UTILS
from logger_utils import get_logger

logger = get_logger("CustomerProfile", "SparkTransformer")

if __name__ == "__main__":
      # Đọc tham số đầu vào từ Airflow orchestrator
      if len(sys.argv) < 3:
            record = logging.LogRecord("CustomerProfile", logging.CRITICAL, "", 0, "Missing required arguments: execution_date and execution_hour", None, None)
            logger.handle(record)
            sys.exit(1)

      execution_date = sys.argv[1]  # Ví dụ: '2026-05-19'
      execution_hour = sys.argv[2]  # Ví dụ: '15'

      job_start_time = time.time()

      # INFO LOG: Đánh dấu khởi chạy Job tính toán phân tán
      init_record = logging.LogRecord("CustomerProfile", logging.INFO, "", 0, f"Initializing Spark Silver-to-Gold Data Mart computation", None, None)
      init_record.extra_data = {
            "pipeline_context": {
                  "target_date": execution_date,
                  "target_hour": execution_hour,
                  "data_mart": "dim_customer_profile"
            }
      }
      logger.handle(init_record)

      spark = None
      try:
            # 1. KHỞI TẠO SPARK SESSION KẾT NỐI MINIO
            spark = SparkSession.builder \
                  .appName(f"Finhouse_Gold_Customer_Profile_Hourly_{execution_date}_{execution_hour}") \
                  .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
                  .config("spark.hadoop.fs.s3a.access.key", "admin") \
                  .config("spark.hadoop.fs.s3a.secret.key", "supersecretpassword") \
                  .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                  .master("spark://spark-master:7077") \
                  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                  .getOrCreate()


            # 2. ĐỌC DỮ LIỆU TỪ TẦNG SILVER (Bọc xử lý phòng trường hợp phân vùng trống)
            silver_path = f"s3a://finhouse-datalake/silver/wide_table_events/date={execution_date}/hour={execution_hour}"
            
            read_start_time = time.time()
            try:
                  df_silver = spark.read.parquet(silver_path)
                  input_row_count = df_silver.count()
                  
            except Exception as read_ex:
                  warn_record = logging.LogRecord("CustomerProfile", logging.WARNING, "", 0, f"No input data found or path is missing in Silver layer for the specified hour", None, None)
                  warn_record.extra_data = {"target_path": silver_path, "error_details": str(read_ex)}
                  logger.handle(warn_record)
                  sys.exit(0)
                  

            # 3. TÍNH TOÁN DỮ LIỆU TỔNG HỢP (DATA MART)
            transform_start_time = time.time()
            
            df_gold = df_silver.groupBy("user_id") \
                  .agg(
                        F.first("full_name", ignorenulls=True).alias("fullname"),
                        F.first("email", ignorenulls=True).alias("email"),
                        F.first("user_created_at", ignorenulls=True).alias("created_at"),
                        F.to_date(F.max("occurred_at")).alias("last_active_date"), # Ép về kiểu Date
                        F.sum(F.when(F.col("event_type") == "PURCHASE", 1).otherwise(0)).alias("total_transactions"),
                        F.mode("platform").alias("primary_device")
                  ) \
                  .withColumnRenamed("user_id", "customer_id") \
                  .withColumn("date", F.lit(execution_date)) \
                  .withColumn("hour", F.lit(execution_hour))
                  
            df_gold = df_gold.select(
                  "customer_id", "fullname", "email", "created_at",
                  "last_active_date", "total_transactions", "primary_device",
                  "date", "hour" # Đưa 2 cột partition xuống cuối cùng
            )

            # 4. GHI XUỐNG TẦNG GOLD VỚI CHẾ ĐỘ DYNAMIC PARTITION OVERWRITE
            gold_path = "s3a://finhouse-datalake/gold/dim_customer_profile/"
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            
            # Đếm số lượng bản ghi Data Mart sau khi tổng hợp để đưa vào KPI log
            output_row_count = df_gold.count()

            df_gold.write \
                  .mode("overwrite") \
                  .partitionBy("date", "hour") \
                  .parquet(gold_path)

            total_duration = time.time() - job_start_time
            
            # INFO LOG: Báo cáo hiệu năng và kết quả nén dữ liệu Data Mart lên Gold
            success_record = logging.LogRecord("CustomerProfile", logging.INFO, "", 0, "Successfully aggregated and loaded data into Gold Data Mart", None, None)
            success_record.extra_data = {
                  "status": "SUCCESS",
                  "metrics": {
                  "input_silver_records": input_row_count,
                  "output_gold_records": output_row_count,
                  "total_duration_seconds": round(total_duration, 4),
                  "transform_duration_seconds": round(time.time() - transform_start_time, 4)
                  },
                  "storage": {
                  "destination_path": gold_path
                  }
            }
            logger.handle(success_record)

      except Exception as e:
            # CRITICAL ERROR LOG: Báo lỗi nghiêm trọng khi cụm Spark bị sập hoặc lỗi phân tán
            error_record = logging.LogRecord("CustomerProfile", logging.ERROR, "", 0, "Distributed processing job failed in Spark engine", None, None)
            error_record.extra_data = {
                  "status": "FAILED",
                  "error_details": str(e),
                  "failed_partition": {"date": execution_date, "hour": execution_hour}
            }
            logger.handle(error_record)
            sys.exit(1) # Báo lỗi để Airflow đổi màu Đỏ (Failed) nhằm gửi Alert
            
      finally:
            if spark:
                  spark.stop()