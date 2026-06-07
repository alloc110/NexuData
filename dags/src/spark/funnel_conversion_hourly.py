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

logger = get_logger("FunnelConversion", "SparkTransformer")

if __name__ == "__main__":
      # Đọc tham số đầu vào từ Airflow orchestrator
      if len(sys.argv) < 3:
            record = logging.LogRecord("FunnelConversion", logging.CRITICAL, "", 0, "Missing required arguments: execution_date and execution_hour", None, None)
            logger.handle(record)
            sys.exit(1)

      execution_date = sys.argv[1]  # Ví dụ: '2026-05-19'
      execution_hour = sys.argv[2]  # Ví dụ: '15'

      job_start_time = time.time()

      # INFO LOG: Đánh dấu khởi chạy Job tính toán phân tán
      init_record = logging.LogRecord("FunnelConversion", logging.INFO, "", 0, f"Initializing Spark Silver-to-Gold Data Mart computation", None, None)
      init_record.extra_data = {
            "pipeline_context": {
                  "target_date": execution_date,
                  "target_hour": execution_hour,
                  "data_mart": "fact_funnel_conversion"
            }
      }
      logger.handle(init_record)

      spark = None
      try:
            # 1. KHỞI TẠO SPARK SESSION KẾT NỐI MINIO
            spark = SparkSession.builder \
                  .appName(f"Finhouse-Gold-Funnel-Conversion-Hourly_{execution_date}_{execution_hour}") \
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
                  warn_record = logging.LogRecord("FunnelConversion", logging.WARNING, "", 0, f"No input data found or path is missing in Silver layer for the specified hour", None, None)
                  warn_record.extra_data = {"target_path": silver_path, "error_details": str(read_ex)}
                  logger.handle(warn_record)
                  sys.exit(0)
                  

            # 3. TÍNH TOÁN DỮ LIỆU TỔNG HỢP (DATA MART)
            transform_start_time = time.time()
            
            # CHÚ Ý: Vì đọc thư mục đích danh nên không có cột "date", chỉ groupBy "platform"
            df_funnel = df_silver.groupBy("platform") \
                  .agg(
                        F.count_distinct(F.when(F.col("event_type") == "SEARCH", F.col("user_id"))).alias("step_1_visit"),
                        F.count_distinct(F.when(F.col("event_type") == "VIEW_PRODUCT", F.col("user_id"))).alias("step_2_view_product"),
                        F.count_distinct(F.when(F.col("event_type") == "ADD_TO_CART", F.col("user_id"))).alias("step_3_initiate_checkout"),
                        F.count_distinct(F.when(F.col("event_type") == "PURCHASE", F.col("user_id"))).alias("step_4_purchase_success")
                  )
            
            df_funnel_final = df_funnel.withColumn(
                  "conversion_rate_overall",
                  F.round(
                        F.when(F.col("step_1_visit") > 0, F.col("step_4_purchase_success") / F.col("step_1_visit"))
                        .otherwise(0.0), 
                        4
                  )
            )

            # Chuẩn hóa lại tên cột và DÙNG F.lit() ĐỂ TÁI TẠO CỘT NGÀY GIỜ
            df_funnel_final = df_funnel_final.select(
                  F.lit(execution_date).alias("log_date"),        # Tiêm ngày chạy vào làm log_date
                  F.col("platform").alias("device_type"),
                  "step_1_visit",
                  "step_2_view_product",
                  "step_3_initiate_checkout",
                  "step_4_purchase_success",
                  "conversion_rate_overall",
                  F.lit(execution_date).alias("date"),           # Phục vụ Partition Key cha cho lệnh write
                  F.lit(execution_hour).alias("hour")            # Phục vụ Partition Key con cho lệnh write
            )

            # =================================================================
            # 4. GHI XUỐNG TẦNG GOLD VỚI CHẾ ĐỘ DYNAMIC PARTITION OVERWRITE
            # =================================================================
            gold_path = "s3a://finhouse-datalake/gold/fact_funnel_conversion/"
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            
            output_row_count = df_funnel_final.count()

            df_funnel_final.write \
                  .mode("overwrite") \
                  .partitionBy("date", "hour") \
                  .parquet(gold_path)

            total_duration = time.time() - job_start_time
            
            # INFO LOG: Báo cáo hiệu năng và kết quả nén dữ liệu Data Mart lên Gold
            success_record = logging.LogRecord("FunnelConversion", logging.INFO, "", 0, "Successfully aggregated and loaded data into Gold Data Mart", None, None)
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
            error_record = logging.LogRecord("FunnelConversion", logging.ERROR, "", 0, "Distributed processing job failed in Spark engine", None, None)
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