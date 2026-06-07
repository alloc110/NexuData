from datetime import datetime
import json
import logging
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window # Gọi thêm thư viện Cửa sổ để chế Session

# GỌI LOGGER TẬP TRUNG TỪ FILE UTILS
from logger_utils import get_logger

logger = get_logger("SessionPerformance", "SparkTransformer")

if __name__ == "__main__":
      # Đọc tham số đầu vào từ Airflow orchestrator (Sửa lỗi check thiếu đối số hour)
      if len(sys.argv) < 3:
            record = logging.LogRecord("SessionPerformance", logging.CRITICAL, "", 0, "Missing required arguments: execution_date and execution_hour", None, None)
            logger.handle(record)
            sys.exit(1)

      execution_date = sys.argv[1]  # Ví dụ: '2026-06-05'
      execution_hour = sys.argv[2]  # Ví dụ: '04' (Nhận thêm tham số để tránh văng index error)

      job_start_time = time.time()

      # INFO LOG: Đánh dấu khởi chạy Job tính toán phân tán
      init_record = logging.LogRecord("SessionPerformance", logging.INFO, "", 0, f"Initializing Spark Silver-to-Gold Session Performance Daily computation", None, None)
      init_record.extra_data = {
            "pipeline_context": {
                  "target_date": execution_date,
                  "target_hour": execution_hour,
                  "data_mart": "fact_session_performance"
            }
      }
      logger.handle(init_record)

      spark = None
      try:
            # 1. KHỞI TẠO SPARK SESSION KẾT NỐI MINIO
            spark = SparkSession.builder \
                  .appName(f"Finhouse-Gold-Session-Performance-Daily_{execution_date}") \
                  .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
                  .config("spark.hadoop.fs.s3a.access.key", "admin") \
                  .config("spark.hadoop.fs.s3a.secret.key", "supersecretpassword") \
                  .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                  .master("spark://spark-master:7077") \
                  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                  .getOrCreate()


            # 2. ĐỌC DỮ LIỆU TỪ TẦNG SILVER (Quét toàn bộ các giờ của ngày execution_date)
            silver_path = f"s3a://finhouse-datalake/silver/wide_table_events/date={execution_date}/*"
            
            read_start_time = time.time()
            try:
                  df_silver = spark.read.parquet(silver_path)
                  input_row_count = df_silver.count()
                  
            except Exception as read_ex:
                  warn_record = logging.LogRecord("SessionPerformance", logging.WARNING, "", 0, f"No input data found or path is missing in Silver layer for date={execution_date}", None, None)
                  warn_record.extra_data = {"target_path": silver_path, "error_details": str(read_ex)}
                  logger.handle(warn_record)
                  sys.exit(0)
                  

            # 3. TÍNH TOÁN DỮ LIỆU TỔNG HỢP (DATA MART - 2 STEP AGGREGATION)
            transform_start_time = time.time()
            
            # -----------------------------------------------------------------
            # FIX LỖI CHÍ MẠNG: TỰ KHỞI TẠO SESSION_ID CHO USER (SESSIONIZATION TRICK)
            # Định nghĩa: Các hành động liên tiếp của 1 user cách nhau không quá 30 phút (1800s)
            # -----------------------------------------------------------------
            user_window = Window.partitionBy("user_id").orderBy("occurred_at")

            # Lấy mốc thời gian của hành động liền trước đó
            df_with_prev = df_silver.withColumn("prev_time", F.lag("occurred_at").over(user_window))

            # Tính khoảng cách giây, nếu vượt quá 1800 giây hoặc là hành động đầu tiên -> Đánh dấu session mới (1)
            df_with_is_new = df_with_prev.withColumn(
                "is_new_session",
                F.when(
                    F.col("prev_time").isNull() | 
                    ((F.col("occurred_at").cast("long") - F.col("prev_time").cast("long")) > 1800), 
                    1
                ).otherwise(0)
            )

            # Cộng dồn tích lũy chỉ số index để sinh mã định danh phiên
            df_with_session_idx = df_with_is_new.withColumn(
                "session_idx", 
                F.sum("is_new_session").over(user_window)
            )

            # Khởi tạo thành công trường session_id
            df_silver_patched = df_with_session_idx.withColumn(
                "session_id", 
                F.concat(F.col("user_id").cast("string"), F.lit("_"), F.col("session_idx").cast("string"))
            )

            # -----------------------------------------------------------------
            # BẮT ĐẦU CÁC BƯỚC GOM CỤM CHỈ SỐ THEO MẪU CŨ CỦA ÔNG
            # -----------------------------------------------------------------
            # Bước 3.1: Gom cụm mức Session để tính số lượng sự kiện phát sinh của từng phiên
            df_session_level = df_silver_patched.groupBy("session_id", "platform") \
                  .agg(
                        F.count("event_id").alias("events_in_session"),
                        F.first("user_id").alias("user_id")
                  )
            
            # Bước 3.2: Gom cụm mức Ngày và Hệ điều hành (Platform)
            df_daily_level = df_session_level.groupBy("platform") \
                  .agg(
                        F.count("session_id").alias("total_sessions"),
                        F.approx_count_distinct("user_id").alias("total_users"),
                        # Bounce Session là phiên chỉ có đúng 1 hành động duy nhất rồi thoát
                        F.sum(F.when(F.col("events_in_session") == 1, 1).otherwise(0)).alias("bounce_sessions"),
                        F.avg("events_in_session").alias("raw_avg_events")
                  )
            
            # Bước 3.3: Tính toán tỷ lệ phần trăm và làm tròn chỉ số trực quan
            df_final = df_daily_level.withColumn(
                  "bounce_rate",
                  F.round(
                        F.when(F.col("total_sessions") > 0, F.col("bounce_sessions") / F.col("total_sessions"))
                        .otherwise(0.0), 
                        4
                  )
            ).withColumn(
                  "avg_events_per_session",
                  F.round(F.col("raw_avg_events"), 2)
)

            # Khớp 100% tên cột và thứ tự vật lý với Trino DDL
            df_gold_session = df_final.select(
                  F.lit(execution_date).alias("log_date"),
                  F.col("total_sessions").cast("bigint"),
                  F.col("total_users").cast("bigint"),
                  F.col("bounce_sessions").cast("bigint"),
                  F.col("bounce_rate").cast("double"),
                  F.col("avg_events_per_session").cast("double"),
                  F.col("platform").alias("operating_system"),
                  F.lit(execution_date).alias("date") # Cột partition key nằm cuối cùng
            )


            # 4. GHI XUỐNG TẦNG GOLD VỚI CHẾ ĐỘ DYNAMIC PARTITION OVERWRITE
            gold_path = "s3a://finhouse-datalake/gold/fact_session_performance/"
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            
            # Đếm số lượng bản ghi Data Mart sau khi tổng hợp để đưa vào KPI log
            output_row_count = df_gold_session.count()

            df_gold_session.write \
                  .mode("overwrite") \
                  .partitionBy("date") \
                  .parquet(gold_path)

            total_duration = time.time() - job_start_time
            
            # INFO LOG: Báo cáo hiệu năng và kết quả nén dữ liệu Data Mart lên Gold
            success_record = logging.LogRecord("SessionPerformance", logging.INFO, "", 0, "Successfully aggregated and loaded data into Gold Data Mart", None, None)
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
            error_record = logging.LogRecord("SessionPerformance", logging.ERROR, "", 0, "Distributed processing job failed in Spark engine", None, None)
            error_record.extra_data = {
                  "status": "FAILED",
                  "error_details": str(e),
                  "failed_partition": {"date": execution_date}
            }
            logger.handle(error_record)
            sys.exit(1) # Báo lỗi để Airflow đổi màu Đỏ (Failed) nhằm gửi Alert
            
      finally:
            if spark:
                  spark.stop()