from datetime import datetime
import json
import logging
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# GỌI LOGGER TẬP TRUNG TỪ FILE UTILS
from logger_utils import get_logger

logger = get_logger("RFMSegments", "SparkTransformer")

if __name__ == "__main__":
      # Đọc tham số đầu vào từ Airflow orchestrator
      if len(sys.argv) < 3:
            record = logging.LogRecord("RFMSegments", logging.CRITICAL, "", 0, "Missing required arguments: execution_date and execution_hour", None, None)
            logger.handle(record)
            sys.exit(1)

      execution_date = sys.argv[1]  # Ví dụ: '2026-06-05'
      execution_hour = sys.argv[2]  # Ví dụ: '04'

      job_start_time = time.time()

      # INFO LOG: Đánh dấu khởi chạy Job tính toán phân tán
      init_record = logging.LogRecord("RFMSegments", logging.INFO, "", 0, f"Initializing Spark Silver-to-Gold Pure Hourly RFM Mart computation", None, None)
      init_record.extra_data = {
            "pipeline_context": {
                  "target_date": execution_date,
                  "target_hour": execution_hour,
                  "data_mart": "fact_rfm_segments"
            }
        }
      logger.handle(init_record)

      spark = None
      try:
            # 1. KHỞI TẠO SPARK SESSION KẾT NỐI MINIO
            spark = SparkSession.builder \
                  .appName(f"Finhouse-Gold-RFM-Segments-Hourly_{execution_date}_{execution_hour}") \
                  .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
                  .config("spark.hadoop.fs.s3a.access.key", "admin") \
                  .config("spark.hadoop.fs.s3a.secret.key", "supersecretpassword") \
                  .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                  .master("spark://spark-master:7077") \
                  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                  .getOrCreate()


            # 2. ĐỌC ĐÚNG 1 FOLDER PHÂN VÙNG GIỜ HIỆN TẠI TỪ TẦNG SILVER
            silver_path = f"s3a://finhouse-datalake/silver/wide_table_events/date={execution_date}/hour={execution_hour}"
            
            read_start_time = time.time()
            try:
                  df_silver = spark.read.parquet(silver_path)
                  input_row_count = df_silver.count()
                  
            except Exception as read_ex:
                  warn_record = logging.LogRecord("RFMSegments", logging.WARNING, "", 0, f"No input data found or path is missing in Silver layer for this specific hour", None, None)
                  warn_record.extra_data = {"target_path": silver_path, "error_details": str(read_ex)}
                  logger.handle(warn_record)
                  sys.exit(0) # Phân vùng trống thì dừng hòa bình
                  

            # 3. TÍNH TOÁN DỮ LIỆU TỔNG HỢP (PURE HOURLY RFM TÍNH THEO PHÚT)
            transform_start_time = time.time()
            
            # Neo mốc thời gian tại giây cuối cùng của giờ chạy (Ví dụ: 04:59:59) để tính Recency dạng phút
            execution_timestamp_str = f"{execution_date} {execution_hour}:59:59"
            
            # Giai đoạn 3.1: Tính toán chỉ số thô (Recency tính bằng PHÚT)
            df_raw_rfm = df_silver.groupBy("user_id") \
                  .agg(
                        F.floor(
                              (F.unix_timestamp(F.lit(execution_timestamp_str)) - F.unix_timestamp(F.max(F.when(F.col("event_type") == "PURCHASE", F.col("occurred_at"))))) / 60
                        ).alias("recency_value"),
                        F.sum(F.when(F.col("event_type") == "PURCHASE", 1).otherwise(0)).alias("frequency_value"),
                        F.sum(F.when(F.col("event_type") == "PURCHASE", F.col("product_price")).otherwise(0.0)).alias("monetary_value")
                  ) \
                  .na.fill({"recency_value": 9999}) # Nếu giờ này không mua gì, gán số phút thật lớn
            
            # Giai đoạn 3.2: Phân phối điểm số chuẩn hóa (Scores 1-5) bằng NTILE
            window_r = Window.orderBy(F.col("recency_value").desc()) # Mua càng gần thời điểm cuối giờ điểm càng cao
            window_f = Window.orderBy(F.col("frequency_value").asc())
            window_m = Window.orderBy(F.col("monetary_value").asc())
            
            df_scores = df_raw_rfm \
                  .withColumn("r_score", F.ntile(5).over(window_r)) \
                  .withColumn("f_score", F.ntile(5).over(window_f)) \
                  .withColumn("m_score", F.ntile(5).over(window_m))
            
            # Giai đoạn 3.3: Tạo tổ hợp chuỗi rfm_cell
            df_cell = df_scores.withColumn(
                  "rfm_cell",
                  F.concat(F.col("r_score").cast("string"), F.col("f_score").cast("string"), F.col("m_score").cast("string"))
            )
            
            # Giai đoạn 3.4: Phân lớp nhãn nhóm khách hàng
            df_segmented = df_cell.withColumn(
                  "segment_name",
                  F.when(F.col("r_score").isin([4, 5]) & F.col("f_score").isin([4, 5]), "Hourly Champions")
                   .when(F.col("r_score").isin([3, 4, 5]) & F.col("f_score").isin([3, 4, 5]), "Hourly Active Users")
                   .when(F.col("r_score").isin([4, 5]) & (F.col("f_score") == 1), "Hourly New Buyers")
                   .otherwise("Hourly Low Active")
            )
            
            # Đồng bộ cấu trúc Schema đầu ra chuẩn khớp 100% với Trino Gold DDL
            df_gold_rfm = df_segmented.select(
                  F.col("user_id").alias("customer_id"),
                  F.col("recency_value").cast("int"),
                  F.col("frequency_value").cast("int"),
                  F.col("monetary_value").cast("double"),
                  F.col("r_score").cast("int"),
                  F.col("f_score").cast("int"),
                  F.col("m_score").cast("int"),
                  "rfm_cell",
                  "segment_name",
                  F.lit(execution_date).alias("date"),
                  F.lit(execution_hour).alias("hour")
            )


            # 4. GHI XUỐNG TẦNG GOLD VỚI CHẾ ĐỘ DYNAMIC PARTITION OVERWRITE
            gold_path = "s3a://finhouse-datalake/gold/fact_rfm_segments/"
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            
            output_row_count = df_gold_rfm.count()

            df_gold_rfm.write \
                  .mode("overwrite") \
                  .partitionBy("date", "hour") \
                  .parquet(gold_path)

            total_duration = time.time() - job_start_time
            
            # INFO LOG: Báo cáo hiệu năng dữ liệu hằng giờ
            success_record = logging.LogRecord("RFMSegments", logging.INFO, "", 0, "Successfully aggregated and loaded pure hourly RFM data into Gold Layer", None, None)
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
            error_record = logging.LogRecord("RFMSegments", logging.ERROR, "", 0, "Pure hourly RFM processing job failed in Spark engine", None, None)
            error_record.extra_data = {
                  "status": "FAILED",
                  "error_details": str(e),
                  "failed_partition": {"date": execution_date, "hour": execution_hour}
            }
            logger.handle(error_record)
            sys.exit(1)
            
      finally:
            if spark:
                  spark.stop()