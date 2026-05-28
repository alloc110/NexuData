from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

if __name__ == "__main__":
    # Nhận 2 tham số thời gian từ Airflow truyền vào
    execution_date = sys.argv[1] # Ví dụ: '2026-05-19'
    execution_hour = sys.argv[2] # Ví dụ: '15' (Định dạng tiếng từ 0-23)

    # 1. Khởi tạo Spark Session kết nối MinIO
    spark = SparkSession.builder \
        .appName(f"Spark_Silver_To_Gold_{execution_date}_{execution_hour}") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "supersecretpassword") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # 2. Đọc CHÍNH XÁC phân vùng Giờ từ tầng Silver
    silver_path = f"s3a://finhouse-datalake/silver/fact_enriched_events/date={execution_date}/hour={execution_hour}"
    df_silver = spark.read.parquet(silver_path)

    # 3. Tính toán dữ liệu tổng hợp (Data Mart) cho tầng Gold
    df_gold = df_silver.groupBy("store_name", "category_name") \
        .agg(
            F.count(F.when(F.col("event_type") == "VIEW_PRODUCT", 1)).alias("total_views"),
            F.count(F.when(F.col("event_type") == "PURCHASE", 1)).alias("total_purchases"),
            F.sum(F.when(F.col("event_type") == "PURCHASE", F.col("product_price")).otherwise(0)).alias("total_revenue")
        ) \
        .withColumn("conversion_rate", F.when(F.col("total_views") == 0, 0).otherwise(F.col("total_purchases") / F.col("total_views")))\
        .withColumn("date", F.lit(execution_date)) \
        .withColumn("hour", F.lit(execution_hour))

    # 4. Ghi xuống tầng Gold (Sử dụng chế độ gộp phân vùng thông minh)
    gold_path = "s3a://finhouse-datalake/gold/dm_store_performance/"
    
    # Cấu hình dynamic partition overwrite để tránh xóa nhầm các giờ khác trong ngày
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    df_gold.write \
        .mode("overwrite") \
        .partitionBy("date", "hour") \
        .parquet(gold_path)

    spark.stop()