from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .appName("ClickHouse-Postgres-Aggregation") \
        .config("spark.jars.ivy", "/tmp/.ivy2") \
        .getOrCreate()

    # --- THÔNG SỐ KẾT NỐI ---
    pg_url = "jdbc:postgresql://postgres:5432/finhouse"
    pg_props = {"user": "finhouse", "password": "finhouse", "driver": "org.postgresql.Driver"}

    ch_url = "jdbc:clickhouse://clickhouse:8123/finhouse"
    ch_props = {"user": "admin", "password": "admin", "driver": "com.clickhouse.jdbc.ClickHouseDriver"}

    # --- 1. ĐỌC DỮ LIỆU ---
    df_products = spark.read.jdbc(pg_url, "products", properties=pg_props)

    # FIX: Thêm event_id vào câu SELECT hoặc dùng * để lấy hết các cột từ ClickHouse
    event_query = "(SELECT user_id, event_id, product_id, event_type, occurred_at FROM finhouse.fact_events) AS events"
    df_events = spark.read.jdbc(ch_url, event_query, properties=ch_props)

    # --- 2. JOIN & TRANSFORM ---
    # Ép kiểu dữ liệu nếu cần (ClickHouse UInt32 vs Postgres Serial/Int)
    df_events = df_events.withColumn("product_id", F.col("product_id").cast("int"))
    
    df_joined = df_events.join(df_products, "product_id", "inner")

    # --- 3. TÍNH TOÁN (4 mục tiêu bạn cần) ---
    # Ví dụ: Tính báo cáo doanh thu theo sản phẩm
    df_report = df_joined.groupBy("name") \
        .agg(
            F.sum("price").alias("total_revenue"), 
            F.count("event_id").alias("total_orders") # Giờ đã có event_id nên sẽ không lỗi
        ) \
        .orderBy(F.desc("total_revenue"))

    # --- 4. GHI DỮ LIỆU VỀ POSTGRES MART ---
    print(">>> Đang ghi dữ liệu vào Postgres Data Mart...")
    df_report.write.jdbc(
        url=pg_url, 
        table="mart_product_sales", 
        mode="overwrite", 
        properties=pg_props
    )

    print(">>> SUCCESS: Dữ liệu đã sẵn sàng cho Metabase!")
    spark.stop()

if __name__ == "__main__":
    main()