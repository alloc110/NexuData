from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

def main():
    # 1. Khởi tạo Spark Session
    # Master 'spark://spark-master:7077' khớp với hostname trong docker-compose của bạn
    spark = SparkSession.builder \
        .appName("Finhouse-Spark-Test") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    print(">>> Spark Session đã khởi tạo thành công!")

    # 2. Tạo dữ liệu mẫu (Giả lập dữ liệu giao dịch tài chính)
    data = [
        ("TCB", "BUY", 1000, 35.5),
        ("VNM", "SELL", 500, 72.0),
        ("FPT", "BUY", 200, 115.2),
        ("TCB", "BUY", 300, 36.0),
        ("VNM", "BUY", 100, 71.5),
    ]
    columns = ["ticker", "side", "quantity", "price"]
    
    df = spark.createDataFrame(data, columns)

    print(">>> Dữ liệu gốc:")
    df.show()

    # 3. Thực hiện transformation (Tính tổng giá trị giao dịch theo ticker)
    print(">>> Tổng giá trị giao dịch (Quantity * Price) mỗi Ticker:")
    result_df = df.withColumn("total_value", col("quantity") * col("price")) \
                  .groupBy("ticker") \
                  .sum("total_value") \
                  .orderBy(desc("sum(total_value)"))

    result_df.show()

    # 4. Test khả năng ghi file (Ghi kết quả ra thư mục tạm trong container)
    output_path = "/tmp/spark_test_output"
    print(f">>> Đang ghi kết quả test vào: {output_path}")
    
    # Dùng overwrite để có thể chạy lại file nhiều lần không bị lỗi tồn tại thư mục
    result_df.write.mode("overwrite").csv(output_path)
    
    print(">>> Ghi file thành công!")
    
    # Dừng Spark
    spark.stop()
    print(">>> Spark Job kết thúc.")

if __name__ == "__main__":
    main()