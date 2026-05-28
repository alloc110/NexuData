from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

MINIO_ENDPOINT = "http://minio:9000"
MINIO_BUCKET = "finhouse-datalake"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "supersecretpassword"

default_args = {
    "owner": "hung_thien_loc",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="clickhouse_to_minio_v2",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 5, 1),
    catchup=False,
) as dag:

    # TASK 1: Cập nhật ép kiểu tường minh theo cấu trúc bảng mới
    export_events_to_s3 = ClickHouseOperator(
        task_id="export_events_to_s3",
        clickhouse_conn_id="clickhouse_finhouse",
        sql=f"""
            INSERT INTO FUNCTION s3(
                '{MINIO_ENDPOINT}/{MINIO_BUCKET}/bronze/events/fact_events/date={{{{ ds }}}}/hour={{{{ data_interval_start.strftime("%H") }}}}/data.parquet',
                '{MINIO_ACCESS_KEY}',
                '{MINIO_SECRET_KEY}',
                'Parquet'
            )
            SETTINGS s3_truncate_on_insert = 1
            
            SELECT
                toString(event_id) AS event_id,
                CAST(user_id AS Int64) AS user_id,
                CAST(product_id AS Int64) AS product_id,
                toString(event_type) AS event_type,
                toString(platform) AS platform,
                toUnixTimestamp(occurred_at) AS occurred_at,
                '{{{{ ds }}}}' AS date  
            FROM finhouse.fact_events
            WHERE toDate(occurred_at) = toDate('{{{{ ds }}}}')
        """
    )

    # TASK 2: Dọn dẹp dữ liệu trong ClickHouse
    delete_archived_data = ClickHouseOperator(
        task_id="delete_archived_data",
        clickhouse_conn_id="clickhouse_finhouse",
        sql="""
            ALTER TABLE finhouse.fact_events
            DELETE
            WHERE occurred_at >= toDateTime('{{ data_interval_start.strftime("%Y-%m-%d %H:00:00") }}')
              AND occurred_at < toDateTime('{{ data_interval_end.strftime("%Y-%m-%d %H:00:00") }}')
        """
    )

    # TASK 3: Trino đồng bộ phân vùng Bronze mới nạp
    sync_trino_events = SQLExecuteQueryOperator(
        task_id="sync_trino_events",
        conn_id="trino_finhouse",
        sql="CALL hive.system.sync_partition_metadata('metadata', 'fact_events', 'ADD')"
    )

    # TASK 4: Biến đổi dữ liệu sang Silver - Sửa logic WHERE tránh lệch múi giờ hệ thống
    bronze_to_silver_transform = SQLExecuteQueryOperator(
        task_id="bronze_to_silver_transform",
        conn_id="trino_finhouse",
        split_statements=True,
        sql=[
            f"""
                DELETE FROM hive.silver.fact_enriched_events 
                WHERE date = '{{{{ ds }}}}' AND hour = '{{{{ data_interval_start.strftime("%H") }}}}'
            """,
            f"""
                INSERT INTO hive.silver.fact_enriched_events
                SELECT 
                    e.event_id,
                    CAST(from_unixtime(e.occurred_at) AS timestamp(3)) AS occurred_at,
                    e.event_type,
                    e.platform,
                    CAST(e.user_id AS bigint) AS user_id,
                    u.username,
                    u.full_name,
                    CAST(e.product_id AS bigint) AS product_id,
                    p.name AS product_name,
                    p.price AS product_price,
                    c.name AS category_name,
                    s.store_name,
                    e.date,
                    '{{{{ data_interval_start.strftime("%H") }}}}' AS hour
                FROM hive.metadata.fact_events e
                LEFT JOIN hive.metadata.users u ON e.user_id = u.user_id
                LEFT JOIN hive.metadata.products p ON e.product_id = p.product_id
                LEFT JOIN hive.metadata.categories c ON p.category_id = cast(c.category_id as bigint)
                LEFT JOIN hive.metadata.stores s ON p.store_id = s.store_id

                WHERE e.date = '{{{{ ds }}}}'
            """
        ]
    )
    # TASK 5: Spark đọc file từ tầng Silver (Truyền tham số giờ chuẩn %H)
    spark_silver_to_gold = SparkSubmitOperator(
        task_id="spark_silver_to_gold",
        conn_id="spark_standalone_cluster",
        application="/opt/airflow/dags/src/spark_silver_to_gold.py",
        application_args=["{{ ds }}", "{{ data_interval_start.strftime('%H') }}"],
        conf={
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        }
    )

    # TASK 6: Trino đồng bộ siêu dữ liệu tầng Gold
    sync_trino_gold = SQLExecuteQueryOperator(
        task_id="sync_trino_gold",
        conn_id="trino_finhouse",
        sql="CALL hive.system.sync_partition_metadata('gold', 'dm_store_performance', 'FULL')"
    )

    # Mạch pipeline dữ liệu
    export_events_to_s3 >> sync_trino_events >> delete_archived_data >> bronze_to_silver_transform >> spark_silver_to_gold >> sync_trino_gold