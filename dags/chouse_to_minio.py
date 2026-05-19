from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.providers.trino.operators.trino import TrinoOperator
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

    export_events_to_s3 = ClickHouseOperator(
    task_id="export_events_to_s3",
    clickhouse_conn_id="clickhouse_finhouse",
    sql=f"""
        INSERT INTO FUNCTION s3(
            -- Dùng f-string để truyền các biến cấu hình Python cố định
            '{MINIO_ENDPOINT}/{MINIO_BUCKET}/bronze/events/fact_events/date={{{{ ds }}}}/events_{{{{ data_interval_start.hour }}}}.parquet',
            '{MINIO_ACCESS_KEY}',
            '{MINIO_SECRET_KEY}',
            'Parquet'
        )
        SETTINGS s3_truncate_on_insert = 1
        
        SELECT
            toString(event_id) AS event_id,
            * EXCEPT(event_id)
        FROM finhouse.fact_events
        WHERE occurred_at >= toDateTime('{{{{ data_interval_start.strftime("%Y-%m-%d %H:00:00") }}}}')
          AND occurred_at < toDateTime('{{{{ data_interval_end.strftime("%Y-%m-%d %H:00:00") }}}}')
    """
)
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

    sync_trino_events = TrinoOperator(
        task_id="sync_trino_events",
        trino_conn_id="trino_finhouse",
        sql="CALL hive.system.sync_partition_metadata('metadata', 'fact_events', 'ADD')"
    )



    bronze_to_silver_transform = TrinoOperator(
        task_id="bronze_to_silver_transform",
        trino_conn_id="trino_finhouse",
        sql=
            [
            f"""
                DELETE FROM hive.silver.fact_enriched_events 
                WHERE date = '{{{{ ds }}}}'
            """,
            f"""
                INSERT INTO hive.silver.fact_enriched_events
                SELECT 
                    e.event_id,
                    from_unixtime(e.occurred_at) AS occurred_at,
                    e.event_type,
                    e.platform,
                    e.user_id,
                    u.username,
                    u.full_name,
                    e.product_id,
                    p.name AS product_name,
                    p.price AS product_price,
                    c.name AS category_name,
                    s.store_name,
                    e.date
                FROM hive.metadata.fact_events e
                LEFT JOIN hive.metadata.users u ON e.user_id = u.user_id AND u.date = '{{{{ ds }}}}'
                LEFT JOIN hive.metadata.products p ON e.product_id = p.product_id AND p.date = '{{{{ ds }}}}'
                LEFT JOIN hive.metadata.categories c ON p.category_id = cast(c.category_id as bigint) AND c.date = '{{{{ ds }}}}'
                LEFT JOIN hive.metadata.stores s ON p.store_id = s.store_id AND s.date = '{{{{ ds }}}}'
                WHERE e.date = '{{{{ ds }}}}'
            """
            ]
    )

    spark_silver_to_gold = SparkSubmitOperator(
        task_id="spark_silver_to_gold",
        application="/opt/airflow/dags/src/spark_silver_to_gold.py",
        conn_id="spark_default",
        # Truyền đúng ngày chạy {{ ds }} và giờ chạy {{ data_interval_start.hour }} vào script
        application_args=["{{ ds }}", "{{ data_interval_start.hour }}"],
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        executor_memory="1g",
        driver_memory="1g",
    )
    sync_trino_gold = TrinoOperator(
        task_id="sync_trino_gold",
        trino_conn_id="trino_finhouse",
        sql="CALL hive.system.sync_partition_metadata('gold', 'dm_store_performance', 'RECONCILE')"
    )

    export_events_to_s3 >> sync_trino_events >> delete_archived_data >> bronze_to_silver_transform >> spark_silver_to_gold >> sync_trino_gold