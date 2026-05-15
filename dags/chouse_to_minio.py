from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from datetime import datetime, timedelta

MINIO_ENDPOINT = "http://minio:9000"
MINIO_BUCKET = "finhouse-archive"
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

    archive_to_minio = ClickHouseOperator(
        task_id="export_events_to_s3",
        clickhouse_conn_id="clickhouse_finhouse",
        sql=f"""
            INSERT INTO FUNCTION s3(
                '{MINIO_ENDPOINT}/{MINIO_BUCKET}/events_{{{{ ds }}}}_{{{{ data_interval_start.hour }}}}.parquet',
                '{MINIO_ACCESS_KEY}',
                '{MINIO_SECRET_KEY}',
                'Parquet'
            )
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

    archive_to_minio >> delete_archived_data