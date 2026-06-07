from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator  # Chuyển sang dùng toán tử rẽ nhánh
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.providers.trino.operators.trino import TrinoOperator

# Khởi tạo logger tích hợp của Airflow Task
logger = logging.getLogger("airflow.task")

# =================================================================
# CALLBACK ĐO THỜI GIAN CHẠY TỰ ĐỘNG (ĐƯA LÊN ĐẦU FILE ĐỂ TRÁNH LỖI)
# =================================================================
def log_task_execution_time(context):
    """Callback tự động tính toán thời gian thực thi của từng Task thành công"""
    ti = context['task_instance']
    task_id = ti.task_id
    dag_id = ti.dag_id
    
    if ti.duration:
        duration_seconds = ti.duration
    elif ti.start_date and ti.end_date:
        duration_seconds = (ti.end_date - ti.start_date).total_seconds()
    else:
        duration_seconds = 0.0

    performance_log = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "level": "INFO",
        "component": "AirflowOrchestrator",
        "message": f"Task performance metrics captured for {task_id}",
        "metrics": {
            "dag_id": dag_id,
            "task_id": task_id,
            "execution_status": "SUCCESS",
            "duration_seconds": round(duration_seconds, 4)
        }
    }
    logger.info(json.dumps(performance_log))

# ==========================================
# CẤU HÌNH PIPELINE & DEFAULT ARGS
# ==========================================
MINIO_ENDPOINT = "http://minio:9000"
MINIO_BUCKET = "finhouse-datalake"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "supersecretpassword"

default_args = {
    "owner": "hung_thien_loc",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_success_callback": log_task_execution_time, 
}

# =================================================================
# LOGIC KIỂM TRA ĐẦU VÀO VÀ ĐIỀU HƯỚNG NHÁNH
# =================================================================
def check_trino_and_route_branch():
    """
    Hàm quyết định đường đi của dòng chảy dữ liệu.
    Trả về đúng task_id mà bạn muốn Airflow kích hoạt tiếp theo.
    """
    hook = TrinoHook(trino_conn_id="trino_finhouse")
    
    try:
        sql = "SELECT COUNT(*) FROM hive.metadata.users"
        record = hook.get_first(sql)
        
        if record and record[0] > 0:
            logger.info(f"⚠️ Trino đã có sẵn {record[0]} dòng dữ liệu metadata.")
            logger.info("👉 Xử lý nhảy cóc: BỎ QUA chuỗi khởi tạo mẫu -> BẮT ĐẦU thẳng vào Pipeline Event!")
            return "export_events_to_s3"  # <--- ĐÂY RỒI: Tên task bạn muốn nhảy cóc tới
            
        logger.info("ℹ️ Trino trống. Tiến hành kích hoạt chuỗi init_db dữ liệu mẫu tuần tự...")
        return "truncate_tables"  # Chạy nhánh tuần tự từ Postgres
        
    except Exception as e:
        logger.warning(f"ℹ️ Không thể kết nối Trino hoặc bảng chưa tạo: {e}")
        logger.info("🚀 Xem như hệ thống mới tinh -> Kích hoạt chuỗi init_db...")
        return "truncate_tables"


# =================================================================
# ĐỊNH NGHĨA DAG PIPELINE
# =================================================================
with DAG(
    dag_id="main_pipeline",
    default_args=default_args,
    schedule="@hourly",              
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,               
    tags=["ecommerce", "analytics", "medallion"]
) as dag:

    # -----------------------------------------------------------------
    # TASK 0: Trọng tài điều hướng (Thay thế cho ShortCircuit cũ)
    # -----------------------------------------------------------------
    check_trino_data = BranchPythonOperator(
        task_id="check_trino_data",
        python_callable=check_trino_and_route_branch
    )

    # -----------------------------------------------------------------
    # NHÁNH KHỞI TẠO (CHỈ CHẠY KHI TRINO TRỐNG)
    # -----------------------------------------------------------------
    truncate_tables = PostgresOperator(
        task_id="truncate_tables",
        postgres_conn_id="postgres_finhouse",
        sql="""
            TRUNCATE TABLE products, stores, categories, users RESTART IDENTITY CASCADE;
        """
    )

    generate_metadata = BashOperator(
        task_id="generate_init_metadata",
        bash_command="python /opt/airflow/dags/src/init_db.py",
        append_env=True,
    )

    transform_to_mino = BashOperator(
        task_id="transform_to_mino",
        bash_command="python /opt/airflow/dags/src/postgresql_to_minio.py",
        append_env=True,
    )

    # Vòng lặp tạo Task đồng bộ siêu dữ liệu Trino cho phần Metadata ban đầu
    metadata_tables = ['users', 'categories', 'stores', 'products']
    sync_trino_tasks = []
    for table_name in metadata_tables:
        sync_task = TrinoOperator(
            task_id=f"sync_trino_{table_name}",
            trino_conn_id="trino_finhouse",
            sql=f"CALL hive.system.sync_partition_metadata('metadata', '{table_name}', 'ADD')"
        )
        sync_trino_tasks.append(sync_task)


    # -----------------------------------------------------------------
    # NHÁNH CHÍNH / HOẶC ĐIỂM HỘI QUÂN NHẢY CÓC (PIPELINE EVENT HẰNG GIỜ)
    # -----------------------------------------------------------------
    export_events_to_s3 = ClickHouseOperator(
        task_id="export_events_to_s3",
        clickhouse_conn_id="clickhouse_finhouse",
        # QUAN TRỌNG: none_failed đảm bảo task này vẫn chạy khi nhánh init ở trên bị skip hoàn toàn
        trigger_rule="none_failed", 
        sql=f"""
            INSERT INTO FUNCTION s3(
                '{MINIO_ENDPOINT}/{MINIO_BUCKET}/bronze/events/date={{{{ ds }}}}/hour={{{{ data_interval_start.strftime("%H") }}}}/data.parquet',
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
            FROM finhouse.events
            WHERE occurred_at >= toDateTime('{{{{ data_interval_start.strftime("%Y-%m-%d %H:00:00") }}}}')
              AND occurred_at < toDateTime('{{{{ data_interval_end.strftime("%Y-%m-%d %H:00:00") }}}}')
        """
    )

    sync_trino_events = SQLExecuteQueryOperator(
        task_id="sync_trino_events",
        conn_id="trino_finhouse",
        sql="CALL hive.system.sync_partition_metadata('metadata', 'events', 'ADD')"
    )

    delete_archived_data = ClickHouseOperator(
        task_id="delete_archived_data",
        clickhouse_conn_id="clickhouse_finhouse",
        sql="""
            ALTER TABLE finhouse.events
            DELETE
            WHERE occurred_at >= toDateTime('{{ data_interval_start.strftime("%Y-%m-%d %H:00:00") }}')
              AND occurred_at < toDateTime('{{ data_interval_end.strftime("%Y-%m-%d %H:00:00") }}')
            SETTINGS mutations_sync = 1;
        """
    )

    bronze_to_silver_transform = SQLExecuteQueryOperator(
        task_id="bronze_to_silver_transform",
        conn_id="trino_finhouse",
        split_statements=True,
        sql=[
            """
                DELETE FROM hive.silver.wide_table_events 
                WHERE date = '{{ ds }}' AND hour = '{{ data_interval_start.strftime("%H") }}'
            """,
            """
                INSERT INTO hive.silver.wide_table_events
                SELECT 
                    e.event_id, CAST(from_unixtime(e.occurred_at) AS timestamp(3)) AS occurred_at,
                    e.event_type, e.platform, CAST(e.user_id AS bigint) AS user_id,
                    u.username, u.full_name, CAST(e.product_id AS bigint) AS product_id,
                    u.email, u.created_at AS user_created_at,
                    p.name AS product_name, p.price AS product_price, c.name AS category_name,
                    s.store_name, e.date, '{{ data_interval_start.strftime("%H") }}' AS hour
                FROM hive.metadata.events e
                LEFT JOIN hive.metadata.users u ON e.user_id = u.user_id
                LEFT JOIN hive.metadata.products p ON e.product_id = p.product_id
                LEFT JOIN hive.metadata.categories c ON p.category_id = cast(c.category_id as bigint)
                LEFT JOIN hive.metadata.stores s ON p.store_id = s.store_id
                WHERE e.date = '{{ ds }}' AND e.hour = '{{ data_interval_start.strftime("%H") }}'
            """
        ]
    )

    customer_profile = SparkSubmitOperator(
        task_id="spark_customer_profile",
        conn_id="spark_standalone_cluster",
        application="/opt/airflow/dags/src/spark/customer_profile.py",
        application_args=["{{ ds }}", "{{ data_interval_start.strftime('%H') }}"],
        py_files="/opt/airflow/plugins/utils/logger_utils.py",
    )

    funnel_conversion_hourly = SparkSubmitOperator(
        task_id="spark_funnel_conversion_hourly",
        conn_id="spark_standalone_cluster",
        application="/opt/airflow/dags/src/spark/funnel_conversion_hourly.py",
        application_args=["{{ ds }}", "{{ data_interval_start.strftime('%H') }}"],
        py_files="/opt/airflow/plugins/utils/logger_utils.py",
    )
    
    rfm_segments = SparkSubmitOperator(
        task_id="spark_rfm_segments",
        conn_id="spark_standalone_cluster",
        application="/opt/airflow/dags/src/spark/rfm_segments.py",
        application_args=["{{ ds }}", "{{ data_interval_start.strftime('%H') }}"],
        py_files="/opt/airflow/plugins/utils/logger_utils.py",
    )
    
    session_performance_hourly = SparkSubmitOperator(
        task_id="spark_session_performance_hourly",
        conn_id="spark_standalone_cluster",
        application="/opt/airflow/dags/src/spark/session_performance_hourly.py",
        application_args=["{{ ds }}", "{{ data_interval_start.strftime('%H') }}"],
        py_files="/opt/airflow/plugins/utils/logger_utils.py",
    )
    
    
    sync_trino_gold = SQLExecuteQueryOperator(
        task_id="sync_trino_gold",
        conn_id="trino_finhouse",
        sql=[
            "CALL hive.system.sync_partition_metadata('gold', 'dim_customer_profile', 'FULL')",
            "CALL hive.system.sync_partition_metadata('gold', 'fact_funnel_conversion', 'FULL')",
            "CALL hive.system.sync_partition_metadata('gold', 'fact_rfm_segments', 'FULL')",
            "CALL hive.system.sync_partition_metadata('gold', 'fact_session_performance', 'FULL')"
        ]
    )

    # =================================================================
    # THIẾT LẬP MẠCH ĐIỀU HƯỚNG RẼ NHÁNH SẠCH SẼ
    # =================================================================
    spark_tasks = [
        customer_profile,
        funnel_conversion_hourly,
        rfm_segments, 
        session_performance_hourly
    ]
    
    # 1. Khai báo 2 đầu ra của bảng phân phối nhánh
    check_trino_data >> [truncate_tables, export_events_to_s3]
    
    # 2. Luồng đi tuần tự của nhánh Khởi tạo (Chạy khi Trino rỗng)
    truncate_tables >> generate_metadata >> transform_to_mino >> sync_trino_tasks >> export_events_to_s3
    
    # 3. Luồng đi của Pipeline Event hằng giờ (Chạy liên tục từ điểm hội quân)
    export_events_to_s3 >> sync_trino_events >> delete_archived_data >> bronze_to_silver_transform >> spark_tasks >> sync_trino_gold