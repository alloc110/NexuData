from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'hung_thien_loc',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # Không nên retry cho việc init metadata để tránh deadlock
}

with DAG(
    dag_id="generate_init_data",
    default_args=default_args,
    description='Khởi tạo metadata cho PostgreSQL Finhouse',
    schedule_interval=None, # Chỉ chạy thủ công khi cần
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['metadata', 'init'],
) as dag:

    generate_metadata = BashOperator(
        task_id="generate_init_metadata",
        # Thêm biến môi trường nếu cần thiết
        bash_command="python /opt/airflow/dags/src/init_db.py",
        # Đảm bảo Airflow ghi lại log đầy đủ
        append_env=True,
    )

    transform_to_mino = BashOperator(
        task_id="transform_to_mino",
        bash_command="python /opt/airflow/dags/src/postgresql_to_minio.py",
        # Đảm bảo Airflow ghi lại log đầy đủ
        append_env=True,
    )
    metadata_tables = ['users', 'categories', 'stores', 'products']
    sync_trino_tasks = []

    for table_name in metadata_tables:
        sync_task = TrinoOperator(
            task_id=f"sync_trino_{table_name}",
            trino_conn_id="trino_finhouse", # Sử dụng connection chúng ta đã cấu hình
            sql=f"CALL hive.system.sync_partition_metadata('metadata', '{table_name}', 'ADD')"
        )
        sync_trino_tasks.append(sync_task)

    truncate_tables = PostgresOperator(
        task_id="truncate_tables",
        postgres_conn_id="postgres_finhouse",
        sql="""
            TRUNCATE TABLE
            products,
            stores,
            categories,
            users
            RESTART IDENTITY CASCADE;
        """
    )
    truncate_tables >> generate_metadata >> transform_to_mino >> sync_trino_tasks