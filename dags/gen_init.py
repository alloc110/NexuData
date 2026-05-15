from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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
        bash_command="python /opt/airflow/dags/init_db.py",
        # Đảm bảo Airflow ghi lại log đầy đủ
        append_env=True,
    )

    generate_metadata