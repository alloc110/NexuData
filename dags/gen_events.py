from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="generate_fake_events",
    start_date=datetime(2025, 1, 1),
    schedule_interval="* * * * *",
    catchup=False
):

    generate_events = BashOperator(
        task_id="generate_events",
        bash_command="python /opt/airflow/dags/src/random_events.py"
    )