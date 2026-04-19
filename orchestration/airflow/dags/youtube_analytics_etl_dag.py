from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/project"

default_args = {
    "owner": "govind",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="youtube_multi_channel_analytics_etl",
    default_args=default_args,
    description="Multi-channel YouTube analytics ETL pipeline",
    start_date=datetime(2026, 4, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["youtube", "etl", "analytics", "postgres"],
) as dag:

    run_pipeline = BashOperator(
        task_id="run_full_pipeline",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "export POSTGRES_HOST=postgres && "
            "python -m etl.run_pipeline"
        ),
    )

    run_pipeline
