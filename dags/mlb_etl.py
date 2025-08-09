from airflow import DAG
from airflow.providers.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "mlb_daily_etl",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # 6 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["mlb"],
)

update_schedule_task = KubernetesPodOperator(
    task_id="update_schedule",
    name="mlb-update-schedule",
    namespace="airflow",
    image="your-registry/sportsflow-mlb:{{ var.value.image_tag }}",
    arguments=["--script", "update_schedule", "--date", "{{ ds }}"],
    dag=dag,
)

ingest_games_task = KubernetesPodOperator(
    task_id="ingest_games",
    name="mlb-ingest-games",
    namespace="airflow",
    image="your-registry/sportsflow-mlb:{{ var.value.image_tag }}",
    arguments=["--script", "ingest_games", "--date", "{{ ds }}"],
    dag=dag,
)

update_schedule_task >> ingest_games_task
