from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner"           : "hg_media",
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="hg_media_eod_pipeline",
    default_args=default_args,
    description="HG Media Daily EOD Pipeline - Bronze → Silver → Gold",
    schedule_interval="0 23 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["hg_media", "lakehouse", "eod"],
) as dag:

    ingest_bronze = BashOperator(
        task_id="ingest_listenbrainz_bronze",
        bash_command="docker exec extraction python3 /datn/ingestion/jobs/01_ingest_listenbrainz.py",
    )

    run_silver = BashOperator(
        task_id="silver_transformation",
        bash_command="docker exec transformation python3 /datn/transformation/jobs/01_silver_transformation.py",
    )

    run_gold = BashOperator(
        task_id="gold_transformation",
        bash_command="docker exec transformation python3 /datn/transformation/jobs/02_gold_transformation.py",
    )

    ingest_bronze >> run_silver >> run_gold
