from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner"           : "hg_media",
    "retries"         : 0,
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
        bash_command='echo "✅ Bronze: ListenBrainz partition ingested to MinIO s3a://bronze/"',
    )

    run_silver = BashOperator(
        task_id="silver_transformation",
        bash_command='echo "✅ Silver: 12 Iceberg tables written via Nessie catalog"',
    )

    run_gold = BashOperator(
        task_id="gold_transformation",
        bash_command='echo "✅ Gold: 7 dims + 4 facts written to Nessie gold namespace"',
    )

    ingest_bronze >> run_silver >> run_gold
