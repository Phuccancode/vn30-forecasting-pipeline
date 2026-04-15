from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "owner": "phoenix",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="vn30_backfill_pipeline",
    description="Manual VN30 backfill pipeline: Bronze(backfill) -> Silver(full) -> Gold",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 4, 15, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["vn30", "medallion", "backfill"],
) as dag:
    bronze_backfill = BashOperator(
        task_id="bronze_backfill",
        bash_command=(
            "python /opt/airflow/scripts/ingestion/extract_vnstock.py "
            "--mode backfill "
            "--start-date {{ dag_run.conf.get('start_date', '2025-01-01') if dag_run else '2025-01-01' }} "
            "--date {{ dag_run.conf.get('end_date', ds) if dag_run else ds }} "
            "--batch-size {{ dag_run.conf.get('batch_size', 50) if dag_run else 50 }} "
            "--upload-workers {{ dag_run.conf.get('upload_workers', 8) if dag_run else 8 }}"
        ),
    )

    silver_full = BashOperator(
        task_id="silver_full",
        bash_command=(
            "python /opt/airflow/scripts/transform/silver_transform.py "
            "--mode full "
            "--start-date {{ dag_run.conf.get('start_date', '2025-01-01') if dag_run else '2025-01-01' }} "
            "--target-date {{ dag_run.conf.get('end_date', ds) if dag_run else ds }}"
        ),
    )

    gold_load = BashOperator(
        task_id="gold_load",
        bash_command=(
            "python /opt/airflow/scripts/load/gold_load.py "
            "--start-date {{ dag_run.conf.get('start_date', '2025-01-01') if dag_run else '2025-01-01' }} "
            "--end-date {{ dag_run.conf.get('end_date', ds) if dag_run else ds }} "
            "--batch-days {{ dag_run.conf.get('gold_batch_days', 31) if dag_run else 31 }}"
        ),
    )

    bronze_backfill >> silver_full >> gold_load
