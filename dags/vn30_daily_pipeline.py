from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "phoenix",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="vn30_daily_pipeline",
    description="VN30 medallion pipeline orchestration: Bronze -> Silver -> Gold -> ML",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 4, 10, tz="Asia/Ho_Chi_Minh"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["vn30", "medallion", "day7"],
) as dag:
    bronze_extract = BashOperator(
        task_id="bronze_extract",
        bash_command=(
            "python /opt/airflow/scripts/ingestion/extract_vnstock.py "
            "--mode daily --date {{ ds }}"
        ),
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command="python /opt/airflow/scripts/transform/silver_transform.py",
    )

    gold_load = BashOperator(
        task_id="gold_load",
        bash_command=(
            'if [ -f /opt/airflow/scripts/load/gold_load.py ]; then '
            'python /opt/airflow/scripts/load/gold_load.py; '
            'else echo "Day 8 pending: scripts/load/gold_load.py not found. Placeholder success."; fi'
        ),
    )

    ml_inference = BashOperator(
        task_id="ml_inference",
        bash_command=(
            'if [ -f /opt/airflow/scripts/ml/ml_inference.py ]; then '
            'python /opt/airflow/scripts/ml/ml_inference.py --date {{ ds }}; '
            'else echo "Day 9-10 pending: scripts/ml/ml_inference.py not found. Placeholder success."; fi'
        ),
    )

    bronze_extract >> silver_transform >> gold_load >> ml_inference
