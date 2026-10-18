from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Starter DAG: replace the echo task with your Python extraction and dbt commands.
with DAG(
    dag_id="sncf_lakehouse_medallion",
    description="Starter orchestration for SNCF public data: Bronze -> Silver -> Gold",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["sncf", "lakehouse", "medallion"],
) as dag:
    extract_bronze = BashOperator(
        task_id="extract_snc_frequentation_to_bronze",
        bash_command="echo 'TODO: run Python ingestion and write raw data to MinIO bronze/'",
    )
    transform_silver = BashOperator(
        task_id="transform_silver_with_dbt",
        bash_command="echo 'TODO: run dbt build against Trino/Polaris'",
    )
    build_gold = BashOperator(
        task_id="build_gold_marts",
        bash_command="echo 'TODO: build Gold dashboard tables'",
    )
    extract_bronze >> transform_silver >> build_gold
