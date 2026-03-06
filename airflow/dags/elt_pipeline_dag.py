"""
ELT Pipeline DAG for the E-Commerce Data Warehouse.

Schedule: Daily at 6 AM UTC
Tasks:
  1. load_raw_data  - Run the Python loader to ingest CSVs into DuckDB
  2. dbt_run        - Execute dbt models (staging -> marts)
  3. dbt_test       - Run dbt tests to validate data quality
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
SCRIPTS_DIR = "/opt/airflow/scripts"

default_args = {
    "owner": "soumya",
    "depends_on_past": False,
    "email": ["Vemparsy@mail.uc.edu"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_elt_pipeline",
    default_args=default_args,
    description="Daily ELT pipeline: load CSVs into DuckDB, run dbt models, validate with dbt test",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["ecommerce", "dbt", "elt"],
) as dag:

    load_raw_data = BashOperator(
        task_id="load_raw_data",
        bash_command=f"python {SCRIPTS_DIR}/load_raw_data.py",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}",
    )

    load_raw_data >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test
