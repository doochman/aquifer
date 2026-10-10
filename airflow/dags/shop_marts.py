"""Rebuilds and tests the Acme Outdoor marts with dbt.

The dbt project lives in ./dbt (mounted at /opt/aquifer/dbt). `./lakehouse demo` loads the raw tables first; this DAG
then runs the same `dbt build` that you can run by hand with `./lakehouse dbt build`. Unpause it for a daily refresh.
"""
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DBT = "/home/airflow/.dbt-venv/bin/dbt"

with DAG(
    dag_id="shop_marts",
    description="dbt build of the shop marts (orders_enriched, daily_revenue, customer_value, monthly_cohorts) with tests",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["aquifer", "dbt", "demo"],
) as dag:
    BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build --exclude tag:streaming",
        append_env=True,
    )
