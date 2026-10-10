"""Rolls the events that Flink landed in Iceberg up into the streaming mart with dbt.

Needs the `streaming` profile (Flink job aquifer-orders-to-iceberg) and some events: `./lakehouse stream`.
Pipeline: Kafka topic shop.orders -> Flink -> Iceberg shop.order_events -> dbt live_orders_by_hour -> Superset.
"""
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DBT = "/home/airflow/.dbt-venv/bin/dbt"

with DAG(
    dag_id="shop_streaming_marts",
    description="dbt build of the streaming mart live_orders_by_hour from the Flink-written order_events",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["aquifer", "dbt", "flink", "streaming"],
) as dag:
    BashOperator(
        task_id="dbt_build_streaming",
        bash_command=f"{DBT} build --select tag:streaming",
        append_env=True,
    )
