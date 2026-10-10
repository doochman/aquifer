from datetime import datetime, timezone

from airflow.sdk import DAG, get_current_context, task
from trino.dbapi import connect


@task
def verify_iceberg_round_trip():
    run_id = get_current_context()["run_id"].replace("'", "''")
    connection = connect(
        host="trino",
        port=8080,
        user="airflow",
        catalog="iceberg",
        schema="demo",
    )
    cursor = connection.cursor()
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS iceberg.demo WITH (location = 's3://warehouse/demo/')")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS iceberg.demo.platform_smoke (
                run_id varchar,
                message varchar,
                written_at timestamp(6) with time zone
            ) WITH (format = 'PARQUET')"""
        )
        cursor.execute(
            """MERGE INTO iceberg.demo.platform_smoke AS target
               USING (VALUES (?, ?, ?)) AS source(run_id, message, written_at)
               ON target.run_id = source.run_id
               WHEN MATCHED THEN UPDATE SET message = source.message, written_at = source.written_at
               WHEN NOT MATCHED THEN INSERT (run_id, message, written_at)
                 VALUES (source.run_id, source.message, source.written_at)""",
            [run_id, "Airflow to Trino to Iceberg", datetime.now(timezone.utc)],
        )
        cursor.execute(
            "SELECT count(*) FROM iceberg.demo.platform_smoke WHERE run_id = ?",
            [run_id],
        )
        count = cursor.fetchone()[0]
        if count != 1:
            raise RuntimeError(f"Expected one Iceberg row for this run, found {count}")
    finally:
        cursor.close()
        connection.close()


with DAG(
    dag_id="iceberg_smoke",
    description="Write and read one idempotent row through Trino and Iceberg",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["local-stack", "smoke"],
) as dag:
    verify_iceberg_round_trip()