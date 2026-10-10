"""Helpers for the AQUIFER notebooks: Trino, DuckDB and PyIceberg against the same lakehouse."""
import os

import duckdb
import pandas as pd
import trino

_EXTENSIONS = "/opt/duckdb/extensions"


def query(sql: str, schema: str = "shop") -> pd.DataFrame:
    """Run SQL on Trino (catalog iceberg) and return a DataFrame."""
    connection = trino.dbapi.connect(
        host=os.environ["TRINO_HOST"], port=int(os.environ["TRINO_PORT"]), user="notebook", catalog="iceberg", schema=schema)
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=[column[0] for column in cursor.description])
    finally:
        cursor.close()
        connection.close()


def duck() -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with the Iceberg catalog attached as `lake` (read access to the same tables)."""
    connection = duckdb.connect()
    connection.sql(f"SET extension_directory='{_EXTENSIONS}'")
    connection.sql("LOAD httpfs")
    connection.sql("LOAD iceberg")
    connection.sql(
        "CREATE OR REPLACE SECRET aquifer_s3 (TYPE s3, KEY_ID '{key}', SECRET '{secret}', ENDPOINT 'minio:9000', "
        "URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')".format(
            key=os.environ["MINIO_ROOT_USER"], secret=os.environ["MINIO_ROOT_PASSWORD"]))
    connection.sql("ATTACH 'warehouse' AS lake (TYPE iceberg, ENDPOINT 'http://iceberg-rest:8181', AUTHORIZATION_TYPE 'none')")
    return connection


def catalog():
    """A PyIceberg REST catalog: `catalog().load_table('shop.customers').scan().to_pandas()`."""
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "aquifer",
        **{
            "type": "rest",
            "uri": "http://iceberg-rest:8181",
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": os.environ["MINIO_ROOT_USER"],
            "s3.secret-access-key": os.environ["MINIO_ROOT_PASSWORD"],
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )
