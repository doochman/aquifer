CREATE ROLE airflow LOGIN PASSWORD 'airflow-local';
CREATE DATABASE airflow OWNER airflow;

CREATE ROLE iceberg LOGIN PASSWORD 'iceberg-local';
CREATE DATABASE iceberg OWNER iceberg;