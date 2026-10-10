-- Streaming ingestion: Kafka topic shop.orders -> Apache Iceberg table shop.order_events (queryable in Trino, DuckDB, Superset).
-- Submitted by flink-submit. Variables in ${...} are filled from the environment before the SQL client runs.
SET 'pipeline.name' = 'aquifer-orders-to-iceberg';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'table.local-time-zone' = 'UTC';

CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = '${MINIO_ROOT_USER}',
  's3.secret-access-key' = '${MINIO_ROOT_PASSWORD}',
  'client.region' = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS lake.shop;

CREATE TABLE IF NOT EXISTS lake.shop.order_events (
  event_id STRING,
  order_id BIGINT,
  customer_id BIGINT,
  product_id BIGINT,
  quantity INT,
  amount DOUBLE,
  status STRING,
  event_time TIMESTAMP(6),
  kafka_partition INT,
  kafka_offset BIGINT,
  ingested_at TIMESTAMP(6)
);

CREATE TEMPORARY TABLE kafka_orders (
  event_id STRING,
  order_id BIGINT,
  customer_id BIGINT,
  product_id BIGINT,
  quantity INT,
  amount DOUBLE,
  status STRING,
  event_time TIMESTAMP(3),
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'shop.orders',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'aquifer-flink-orders',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'scan.topic-partition-discovery.interval' = '10s',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true'
);

INSERT INTO lake.shop.order_events
SELECT
  event_id,
  order_id,
  customer_id,
  product_id,
  quantity,
  amount,
  status,
  CAST(event_time AS TIMESTAMP(6)),
  kafka_partition,
  kafka_offset,
  CAST(LOCALTIMESTAMP AS TIMESTAMP(6))
FROM kafka_orders;
