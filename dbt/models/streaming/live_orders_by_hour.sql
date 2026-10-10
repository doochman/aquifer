-- Streaming mart: events that Flink landed in Iceberg, rolled up per hour. Tagged `streaming` by dbt_project.yml.
select
    date_trunc('hour', event_time) as event_hour,
    status,
    count(*) as events,
    sum(quantity) as units,
    cast(sum(amount) as double) as revenue,
    avg(date_diff('millisecond', event_time, ingested_at)) as avg_latency_ms
from {{ source('shop', 'order_events') }}
group by date_trunc('hour', event_time), status
