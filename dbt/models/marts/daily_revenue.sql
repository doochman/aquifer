-- Revenue per day, country and category. Refunds are tracked separately from net revenue.
select
    order_date,
    country,
    category,
    count(*) as orders,
    sum(quantity) as units,
    cast(sum(amount) as double) as gross_revenue,
    cast(sum(case when status = 'refunded' then amount else 0 end) as double) as refunded_revenue,
    cast(sum(case when status = 'completed' then amount else 0 end) as double) as net_revenue
from {{ ref('orders_enriched') }}
where status in ('completed', 'refunded')
group by order_date, country, category
