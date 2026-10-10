-- One row per customer with lifetime value, value tier and lifecycle status.
with spend as (
    select
        customer_id,
        count(*) as orders,
        cast(sum(amount) as double) as net_revenue,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from {{ ref('orders_enriched') }}
    where status = 'completed'
    group by customer_id
)

select
    c.customer_id,
    c.country,
    c.acquisition_channel,
    c.signup_date,
    c.marketing_opt_in,
    coalesce(s.orders, 0) as orders,
    coalesce(s.net_revenue, 0e0) as net_revenue,
    s.first_order_date,
    s.last_order_date,
    case
        when s.orders is null then 'no_orders'
        when s.net_revenue >= 450 then 'gold'
        when s.net_revenue >= 180 then 'silver'
        else 'bronze'
    end as value_tier,
    case
        when s.last_order_date is null then 'never_ordered'
        when date_diff('day', s.last_order_date, current_date) <= 60 then 'active'
        when date_diff('day', s.last_order_date, current_date) <= 150 then 'at_risk'
        else 'lapsed'
    end as lifecycle_status
from {{ source('shop', 'customers') }} as c
left join spend as s on s.customer_id = c.customer_id
