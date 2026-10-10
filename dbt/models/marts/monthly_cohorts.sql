-- Signup cohort by months since signup: how many customers keep ordering.
with sizes as (
    select date_trunc('month', signup_date) as cohort_month, count(*) as cohort_size
    from {{ source('shop', 'customers') }}
    group by 1
)

select
    date_format(s.cohort_month, '%Y-%m') as cohort,
    s.cohort_month,
    date_diff('month', s.cohort_month, date_trunc('month', o.order_date)) as months_since_signup,
    format('M%02d', date_diff('month', s.cohort_month, date_trunc('month', o.order_date))) as month_label,
    s.cohort_size,
    count(distinct o.customer_id) as active_customers,
    cast(count(distinct o.customer_id) as double) / s.cohort_size as retention,
    cast(sum(o.amount) as double) as net_revenue
from {{ ref('orders_enriched') }} as o
join sizes as s on s.cohort_month = date_trunc('month', o.signup_date)
where o.status = 'completed'
group by s.cohort_month, s.cohort_size, date_diff('month', s.cohort_month, date_trunc('month', o.order_date))
