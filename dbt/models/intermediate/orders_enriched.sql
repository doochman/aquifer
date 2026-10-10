{{ config(materialized='table') }}

-- One row per order with its customer and product attributes; the shared building block of the marts.
select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    o.quantity,
    o.discount_pct,
    o.amount,
    p.product_id,
    p.product_name,
    p.category,
    c.country,
    c.acquisition_channel,
    c.signup_date,
    c.marketing_opt_in
from {{ source('shop', 'orders') }} as o
join {{ source('shop', 'customers') }} as c on c.customer_id = o.customer_id
join {{ source('shop', 'products') }} as p on p.product_id = o.product_id
