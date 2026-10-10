-- Net revenue can never exceed gross revenue.
select order_date, country, category
from {{ ref('daily_revenue') }}
where net_revenue > gross_revenue + 0.01
