select cast(order_event_ts as date) as order_date,
       count(*) as order_count,
       sum(amount_eur) as revenue_eur
from {{ ref('fct_orders') }}
where not is_deleted
group by cast(order_event_ts as date)
