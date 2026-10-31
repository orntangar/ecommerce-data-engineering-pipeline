with expected as (
    select cast(order_event_ts as date) as order_date,
           count(*) as order_count, sum(amount_eur) as revenue_eur
    from {{ ref('fct_orders') }}
    where not is_deleted
    group by cast(order_event_ts as date)
)
select coalesce(m.order_date, e.order_date) as invalid_date
from {{ ref('mart_daily_revenue') }} m
full outer join expected e on m.order_date = e.order_date
where m.order_count is distinct from e.order_count
   or m.revenue_eur is distinct from e.revenue_eur
