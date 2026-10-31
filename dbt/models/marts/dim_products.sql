select product_id, min(event_ts) as first_seen_at
from {{ ref('stg_order_events') }}
group by product_id
