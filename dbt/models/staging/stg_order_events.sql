select
    event_id, batch_id, order_id, customer_id, product_id,
    quantity, amount_eur, event_ts, ingested_at, is_deleted
from {{ source('raw', 'order_events') }}
