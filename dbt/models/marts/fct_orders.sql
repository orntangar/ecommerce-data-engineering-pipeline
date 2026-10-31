{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id',
        on_schema_change='fail'
    )
}}

-- Recompute only changed orders, using ALL historical event versions.
{% if is_incremental() and var('batch_id', '') == '' %}
    {{ exceptions.raise_compiler_error("An incremental run needs --vars '{\"batch_id\": \"...\"}'.") }}
{% endif %}

with eligible_events as (
    select e.*
    from {{ ref('stg_order_events') }} as e
    join {{ source('raw', 'ingest_batches') }} as b
      on e.batch_id = b.batch_id
    where b.status = 'processed'
       or e.batch_id = '{{ var("batch_id", "__no_batch__") | replace("'", "''") }}'
),
{% if is_incremental() %}
changed_order_ids as (
    select distinct order_id
    from eligible_events
    where batch_id = '{{ var("batch_id") | replace("'", "''") }}'
),
{% endif %}
ranked_events as (
    select e.*,
           row_number() over (
               partition by e.order_id
               order by e.event_ts desc, e.event_id desc
           ) as event_rank
    from eligible_events as e
    {% if is_incremental() %}
    inner join changed_order_ids as changed
      on e.order_id = changed.order_id
    {% endif %}
)
select order_id, event_id as latest_event_id, customer_id, product_id,
       quantity, amount_eur, event_ts as order_event_ts,
       ingested_at as order_ingested_at, is_deleted
from ranked_events
where event_rank = 1
