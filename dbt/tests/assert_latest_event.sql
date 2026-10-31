-- Expected state includes processed batches and the batch under test.
with eligible_events as (
    select e.*
    from {{ ref('stg_order_events') }} as e
    join {{ source('raw', 'ingest_batches') }} as b
      on e.batch_id = b.batch_id
    where b.status = 'processed'
       or e.batch_id = '{{ var("batch_id", "__no_batch__") | replace("'", "''") }}'
),
latest as (
    select order_id, event_id,
           row_number() over (
               partition by order_id
               order by event_ts desc, event_id desc
           ) as rn
    from eligible_events
)
select coalesce(f.order_id, l.order_id) as wrong_order_id
from {{ ref('fct_orders') }} as f
full outer join (select order_id, event_id from latest where rn = 1) as l
    on f.order_id = l.order_id
where f.latest_event_id is distinct from l.event_id
