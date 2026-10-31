CREATE TABLE IF NOT EXISTS public.ingest_batches (
    batch_id TEXT PRIMARY KEY,
    payload_sha256 TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS public.order_events (
    event_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES public.ingest_batches(batch_id),
    order_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    amount_eur NUMERIC(14, 2) NOT NULL CHECK (amount_eur >= 0),
    event_ts TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_order_events_batch_id
  ON public.order_events (batch_id);
CREATE INDEX IF NOT EXISTS idx_order_events_order_ts
  ON public.order_events (order_id, event_ts DESC, event_id DESC);
