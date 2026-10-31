#!/usr/bin/env python3
"""Integration assertions for two processed demo batches."""
import os
from decimal import Decimal
import psycopg

dsn = os.getenv("DB_DSN", "postgresql://portfolio:portfolio@localhost:55432/portfolio")
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        def scalar(sql):
            cur.execute(sql)
            return cur.fetchone()[0]
        assert scalar("SELECT COUNT(*) FROM public.order_events") == 8
        assert scalar("SELECT COUNT(*) FROM analytics.fct_orders") == 5
        assert scalar("SELECT COUNT(*) FROM analytics.fct_orders WHERE NOT is_deleted") == 4
        assert scalar("SELECT SUM(revenue_eur) FROM analytics.mart_daily_revenue") == Decimal("300.00")
        assert scalar("SELECT latest_event_id FROM analytics.fct_orders WHERE order_id='o-001'") == "e-004"
        assert scalar("SELECT is_deleted FROM analytics.fct_orders WHERE order_id='o-002'") is True
        assert scalar("SELECT COUNT(*) FROM public.ingest_batches WHERE status='processed'") == 3
print("PASS: counts, late event, deletion, revenue, and statuses")
