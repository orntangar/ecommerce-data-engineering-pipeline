#!/usr/bin/env python3
"""Acknowledge one batch after dbt build and tests; verify merged states."""
import argparse
import os

import psycopg


def mark_processed(batch_id: str):
    dsn = os.getenv("DB_DSN", "postgresql://portfolio:portfolio@localhost:55432/portfolio")
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT status FROM public.ingest_batches
                           WHERE batch_id = %s FOR UPDATE""", (batch_id,))
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"Unknown batch: {batch_id}")
            if row[0] == "processed":
                print(f"Batch {batch_id}: already processed")
                return
            cur.execute("""
                WITH affected AS (
                    SELECT DISTINCT order_id FROM public.order_events
                    WHERE batch_id = %s
                ), latest AS (
                    SELECT DISTINCT ON (e.order_id)
                        e.order_id, e.event_id
                    FROM public.order_events e
                    JOIN affected a USING (order_id)
                    JOIN public.ingest_batches b ON e.batch_id = b.batch_id
                    WHERE b.status = 'processed' OR e.batch_id = %s
                    ORDER BY e.order_id, e.event_ts DESC, e.event_id DESC
                )
                SELECT COUNT(*)
                FROM latest l
                LEFT JOIN analytics.fct_orders f ON f.order_id = l.order_id
                WHERE f.latest_event_id IS DISTINCT FROM l.event_id
            """, (batch_id, batch_id))
            invalid = cur.fetchone()[0]
            if invalid:
                raise RuntimeError(f"{invalid} order states not merged; batch remains pending")
            cur.execute("""UPDATE public.ingest_batches SET
                           status = 'processed', processed_at = NOW()
                           WHERE batch_id = %s""", (batch_id,))
    print(f"Batch {batch_id}: marked processed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    mark_processed(args.batch_id)
