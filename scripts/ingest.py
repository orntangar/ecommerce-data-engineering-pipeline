#!/usr/bin/env python3
"""Transactional, replay-safe ingestion of immutable order events."""
import argparse
import hashlib
import json
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import psycopg

FIELDS = {"event_id", "order_id", "customer_id", "product_id",
          "quantity", "amount_eur", "event_ts", "is_deleted"}


def parse_file(path: Path):
    events = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        event = json.loads(line)
        if set(event) != FIELDS:
            raise ValueError(f"Line {line_no}: expected fields {sorted(FIELDS)}")
        for key in ("event_id", "order_id", "customer_id", "product_id"):
            if not isinstance(event[key], str) or not event[key].strip():
                raise ValueError(f"Line {line_no}: invalid {key}")
        if type(event["quantity"]) is not int or event["quantity"] < 0:
            raise ValueError(f"Line {line_no}: invalid quantity")
        amount = Decimal(str(event["amount_eur"]))
        if not amount.is_finite() or amount < 0 or amount.as_tuple().exponent < -2:
            raise ValueError(f"Line {line_no}: invalid amount")
        ts = datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00"))
        if ts.tzinfo is None:
            raise ValueError(f"Line {line_no}: event_ts needs a timezone")
        if type(event["is_deleted"]) is not bool:
            raise ValueError(f"Line {line_no}: invalid is_deleted")
        events.append(event)
    if not events:
        raise ValueError("Refusing an empty batch")
    if len({e["event_id"] for e in events}) != len(events):
        raise ValueError("Duplicate event_id in input file")
    canonical = "\n".join(json.dumps(e, sort_keys=True, separators=(",", ":"))
                          for e in events)
    return events, hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def ingest(path: Path, batch_id: str):
    if not batch_id.strip():
        raise ValueError("batch_id must not be empty")
    events, digest = parse_file(path)
    dsn = os.getenv("DB_DSN", "postgresql://portfolio:portfolio@localhost:55432/portfolio")
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO public.ingest_batches (batch_id, payload_sha256)
                   VALUES (%s, %s) ON CONFLICT (batch_id) DO NOTHING""",
                (batch_id, digest),
            )
            cur.execute(
                """SELECT payload_sha256, status
                   FROM public.ingest_batches WHERE batch_id = %s FOR UPDATE""",
                (batch_id,),
            )
            saved_hash, status = cur.fetchone()
            if saved_hash != digest:
                raise ValueError(f"Batch {batch_id!r} has different content")
            if status == "processed":
                print(f"Batch {batch_id} already processed — no changes")
                return
            for e in events:
                cur.execute(
                    """INSERT INTO public.order_events
                       (event_id, batch_id, order_id, customer_id, product_id,
                        quantity, amount_eur, event_ts, is_deleted)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (event_id) DO NOTHING
                       RETURNING event_id""",
                    (e["event_id"], batch_id, e["order_id"], e["customer_id"],
                     e["product_id"], e["quantity"], Decimal(str(e["amount_eur"])),
                     datetime.fromisoformat(e["event_ts"].replace("Z", "+00:00")),
                     e["is_deleted"]),
                )
                if cur.fetchone() is None:
                    cur.execute("SELECT batch_id FROM public.order_events WHERE event_id = %s",
                                (e["event_id"],))
                    if cur.fetchone()[0] != batch_id:
                        raise ValueError(f"Event {e['event_id']} belongs to another batch")
    print(f"Batch {batch_id}: accepted {len(events)} events, replay-safe")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, type=Path)
    parser.add_argument("--batch-id", required=True)
    a = parser.parse_args()
    ingest(a.file, a.batch_id)
