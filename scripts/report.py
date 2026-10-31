#!/usr/bin/env python3
"""Print revenue per day to capture portfolio screenshots."""
import os
import psycopg

dsn = os.getenv("DB_DSN", "postgresql://portfolio:portfolio@localhost:55432/portfolio")
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("""SELECT order_date, order_count, revenue_eur
                       FROM analytics.mart_daily_revenue ORDER BY order_date""")
        print("date       | active orders | revenue (EUR)")
        for date, count, revenue in cur.fetchall():
            print(f"{date} | {count:13d} | {revenue}")
