#!/usr/bin/env python3
"""Fire-and-Forget mixed workload driver for Calm (ES Query Version).

Features
--------
* Ensures table existence through GraphQL (GraphQL is Calm's only DDL path).
* Launches multiple writer threads that issue small SQL batches through the MySQL wire protocol.
* Launches multiple reader threads that continuously run ES queries to observe the impact of concurrent writes.
* Periodically prints latency/QPS snapshots for both writers and readers.

Requirements
------------
python -m pip install requests pymysql

Example
-------
python scripts/mixed_workload_es.py \
    --graphql-endpoint http://127.0.0.1:9567/graphql \
    --es-endpoint http://127.0.0.1:9200 \
    --mysql-host 127.0.0.1 --mysql-port 3307 --mysql-user root --mysql-password calm \
    --table label_event_v1_mix --insert-threads 8 --query-threads 4 --duration 120
"""

from __future__ import annotations

import argparse
import json
import os
import random
import string
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional, Tuple

import pymysql  # type: ignore
import requests

# --------------------------- stats helpers --------------------------- #


@dataclass
class Snapshot:
    insert_batches: int
    insert_rows: int
    query_count: int
    insert_latency_ms: float
    query_latency_ms: float
    insert_qps: float
    query_qps: float


class LatencyTracker:
    """Lock-protected tracker with fixed window to keep memory bounded."""

    def __init__(self, window_size: int = 1024) -> None:
        self._latencies: Deque[float] = deque(maxlen=window_size)
        self._count = 0
        self._lock = threading.Lock()

    def record(self, latency_ms: float) -> None:
        with self._lock:
            self._latencies.append(latency_ms)
            self._count += 1

    def snapshot(self) -> Tuple[int, float]:
        with self._lock:
            count = self._count
            median = median_or_zero(self._latencies)
        return count, median


def median_or_zero(values: Deque[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


# --------------------------- GraphQL helpers --------------------------- #

CREATE_TABLE_MUTATION = """
mutation EnsureLabelEvent($name: String!) {
  createTable(input: {
    name: $name
    description: "Benchmark table created by mixed_workload.py"
    primaryKey: "event_id"
    partitionStrategy: { pkHash: { numPartitions: 8 } }
    fields: [
      { name: "event_id", fieldType: U64, nullable: false }
      { name: "user_id", fieldType: U64, nullable: false }
      { name: "ts_ms", fieldType: U64, nullable: false }
      { name: "source", fieldType: KEYWORD, nullable: true }
    ]
  }) {
    name
    partitionCount
  }
}
"""


def ensure_table(args: argparse.Namespace) -> None:
    payload = {
        "query": CREATE_TABLE_MUTATION,
        "variables": {"name": args.table},
    }
    resp = requests.post(args.graphql_endpoint, json=payload, timeout=10)
    data = resp.json()

    if "errors" in data:
        # When the table already exists GraphQL returns an error that contains "existed".
        message = data["errors"][0].get("message", "")
        if "existed" in message.lower():
            print(f"[GraphQL] Table '{args.table}' already exists, skipping creation")
            return
        raise RuntimeError(f"GraphQL createTable failed: {message}")

    print(
        f"[GraphQL] Table '{data['data']['createTable']['name']}' ready with"
        f" {data['data']['createTable']['partitionCount']} partitions"
    )


# --------------------------- SQL workload --------------------------- #


def new_mysql_connection(args: argparse.Namespace) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
        charset="utf8mb4",
        autocommit=True,
    )


def random_source_tag(length: int = 6) -> str:
    return "src_" + "".join(random.choices(string.ascii_lowercase, k=length))


def build_batch_rows(batch_size: int) -> Tuple[list, int]:
    rows = []
    now_ms = int(time.time() * 1000)
    for _ in range(batch_size):
        event_id = random.getrandbits(63)
        user_id = random.randint(1, 1_000_000)
        ts_ms = now_ms - random.randint(0, 60_000)
        source = random_source_tag(4)
        rows.append((event_id, user_id, ts_ms, source))
    return rows, now_ms


def insert_worker(
    worker_id: int,
    args: argparse.Namespace,
    tracker: LatencyTracker,
    stop_event: threading.Event,
) -> None:
    conn = new_mysql_connection(args)
    cursor = conn.cursor()
    sql = f"INSERT INTO {args.table} (event_id, user_id, ts_ms, source) VALUES (%s, %s, %s, %s)"
    inserted_batches = 0

    try:
        while not stop_event.is_set():
            rows, _ = build_batch_rows(args.batch_size)
            start = time.perf_counter()
            cursor.executemany(sql, rows)
            latency_ms = (time.perf_counter() - start) * 1000
            tracker.record(latency_ms)
            inserted_batches += 1
    except Exception as exc:  # pylint: disable=broad-except
        print(f"[insert-{worker_id}] ERROR: {exc}")
    finally:
        cursor.close()
        conn.close()
        print(f"[insert-{worker_id}] stopped after {inserted_batches} batches")


def query_worker(
    worker_id: int,
    args: argparse.Namespace,
    tracker: LatencyTracker,
    stop_event: threading.Event,
) -> None:
    # 使用 ES 查询代替 MySQL SELECT
    # ES 查询: match_all，相当于 SELECT * FROM table LIMIT 10
    completed = 0

    try:
        while not stop_event.is_set():
            start = time.perf_counter()
            url = f"{args.es_endpoint}/{args.table}/_search"
            payload = {"query": {"match_all": {}}, "size": 10}
            resp = requests.post(url, json=payload, timeout=10)
            data = resp.json()

            if "error" in data:
                print(f"[query-{worker_id}] ES error: {data['error']}")

            latency_ms = (time.perf_counter() - start) * 1000
            tracker.record(latency_ms)
            completed += 1
            time.sleep(args.query_pause_ms / 1000.0)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"[query-{worker_id}] ERROR: {exc}")
    finally:
        print(f"[query-{worker_id}] stopped after {completed} queries")


# --------------------------- main orchestration --------------------------- #


def reporter_loop(
    insert_tracker: LatencyTracker,
    query_tracker: LatencyTracker,
    stop_event: threading.Event,
    interval: int,
) -> None:
    last_insert_count = 0
    last_query_count = 0

    while not stop_event.is_set():
        time.sleep(interval)
        insert_count, insert_p50 = insert_tracker.snapshot()
        query_count, query_p50 = query_tracker.snapshot()

        insert_qps = (insert_count - last_insert_count) / interval
        query_qps = (query_count - last_query_count) / interval
        last_insert_count = insert_count
        last_query_count = query_count

        print(
            "[stats] inserts=%d batches, p50=%.2fms, qps=%.2f | queries=%d ops, p50=%.2fms, qps=%.2f"
            % (
                insert_count,
                insert_p50,
                insert_qps,
                query_count,
                query_p50,
                query_qps,
            )
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mixed insert/query workload for Calm")
    parser.add_argument("--graphql-endpoint", default="http://127.0.0.1:9567/graphql")
    parser.add_argument("--es-endpoint", default="http://127.0.0.1:9200")

    parser.add_argument("--mysql-host", default="127.0.0.1")
    parser.add_argument("--mysql-port", type=int, default=3307)
    parser.add_argument("--mysql-user", default="root")
    parser.add_argument("--mysql-password", default="")
    parser.add_argument("--mysql-database", default="calm")

    parser.add_argument("--table", default="label_event_v1_mix")
    parser.add_argument("--insert-threads", type=int, default=4)
    parser.add_argument("--query-threads", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--query-window-ms", type=int, default=60_000)
    parser.add_argument("--query-pause-ms", type=int, default=50)
    parser.add_argument(
        "--duration", type=int, default=120, help="Benchmark duration in seconds"
    )
    parser.add_argument("--report-interval", type=int, default=5)

    parsed = parser.parse_args()
    return parsed


def main() -> None:
    args = parse_args()
    ensure_table(args)

    stop_event = threading.Event()
    insert_tracker = LatencyTracker()
    query_tracker = LatencyTracker()

    threads = []
    for idx in range(args.insert_threads):
        t = threading.Thread(
            target=insert_worker,
            args=(idx, args, insert_tracker, stop_event),
            daemon=True,
        )
        t.start()
        threads.append(t)

    for idx in range(args.query_threads):
        t = threading.Thread(
            target=query_worker,
            args=(idx, args, query_tracker, stop_event),
            daemon=True,
        )
        t.start()
        threads.append(t)

    reporter = threading.Thread(
        target=reporter_loop,
        args=(insert_tracker, query_tracker, stop_event, args.report_interval),
        daemon=True,
    )
    reporter.start()

    print(
        f"Running mixed workload for {args.duration}s with"
        f" {args.insert_threads} insert threads and {args.query_threads} query threads (ES queries)"
    )

    try:
        time.sleep(args.duration)
    except KeyboardInterrupt:
        print("Interrupted, shutting down...")
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=5)
        reporter.join(timeout=5)
        insert_total, insert_p50 = insert_tracker.snapshot()
        query_total, query_p50 = query_tracker.snapshot()
        print(
            "[stats-final] inserts=%d batches, p50=%.2fms | queries=%d ops, p50=%.2fms"
            % (insert_total, insert_p50, query_total, query_p50)
        )


if __name__ == "__main__":
    main()
