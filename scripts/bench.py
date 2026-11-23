#!/usr/bin/env python3
"""Minimal put/get benchmark for the distributed KV store.

Measures latency (p50/p95) and throughput for both redundancy modes and prints
a Markdown table suitable for the report.

Usage:
    python scripts/bench.py --keys 50 --value-size 1024
    python scripts/bench.py --peers localhost:50051,...,localhost:50055
    PEERS=node1:50051,... python scripts/bench.py

Run against a healthy cluster:
    docker compose up -d
    python scripts/bench.py
"""
import argparse
import os
import sys
import time
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from gateway.client import GatewayClient  # noqa: E402

DEFAULT_PEERS = "localhost:50051,localhost:50052,localhost:50053,localhost:50054,localhost:50055"


def percentile(values, pct):
    if not values:
        return 0.0
    ordered = sorted(values)
    k = int(round((pct / 100.0) * (len(ordered) - 1)))
    return ordered[k]


def run_mode(client, mode, keys, value_size, get_repeats):
    prefix = f"bench-{mode}-{uuid.uuid4().hex[:8]}"
    value = b"x" * max(1, value_size)
    put_lat = []
    get_lat = []
    failures = 0

    for i in range(keys):
        key = f"{prefix}-{i}"
        start = time.perf_counter()
        ok = client.put(key, value, mode=mode)
        elapsed = (time.perf_counter() - start) * 1000.0
        if not ok:
            failures += 1
            continue
        put_lat.append(elapsed)

    for i in range(keys):
        key = f"{prefix}-{i}"
        for _ in range(get_repeats):
            start = time.perf_counter()
            got = client.get(key)
            elapsed = (time.perf_counter() - start) * 1000.0
            if got != value:
                failures += 1
                continue
            get_lat.append(elapsed)

    def row(op, lat):
        if not lat:
            return (op, 0, 0.0, 0.0, 0.0)
        total_s = sum(lat) / 1000.0
        throughput = len(lat) / total_s if total_s > 0 else 0.0
        return (op, len(lat), percentile(lat, 50), percentile(lat, 95), throughput)

    return {
        "put": row(f"put {mode}", put_lat),
        "get": row(f"get {mode}", get_lat),
        "failures": failures,
    }


def main():
    parser = argparse.ArgumentParser(description="Benchmark put/get for the distributed KV store")
    parser.add_argument("--peers", default=os.environ.get("PEERS", DEFAULT_PEERS))
    parser.add_argument("--keys", type=int, default=50)
    parser.add_argument("--value-size", type=int, default=1024)
    parser.add_argument("--get-repeats", type=int, default=3)
    parser.add_argument("--modes", default="rs32,replication")
    args = parser.parse_args()

    client = GatewayClient(args.peers)
    rows = []
    failures = 0
    for mode in [m.strip() for m in args.modes.split(",") if m.strip()]:
        result = run_mode(client, mode, args.keys, args.value_size, args.get_repeats)
        rows.extend([result["put"], result["get"]])
        failures += result["failures"]

    print()
    print(f"Benchmark: keys={args.keys}, value_size={args.value_size}B, "
          f"get_repeats={args.get_repeats}, peers={len(client.peers)}")
    print()
    print("| Operation | Completed | p50 (ms) | p95 (ms) | Throughput (ops/s) |")
    print("|---|---:|---:|---:|---:|")
    for op, n, p50, p95, thr in rows:
        print(f"| {op} | {n} | {p50:.1f} | {p95:.1f} | {thr:.1f} |")
    if failures:
        print(f"\nFailures: {failures}")
    print()


if __name__ == "__main__":
    main()
