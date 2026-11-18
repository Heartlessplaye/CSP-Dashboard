#!/usr/bin/env python3
"""Throughput and lag benchmark.

1. Throughput: write N records spread over the 5 source formats, then time a
   one-shot run (`Engine.run_once`) with 1 worker and with the process pool.
2. Lag: run the live engine, append records continuously at --live-rate for
   --live-seconds, and sample `processing.lag_ms` from the insights files.

Usage: python tools/bench.py [--records 1000000] [--workers 4]
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
import statistics
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from csp_traffic.config import Settings  # noqa: E402
from csp_traffic.engine import Engine  # noqa: E402

EPS = ["/v1/users", "/v1/orders", "/v1/auth", "/v1/cart", "/v1/search", "/v1/payments"]


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def rows(n: int, rng: random.Random, t0: float):
    for i in range(n):
        yield (t0 + i * 0.001, f"client-{int(rng.paretovariate(1.2)) % 5000:04d}", rng.choice(EPS),
               rng.choice(["GET", "POST"]), rng.randint(5, 900), rng.choice([200, 200, 200, 201, 404, 500]))


def write_dataset(root: Path, n: int, files_per_source: int, t0: float) -> None:
    rng = random.Random(1)
    per_file = n // (5 * files_per_source)
    for f in range(files_per_source):
        data = list(rows(per_file * 5, rng, t0 + f * per_file * 5 * 0.001))
        chunks = [data[i::5] for i in range(5)]
        d = root / "Microservice"; d.mkdir(parents=True, exist_ok=True)
        with open(d / f"m{f}.csv", "w") as fh:
            fh.write("timestamp,client_id,endpoint,method,response_time_ms,status_code\n")
            fh.writelines(f"{iso(t)},{c},{e},{m},{lat},{s}\n" for t, c, e, m, lat, s in chunks[0])
        d = root / "LoadBalancer"; d.mkdir(parents=True, exist_ok=True)
        with open(d / f"l{f}.csv", "w") as fh:
            fh.write("event_time;apiKey;path;http_method;latency;http_status\n")
            fh.writelines(f"{int(t)};{c};{e};{m};{lat};{s}\n" for t, c, e, m, lat, s in chunks[1])
        d = root / "APIGateway"; d.mkdir(parents=True, exist_ok=True)
        with open(d / f"g{f}.jsonl", "w") as fh:
            fh.writelines(json.dumps({"ts": int(t * 1000), "client": {"key": c}, "request": {"path": e, "verb": m},
                                      "response": {"code": s, "duration_s": lat / 1000}}) + "\n"
                          for t, c, e, m, lat, s in chunks[2])
        d = root / "EdgeServer"; d.mkdir(parents=True, exist_ok=True)
        with open(d / f"e{f}.json", "w") as fh:
            fh.write(json.dumps({"timestamp": iso(chunks[3][0][0]), "requests": [
                {"api_key": c, "endpoint": e, "method": m, "latency_ms": lat, "status": s}
                for t, c, e, m, lat, s in chunks[3]]}))
        d = root / "MobileBackend"; d.mkdir(parents=True, exist_ok=True)
        with open(d / f"mb{f}.json", "w") as fh:
            fh.write(json.dumps([{"eventTime": datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                  "clientId": c, "route": e, "httpMethod": m, "durationMs": lat, "statusCode": s}
                                 for t, c, e, m, lat, s in chunks[4]]))


def settings(tmp: Path, workers: int, **kw) -> Settings:
    return Settings(input_dir=tmp / "input", output_dir=tmp / "output", state_dir=tmp / "state",
                    sources_file=ROOT / "config" / "sources.yaml", limits_file=ROOT / "config" / "limits.json",
                    plugins_dir=ROOT / "plugins", workers=workers, use_watchdog=False, **kw)


def throughput(n: int, workers: int, files_per_source: int) -> None:
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        t = time.perf_counter()
        write_dataset(tmp / "input", n, files_per_source, time.time() - 7200)
        print(f"dataset: {n:,} records in {5 * files_per_source} files ({time.perf_counter() - t:.1f}s to write)")
        for w in sorted({1, workers}):
            shutil.rmtree(tmp / "output", ignore_errors=True)
            shutil.rmtree(tmp / "state", ignore_errors=True)
            eng = Engine(settings(tmp, w))
            t = time.perf_counter()
            eng.run_once()
            dt = time.perf_counter() - t
            done = eng.counters["records_parsed"]
            print(f"  workers={w}: {done:,} records in {dt:.2f}s  ->  {done / dt:,.0f} records/s "
                  f"({done / dt * 3600 / 1e6:,.0f}M/hour)")


def live_lag(rate: int, seconds: int, workers: int) -> None:
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        s = settings(tmp, workers, tick_seconds=1.0, reconcile_seconds=2.0)
        (tmp / "input" / "Microservice").mkdir(parents=True)
        eng = Engine(s)
        th = threading.Thread(target=eng.run, daemon=True)
        th.start()
        rng = random.Random(2)
        lags: list[int] = []
        path = tmp / "input" / "Microservice" / "live.csv"
        path.write_text("timestamp,client_id,endpoint,method,response_time_ms,status_code\n")
        out = s.output_dir / "Microservice" / "insights.json"
        end = time.time() + seconds
        written = 0
        while time.time() < end:
            now = time.time()
            with open(path, "a") as fh:
                fh.writelines(f"{iso(t)},{c},{e},{m},{lat},{st}\n" for t, c, e, m, lat, st in rows(rate // 10, rng, now))
            written += rate // 10
            time.sleep(0.1)
            try:
                lag = json.loads(out.read_text())["processing"]["lag_ms"]
                if lag is not None:
                    lags.append(lag)
            except (FileNotFoundError, json.JSONDecodeError, KeyError):
                pass
        time.sleep(3)
        eng.stop()
        th.join(timeout=30)
        got = eng.counters["records_parsed"]
        lags.sort()
        p99 = lags[min(len(lags) - 1, int(0.99 * len(lags)))] if lags else None
        print(f"live: wrote {written:,} records at ~{rate:,}/s for {seconds}s, processed {got:,}")
        if lags:
            print(f"  lag_ms (file write -> insights): median {statistics.median(lags):.0f}, p99 {p99}, max {lags[-1]}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=1_000_000)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--files-per-source", type=int, default=8)
    ap.add_argument("--live-rate", type=int, default=20_000, help="records/s appended in the live test")
    ap.add_argument("--live-seconds", type=int, default=15)
    args = ap.parse_args()
    throughput(args.records, args.workers, args.files_per_source)
    live_lag(args.live_rate, args.live_seconds, args.workers)


if __name__ == "__main__":
    main()
