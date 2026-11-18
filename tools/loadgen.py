#!/usr/bin/env python3
"""Synthetic API traffic for the 5 configured sources, each in its own log format.

Writes realistic patterns the processor must handle:
  EdgeServer     a new JSON envelope file per batch (written atomically)
  Microservice   CSV appended line by line, rotated every --rotate-lines lines
  APIGateway     NDJSON appended, rotated likewise
  LoadBalancer   ';'-CSV appended, rotated likewise
  MobileBackend  JSON array rewritten in place with more entries (temp file + rename)

Scenarios:
  steady   only the configured sources
  evolve   additionally, after --evolve-after seconds a new source "PaymentService"
           appears with an unseen TSV format (auto-detected), and after twice that
           "LoadBalancer" disappears (its folder is removed)

Client popularity is Zipf-like, so a few clients exceed the default 1000/h limit.
`--burst CLIENT` sends an extra 5 req/s for that client to EdgeServer.

Examples:
  python tools/loadgen.py --input input --rate 2000
  python tools/loadgen.py --input input --rate 500 --scenario evolve --duration 120
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

ENDPOINTS = [
    ("/v1/users", "GET", 90), ("/v1/orders", "POST", 220), ("/v1/orders", "GET", 120), ("/v1/auth", "POST", 160),
    ("/v1/cart", "PUT", 140), ("/v1/search", "GET", 260), ("/v1/payments", "POST", 400), ("/v1/health", "GET", 15),
    ("/v1/products", "GET", 110), ("/v1/inventory", "GET", 180),
]
ENDPOINT_WEIGHTS = [30, 12, 10, 14, 8, 9, 4, 2, 7, 4]
STATUSES = [200, 200, 200, 200, 201, 204, 400, 401, 404, 429, 500, 503]
STATUS_WEIGHTS = [60, 10, 8, 6, 6, 2, 2, 1.5, 2, 0.8, 1, 0.7]


class Gen:
    def __init__(self, root: Path, clients: int, seed: int, rotate_lines: int) -> None:
        self.root = root
        self.rng = random.Random(seed)
        self.clients = [f"client-{i:04d}" for i in range(clients)] + ["abc123", "xyz789", "def456", "ghi789"]
        self.client_weights = [1 / (i + 1) ** 1.1 for i in range(len(self.clients))]
        self.rotate_lines = rotate_lines
        self.lines: dict[str, int] = {}
        self.file_no: dict[str, int] = {}
        self.mobile: list[dict] = []
        self.batch_no = 0

    # ------------------------------------------------------------- helpers
    def req(self, now: float, client: str | None = None) -> dict:
        ep, method, base = self.rng.choices(ENDPOINTS, ENDPOINT_WEIGHTS)[0]
        return {
            "ts": now - self.rng.random() * 0.9,
            "client": client or self.rng.choices(self.clients, self.client_weights)[0],
            "endpoint": ep,
            "method": method,
            "latency": max(1.0, self.rng.lognormvariate(0, 0.5) * base),
            "status": self.rng.choices(STATUSES, STATUS_WEIGHTS)[0],
        }

    def _dir(self, source: str) -> Path:
        d = self.root / source
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _append(self, source: str, ext: str, header: str | None, lines: list[str]) -> None:
        n = self.file_no.setdefault(source, 1)
        path = self._dir(source) / f"{source.lower()}-{n:04d}{ext}"
        new = not path.exists()
        with open(path, "a", encoding="utf-8") as fh:
            if new and header:
                fh.write(header)
            fh.writelines(lines)
        self.lines[source] = self.lines.get(source, 0) + len(lines)
        if self.lines[source] >= self.rotate_lines:
            self.lines[source] = 0
            self.file_no[source] = n + 1

    @staticmethod
    def _atomic_write(path: Path, data: str) -> None:
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(data, encoding="utf-8")
        os.replace(tmp, path)

    # ------------------------------------------------------------- formats
    def edge(self, reqs: list[dict]) -> None:
        if not reqs:
            return
        self.batch_no += 1
        doc = {
            "timestamp": iso(reqs[0]["ts"]),
            "requests": [
                {"api_key": r["client"], "endpoint": r["endpoint"], "method": r["method"],
                 "latency_ms": round(r["latency"]), "status": r["status"]}
                for r in reqs
            ],
        }
        path = self._dir("EdgeServer") / f"edge-{int(time.time())}-{self.batch_no:06d}.json"
        self._atomic_write(path, json.dumps(doc))

    def microservice(self, reqs: list[dict]) -> None:
        self._append(
            "Microservice", ".csv", "timestamp,client_id,endpoint,method,response_time_ms,status_code\n",
            [f"{iso(r['ts'])},{r['client']},{r['endpoint']},{r['method']},{round(r['latency'])},{r['status']}\n"
             for r in reqs],
        )

    def gateway(self, reqs: list[dict]) -> None:
        self._append(
            "APIGateway", ".jsonl", None,
            [json.dumps({"ts": int(r["ts"] * 1000), "client": {"key": r["client"]},
                         "request": {"path": r["endpoint"], "verb": r["method"]},
                         "response": {"code": r["status"], "duration_s": round(r["latency"] / 1000, 4)}}) + "\n"
             for r in reqs],
        )

    def loadbalancer(self, reqs: list[dict]) -> None:
        self._append(
            "LoadBalancer", ".csv", "event_time;apiKey;path;http_method;latency;http_status\n",
            [f"{int(r['ts'])};{r['client']};{r['endpoint']};{r['method']};{round(r['latency'])};{r['status']}\n"
             for r in reqs],
        )

    def mobile_backend(self, reqs: list[dict]) -> None:
        self.mobile.extend(
            {"eventTime": datetime.fromtimestamp(r["ts"], timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
             "clientId": r["client"], "route": r["endpoint"], "httpMethod": r["method"],
             "durationMs": round(r["latency"]), "statusCode": r["status"]}
            for r in reqs
        )
        n = self.file_no.setdefault("MobileBackend", 1)
        path = self._dir("MobileBackend") / f"mobile-{n:04d}.json"
        self._atomic_write(path, json.dumps(self.mobile, indent=1))
        if len(self.mobile) >= self.rotate_lines:
            self.mobile = []
            self.file_no["MobileBackend"] = n + 1

    def payment_tsv(self, reqs: list[dict]) -> None:
        """A source nobody configured: TSV with its own column names."""
        self._append(
            "PaymentService", ".tsv", "time\tapiKey\tpath\tverb\tduration_ms\thttp_status\n",
            [f"{iso(r['ts'])}\t{r['client']}\t{r['endpoint']}\t{r['method']}\t{round(r['latency'])}\t{r['status']}\n"
             for r in reqs],
        )


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", default="input", help="input directory (default: input)")
    ap.add_argument("--rate", type=float, default=2000, help="total requests per second (default 2000)")
    ap.add_argument("--clients", type=int, default=500)
    ap.add_argument("--sources", default="EdgeServer,Microservice,APIGateway,LoadBalancer,MobileBackend",
                    help="comma-separated subset of the configured sources")
    ap.add_argument("--scenario", choices=["steady", "evolve"], default="steady")
    ap.add_argument("--evolve-after", type=float, default=30.0, help="seconds before the new source appears")
    ap.add_argument("--burst", default=None, help="client id to push over its limit")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    ap.add_argument("--rotate-lines", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    root = Path(args.input)
    root.mkdir(parents=True, exist_ok=True)
    gen = Gen(root, args.clients, args.seed, args.rotate_lines)
    writers = {
        "EdgeServer": gen.edge, "Microservice": gen.microservice, "APIGateway": gen.gateway,
        "LoadBalancer": gen.loadbalancer, "MobileBackend": gen.mobile_backend,
    }
    active = [s.strip() for s in args.sources.split(",") if s.strip() in writers]
    if not active:
        ap.error(f"--sources must name some of {list(writers)}")

    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))

    start = time.time()
    tick = 0.5
    sent = 0
    removed = added = False
    print(f"loadgen: {args.rate:.0f} req/s across {active} into {root} (scenario={args.scenario})", flush=True)
    next_t = time.monotonic()
    while not stop["flag"]:
        now = time.time()
        elapsed = now - start
        if args.duration and elapsed >= args.duration:
            break
        if args.scenario == "evolve":
            if not added and elapsed >= args.evolve_after:
                added = True
                writers["PaymentService"] = gen.payment_tsv
                active.append("PaymentService")
                print("loadgen: new source PaymentService (TSV, not in sources.yaml)", flush=True)
            if not removed and elapsed >= 2 * args.evolve_after and "LoadBalancer" in active:
                removed = True
                active.remove("LoadBalancer")
                shutil.rmtree(root / "LoadBalancer", ignore_errors=True)
                print("loadgen: source LoadBalancer removed", flush=True)

        n = int(args.rate * tick)
        n += 1 if gen.rng.random() < (args.rate * tick - n) else 0
        per: dict[str, list[dict]] = {s: [] for s in active}
        for _ in range(n):
            per[gen.rng.choice(active)].append(gen.req(now))
        if args.burst:
            per.setdefault("EdgeServer", []).extend(gen.req(now, args.burst) for _ in range(int(5 * tick) or 1))
        for source, reqs in per.items():
            if reqs and source in writers:
                writers[source](reqs)
                sent += len(reqs)
        if int(elapsed) % 10 == 0 and elapsed - int(elapsed) < tick:
            print(f"loadgen: {sent} requests written ({elapsed:.0f}s)", flush=True)
        next_t += tick
        time.sleep(max(0.0, next_t - time.monotonic()))
    print(f"loadgen: stopped after {sent} requests", flush=True)


if __name__ == "__main__":
    main()
