"""Insights documents (generic output schema) and atomic file writing."""

from __future__ import annotations

import heapq
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import orjson

from .metrics import SourceStats
from .ratelimit import RateLimiter

SCHEMA_VERSION = "1.0"
GLOBAL = "_global"


def iso(ts: float | None) -> str | None:
    if ts is None or not math.isfinite(ts) or ts <= 0:
        return None
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json_atomic(path: Path, obj: Any) -> None:
    """Write via a temp file + rename so readers never see a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = orjson.dumps(obj, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def build_insights(
    *,
    source: str,
    status: str,
    stats: SourceStats,
    limiter: RateLimiter,
    watermark: float,
    now: float,
    top_n: int,
    timeseries_minutes: int,
    max_clients: int,
    source_meta: dict[str, Any] | None,
    processing: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    total = stats.requests
    clients_map = stats.clients

    # busiest clients, plus every client currently over its limit
    violations = limiter.violations(only=clients_map)
    if len(clients_map) > max_clients:
        top_ids = heapq.nlargest(max_clients, clients_map, key=lambda c: clients_map[c].requests)
        listed = set(top_ids)
        top_ids.extend(v["client_id"] for v in violations if v["client_id"] not in listed)
    else:
        top_ids = sorted(clients_map, key=lambda c: clients_map[c].requests, reverse=True)

    clients = []
    for cid in top_ids:
        d = {"client_id": cid}
        d.update(clients_map[cid].to_dict())
        rl = limiter.status(cid)
        d["window_requests"] = rl["current"] if rl else 0
        d["rate_limit"] = rl
        clients.append(d)

    wm_min = int(watermark // 60) if watermark > 0 else int(now // 60)
    series = [
        [iso(m * 60), stats.minutes.get(m, 0)] for m in range(wm_min - timeseries_minutes + 1, wm_min + 1)
    ]

    doc: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "source": source,
        "status": status,
        "generated_at": iso(now),
        "watermark": iso(watermark),
        "source_meta": source_meta,
        "totals": {
            "requests": total,
            "clients": len(clients_map),
            "avg_latency_ms": round(stats.lat_sum / stats.lat_n, 2) if stats.lat_n else None,
            "error_rate": round(stats.error_rate(), 4),
            "first_event_at": iso(stats.min_ts),
            "last_event_at": iso(stats.max_ts),
            "clients_listed": len(clients),
            "clients_truncated": len(clients) < len(clients_map),
        },
        "top_endpoints": [
            {"endpoint": ep, "count": c, "share": round(c / total, 4) if total else 0.0}
            for ep, c in stats.endpoints.most_common(top_n)
        ],
        "clients": clients,
        "rate_limit_violations": violations,
        "timeseries": {"bucket_seconds": 60, "requests": series},
        "processing": processing,
    }
    if extra:
        doc.update(extra)
    return doc


class ErrorSink:
    """Appends a capped sample of rejected records to output/_errors/<source>.jsonl."""

    def __init__(self, output_dir: Path, limit: int, written: dict[str, int] | None = None) -> None:
        self.dir = Path(output_dir) / "_errors"
        self.limit = limit
        self.written: dict[str, int] = written or {}

    def write(self, source: str, file: str, errors: list[dict[str, Any]]) -> None:
        room = self.limit - self.written.get(source, 0)
        if room <= 0 or not errors:
            return
        batch = errors[:room]
        self.dir.mkdir(parents=True, exist_ok=True)
        with open(self.dir / f"{source}.jsonl", "ab") as fh:
            for e in batch:
                fh.write(orjson.dumps({"file": file, **e}, default=str, option=orjson.OPT_NON_STR_KEYS) + b"\n")
        self.written[source] = self.written.get(source, 0) + len(batch)
