"""Mergeable usage aggregates (per client, per source).

Workers build a `SourceStats` per file chunk; the engine merges them into the
per-source and global aggregates (map-reduce), so raw records never cross
process boundaries.
"""

from __future__ import annotations

import math
from collections import Counter
from typing import Any

from .models import LogRecord

# Latency histogram: log-spaced buckets with growth factor G (≈ ±5 % relative error).
_G = 1.1
_LOG_G = math.log(_G)
STATUS_CLASSES = ("1xx", "2xx", "3xx", "4xx", "5xx", "other")


def latency_bucket(ms: float) -> int:
    return 0 if ms < 1.0 else int(math.log(ms) / _LOG_G) + 1


def bucket_value(idx: int) -> float:
    """Representative value (geometric middle) of a histogram bucket."""
    if idx == 0:
        return 0.5
    return _G ** (idx - 1) * math.sqrt(_G)


def status_index(status: int | None) -> int:
    if status is None or not 100 <= status <= 599:
        return 5
    return status // 100 - 1


class ClientStats:
    __slots__ = ("requests", "lat_n", "lat_sum", "lat_min", "lat_max", "hist", "status")

    def __init__(self) -> None:
        self.requests = 0
        self.lat_n = 0
        self.lat_sum = 0.0
        self.lat_min = math.inf
        self.lat_max = 0.0
        self.hist: dict[int, int] = {}
        self.status = [0, 0, 0, 0, 0, 0]

    def add(self, latency: float | None, status: int | None) -> None:
        self.requests += 1
        if latency is not None:
            self.lat_n += 1
            self.lat_sum += latency
            if latency < self.lat_min:
                self.lat_min = latency
            if latency > self.lat_max:
                self.lat_max = latency
            b = 0 if latency < 1.0 else int(math.log(latency) / _LOG_G) + 1
            self.hist[b] = self.hist.get(b, 0) + 1
        self.status[status_index(status)] += 1

    def merge(self, o: ClientStats) -> None:
        self.requests += o.requests
        self.lat_n += o.lat_n
        self.lat_sum += o.lat_sum
        self.lat_min = min(self.lat_min, o.lat_min)
        self.lat_max = max(self.lat_max, o.lat_max)
        h = self.hist
        for b, c in o.hist.items():
            h[b] = h.get(b, 0) + c
        for i, c in enumerate(o.status):
            self.status[i] += c

    def percentile(self, q: float) -> float | None:
        if self.lat_n == 0:
            return None
        target = q * self.lat_n
        cum = 0
        for b in sorted(self.hist):
            cum += self.hist[b]
            if cum >= target:
                return min(max(bucket_value(b), self.lat_min), self.lat_max)
        return self.lat_max

    def error_rate(self) -> float:
        known = self.requests - self.status[5]
        return (self.status[3] + self.status[4]) / known if known else 0.0

    def to_dict(self) -> dict[str, Any]:
        n = self.lat_n
        return {
            "requests": self.requests,
            "latency_ms": {
                "avg": _r(self.lat_sum / n) if n else None,
                "min": _r(self.lat_min) if n else None,
                "max": _r(self.lat_max) if n else None,
                "p50": _r(self.percentile(0.50)),
                "p95": _r(self.percentile(0.95)),
                "p99": _r(self.percentile(0.99)),
            },
            "status_classes": {name: c for name, c in zip(STATUS_CLASSES, self.status) if c},
            "error_rate": round(self.error_rate(), 4),
        }

    def __getstate__(self):
        return tuple(getattr(self, s) for s in self.__slots__)

    def __setstate__(self, state):
        for s, v in zip(self.__slots__, state):
            setattr(self, s, v)


class SourceStats:
    """Aggregates for one source (or for all sources, as the global view)."""

    def __init__(self) -> None:
        self.clients: dict[str, ClientStats] = {}
        self.endpoints: Counter[str] = Counter()
        self.minutes: dict[int, int] = {}  # epoch minute -> requests
        self.requests = 0
        self.lat_n = 0
        self.lat_sum = 0.0
        self.status = [0, 0, 0, 0, 0, 0]
        self.min_ts = math.inf
        self.max_ts = 0.0

    def add(self, r: LogRecord) -> None:
        c = self.clients.get(r.client_id)
        if c is None:
            c = self.clients[r.client_id] = ClientStats()
        c.add(r.latency_ms, r.status)
        self.endpoints[r.endpoint] += 1
        m = int(r.ts // 60)
        self.minutes[m] = self.minutes.get(m, 0) + 1
        self.requests += 1
        if r.latency_ms is not None:
            self.lat_n += 1
            self.lat_sum += r.latency_ms
        self.status[status_index(r.status)] += 1
        if r.ts > self.max_ts:
            self.max_ts = r.ts
        if r.ts < self.min_ts:
            self.min_ts = r.ts

    def merge(self, o: SourceStats) -> None:
        for cid, cs in o.clients.items():
            mine = self.clients.get(cid)
            if mine is None:
                mine = self.clients[cid] = ClientStats()
            mine.merge(cs)
        self.endpoints.update(o.endpoints)
        for m, c in o.minutes.items():
            self.minutes[m] = self.minutes.get(m, 0) + c
        self.requests += o.requests
        self.lat_n += o.lat_n
        self.lat_sum += o.lat_sum
        for i, c in enumerate(o.status):
            self.status[i] += c
        self.min_ts = min(self.min_ts, o.min_ts)
        self.max_ts = max(self.max_ts, o.max_ts)

    def trim_minutes(self, keep_from_minute: int) -> None:
        old = [m for m in self.minutes if m < keep_from_minute]
        for m in old:
            del self.minutes[m]

    def error_rate(self) -> float:
        known = self.requests - self.status[5]
        return (self.status[3] + self.status[4]) / known if known else 0.0


def _r(x: float | None) -> float | None:
    return None if x is None else round(x, 2)
