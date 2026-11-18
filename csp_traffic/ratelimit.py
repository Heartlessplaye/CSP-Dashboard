"""Per-client sliding-window rate-limit monitoring on event (log) time.

Each client has one limit and window (from limits.json). The window is split into
60 buckets (bucket = window / 60, e.g. 60 s buckets for 1 h, 1 s buckets for 1 min)
kept in a small ring. The window count is the sum of the ring's buckets, and
buckets that slide out of the window are zeroed as the ring advances.

"Now" is the watermark: the latest event timestamp seen across all sources, so
replayed or late logs are judged by when the requests happened, not when the
files arrived. Status is checked on ingest (at every bucket boundary, so spikes
inside a batch are caught) and on every output tick (so idle clients recover).
This module flags clients; it does not block traffic.
"""

from __future__ import annotations

import math
from array import array
from typing import Any

from .config import LimitsConfig

BUCKETS = 60
OK, WARNING, EXCEEDED, NO_LIMIT = "OK", "WARNING", "EXCEEDED", "NO_LIMIT"


class ClientWindow:
    __slots__ = (
        "limit",
        "window",
        "bucket_s",
        "ring",
        "head",
        "current",
        "peak",
        "status",
        "exceeded_since",
        "last_exceeded_at",
    )

    def __init__(self, limit: int | None, window: int) -> None:
        self.limit = limit
        self.current = 0
        self.peak = 0
        self.status = OK if limit is not None else NO_LIMIT
        self.exceeded_since: float | None = None
        self.last_exceeded_at: float | None = None
        self.reset_window(window)

    def reset_window(self, window: int) -> None:
        self.window = window
        self.bucket_s = max(1.0, window / BUCKETS)
        self.ring = array("I", bytes(4 * BUCKETS))
        self.head: int | None = None  # absolute index of the newest bucket
        self.current = 0

    def bucket_of(self, ts: float) -> int:
        return int(ts // self.bucket_s)

    def advance(self, b: int) -> None:
        if self.head is None:
            self.head = b
            return
        steps = b - self.head
        if steps <= 0:
            return
        ring = self.ring
        if steps >= BUCKETS:
            for i in range(BUCKETS):
                ring[i] = 0
        else:
            for i in range(self.head + 1, b + 1):
                ring[i % BUCKETS] = 0
        self.head = b

    def add(self, b: int, count: int) -> None:
        if self.head is None or b > self.head:
            self.advance(b)
        elif b <= self.head - BUCKETS:
            return  # older than the window: counted in totals, not in the limit
        self.ring[b % BUCKETS] += count

    def evaluate(self, at_ts: float, warning_ratio: float) -> bool:
        """Recompute current/status for the window ending at the ring head. Returns True if status changed."""
        cur = sum(self.ring)
        self.current = cur
        if cur > self.peak:
            self.peak = cur
        prev = self.status
        if self.limit is None:
            self.status = NO_LIMIT
        elif cur > self.limit:
            self.status = EXCEEDED
            if self.exceeded_since is None:
                self.exceeded_since = at_ts
            self.last_exceeded_at = at_ts
        else:
            self.status = WARNING if cur >= warning_ratio * self.limit else OK
            self.exceeded_since = None
        return self.status != prev

    def __getstate__(self):
        return tuple(getattr(self, s) for s in self.__slots__)

    def __setstate__(self, state):
        for s, v in zip(self.__slots__, state):
            setattr(self, s, v)


class RateLimiter:
    def __init__(self, limits: LimitsConfig, warning_ratio: float = 0.8) -> None:
        self.limits = limits
        self.warning_ratio = warning_ratio
        self.windows: dict[str, ClientWindow] = {}
        self.active: set[str] = set()  # clients with a non-empty window
        self.watermark = 0.0

    # ----------------------------------------------------------------- config
    def set_limits(self, limits: LimitsConfig) -> None:
        """Apply a reloaded limits.json. A changed window size restarts that client's window."""
        self.limits = limits
        for cid, w in self.windows.items():
            limit, window = limits.resolve(cid)
            w.limit = limit
            if window != w.window:
                w.reset_window(window)
            self.active.add(cid)

    def _window(self, cid: str) -> ClientWindow:
        w = self.windows.get(cid)
        if w is None:
            limit, window = self.limits.resolve(cid)
            w = self.windows[cid] = ClientWindow(limit, window)
        return w

    # ----------------------------------------------------------------- ingest
    def ingest(self, client_seconds: dict[str, dict[int, int]], max_ts: float) -> set[str]:
        """Add per-client per-second request counts. Returns clients whose status changed."""
        if max_ts > self.watermark:
            self.watermark = max_ts
        changed: set[str] = set()
        ratio = self.warning_ratio
        for cid, secs in client_seconds.items():
            w = self._window(cid)
            bs = w.bucket_s
            touched = False
            for sec in sorted(secs):
                b = int(sec // bs)
                if w.head is not None and b > w.head and touched:
                    # crossing a bucket boundary: check the window that just closed
                    if w.evaluate((w.head + 1) * bs, ratio):
                        changed.add(cid)
                w.add(b, secs[sec])
                touched = True
            if w.evaluate(min(self.watermark, (w.head + 1) * bs), ratio):
                changed.add(cid)
            self.active.add(cid)
        return changed

    # ----------------------------------------------------------------- tick
    def tick(self) -> set[str]:
        """Slide active clients' windows to the watermark. Returns clients whose status changed."""
        changed: set[str] = set()
        wm = self.watermark
        idle: list[str] = []
        for cid in self.active:
            w = self.windows[cid]
            w.advance(w.bucket_of(wm))
            if w.evaluate(wm, self.warning_ratio):
                changed.add(cid)
            if w.current == 0:
                idle.append(cid)
        self.active.difference_update(idle)
        return changed

    # ----------------------------------------------------------------- views
    def status(self, cid: str) -> dict[str, Any] | None:
        w = self.windows.get(cid)
        if w is None:
            return None
        return {
            "limit": w.limit,
            "window_seconds": w.window,
            "current": w.current,
            "utilization": round(w.current / w.limit, 4) if w.limit else None,
            "status": w.status,
            "peak": w.peak,
            "exceeded_since": _iso(w.exceeded_since),
            "last_exceeded_at": _iso(w.last_exceeded_at),
        }

    def violations(self, only: set[str] | dict | None = None) -> list[dict[str, Any]]:
        out = []
        for cid in self.active:
            w = self.windows[cid]
            if w.status != EXCEEDED or (only is not None and cid not in only):
                continue
            out.append(
                {
                    "client_id": cid,
                    "current": w.current,
                    "limit": w.limit,
                    "window_seconds": w.window,
                    "utilization": round(w.current / w.limit, 4) if w.limit else None,
                    "since": _iso(w.exceeded_since),
                }
            )
        out.sort(key=lambda v: v["utilization"] or 0, reverse=True)
        return out

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("limits", None)  # always re-read from limits.json
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.limits = LimitsConfig()


def _iso(ts: float | None) -> str | None:
    if ts is None or not math.isfinite(ts):
        return None
    from datetime import datetime, timezone

    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
