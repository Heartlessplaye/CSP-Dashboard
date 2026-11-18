"""Timestamp formatting.

Stored timestamps (insights.json, checkpoints, archive folder names) are always UTC.
`timezone` in settings.yaml is display-only: the dashboard converts to it, and the
processor's log lines are printed in it (with the offset, so they stay unambiguous).
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def load_zone(name: str) -> tzinfo:
    if name.upper() in ("UTC", "Z", "ETC/UTC"):
        return timezone.utc
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError(f"unknown timezone {name!r} (use an IANA name such as 'Asia/Kolkata' or 'UTC')") from None


class TimeFormat:
    """Formats epoch seconds as ISO 8601 in the configured zone, e.g. 2025-04-04T15:30:01+05:30."""

    def __init__(self, name: str = "UTC") -> None:
        self.name = name
        self.zone = load_zone(name)
        self._utc = self.zone is timezone.utc

    def iso(self, ts: float | None) -> str | None:
        if ts is None or not math.isfinite(ts) or ts <= 0:
            return None
        s = datetime.fromtimestamp(ts, self.zone).isoformat(timespec="seconds")
        return s[:-6] + "Z" if self._utc else s

    def stamp(self, ts: float) -> str:
        """Compact form for file/folder names, e.g. 20250404T153001."""
        return datetime.fromtimestamp(ts, self.zone).strftime("%Y%m%dT%H%M%S")


class ZoneFormatter(logging.Formatter):
    """Log formatter whose %(asctime)s is in the configured zone (with offset)."""

    def __init__(self, fmt: str, zone: tzinfo) -> None:
        super().__init__(fmt)
        self.zone = zone

    def formatTime(self, record, datefmt=None):  # noqa: N802 (logging API)
        dt = datetime.fromtimestamp(record.created, self.zone)
        return dt.strftime(datefmt) if datefmt else dt.strftime("%Y-%m-%d %H:%M:%S,") + f"{int(record.msecs):03d}" + dt.strftime("%z")