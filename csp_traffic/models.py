"""Internal record shared by every input format."""

from __future__ import annotations

from dataclasses import dataclass

# Internal field names that source mappings target.
INTERNAL_FIELDS: tuple[str, ...] = (
    "timestamp",
    "client_id",
    "endpoint",
    "method",
    "latency_ms",
    "status",
)
REQUIRED_FIELDS: tuple[str, ...] = ("timestamp", "client_id")


@dataclass(slots=True)
class LogRecord:
    ts: float  # epoch seconds (UTC)
    client_id: str
    endpoint: str
    method: str
    latency_ms: float | None
    status: int | None
