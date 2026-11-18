"""Config-driven mapping from raw parsed dicts to `LogRecord`, plus alias-based auto-detection."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Callable, Mapping

from .config import FieldSpec
from .models import INTERNAL_FIELDS, REQUIRED_FIELDS, LogRecord


class MappingError(ValueError):
    """A raw record could not be mapped (missing or invalid required field)."""


# --------------------------------------------------------------------------- helpers
def _getter(path: str) -> Callable[[Mapping[str, Any]], Any]:
    parts = path.split(".")
    if len(parts) == 1:
        return lambda d: d.get(path)

    def get(d: Mapping[str, Any]) -> Any:
        v = d.get(path)  # a literal dotted key wins
        if v is not None:
            return v
        cur: Any = d
        for p in parts:
            if isinstance(cur, dict):
                cur = cur.get(p)
            else:
                return None
        return cur

    return get


def _tzinfo(tz: str | None):
    if tz in (None, "", "UTC", "utc", "Z"):
        return timezone.utc
    from zoneinfo import ZoneInfo

    return ZoneInfo(tz)


_EPOCH_SCALE = {"epoch_s": 1.0, "epoch_ms": 1e-3, "epoch_us": 1e-6, "epoch_ns": 1e-9}


def _auto_epoch(x: float) -> float:
    ax = abs(x)
    if ax < 1e11:
        return x
    if ax < 1e14:
        return x / 1e3
    if ax < 1e17:
        return x / 1e6
    return x / 1e9


def make_timestamp_parser(fmt: str | None, tz: str | None) -> Callable[[Any], float]:
    """Return a function converting a raw timestamp value to epoch seconds."""
    tzinfo = _tzinfo(tz)
    fmt = None if fmt in (None, "", "auto") else fmt
    scale = _EPOCH_SCALE.get(fmt or "")

    def _finish(dt: datetime) -> float:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tzinfo)
        return dt.timestamp()

    @lru_cache(maxsize=65536)
    def parse_str(s: str) -> float:
        s = s.strip()
        if not s:
            raise MappingError("empty timestamp")
        try:
            if scale is not None:
                return float(s) * scale
            if fmt is None:
                if s[0].isdigit() or s[0] == "-":
                    try:
                        return _auto_epoch(float(s))
                    except ValueError:
                        pass
                return _finish(datetime.fromisoformat(s))
            if fmt == "iso":
                return _finish(datetime.fromisoformat(s))
            return _finish(datetime.strptime(s, fmt))
        except ValueError as exc:
            raise MappingError(f"invalid timestamp {s!r}: {exc}") from None

    def parse(v: Any) -> float:
        if isinstance(v, str):
            ts = parse_str(v)
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            if scale is not None:
                ts = float(v) * scale
            elif fmt is None:
                ts = _auto_epoch(float(v))
            else:
                raise MappingError(f"numeric timestamp {v!r} does not match format {fmt!r}")
        else:
            raise MappingError(f"invalid timestamp {v!r}")
        if not math.isfinite(ts):
            raise MappingError(f"invalid timestamp {v!r}")
        return ts

    return parse


def _number(v: Any, spec: FieldSpec | None, as_int: bool) -> float | int | None:
    """Lenient numeric conversion for optional fields: invalid values become None."""
    if v is None or isinstance(v, bool):
        return None
    try:
        if spec is not None and spec.type == "int":
            x: float = int(float(v))
        else:
            x = float(v)
    except (TypeError, ValueError):
        return None
    if spec is not None and spec.scale is not None:
        x = x * spec.scale
    if not math.isfinite(x):
        return None
    return int(x) if as_int else x


def _as_spec(v: str | FieldSpec | Mapping[str, Any]) -> FieldSpec:
    if isinstance(v, FieldSpec):
        return v
    if isinstance(v, str):
        return FieldSpec(path=v)
    return FieldSpec.model_validate(v)


# --------------------------------------------------------------------------- mapper
class FieldMapper:
    """Maps one raw dict to a LogRecord using a resolved {internal_field: FieldSpec} mapping."""

    def __init__(self, fields: Mapping[str, str | FieldSpec | Mapping[str, Any]]):
        self.specs: dict[str, FieldSpec] = {k: _as_spec(v) for k, v in fields.items() if k in INTERNAL_FIELDS}
        s = self.specs
        self._missing_required = [f for f in REQUIRED_FIELDS if f not in s]
        none = lambda d: None  # noqa: E731
        self._g_ts = _getter(s["timestamp"].path) if "timestamp" in s else none
        self._g_cid = _getter(s["client_id"].path) if "client_id" in s else none
        self._g_ep = _getter(s["endpoint"].path) if "endpoint" in s else none
        self._g_method = _getter(s["method"].path) if "method" in s else none
        self._g_lat = _getter(s["latency_ms"].path) if "latency_ms" in s else none
        self._g_status = _getter(s["status"].path) if "status" in s else none
        ts_spec = s.get("timestamp")
        self._ts = make_timestamp_parser(ts_spec.format if ts_spec else None, ts_spec.tz if ts_spec else None)
        self._defaults = {k: v.default for k, v in s.items() if v.default is not None}

    def describe(self) -> dict[str, Any]:
        """JSON-friendly view of the mapping (used in insights `source_meta`)."""
        out: dict[str, Any] = {}
        for k, v in self.specs.items():
            extra = v.model_dump(exclude_none=True, exclude={"path"})
            out[k] = {"path": v.path, **extra} if extra else v.path
        return out

    def map(self, raw: Mapping[str, Any]) -> LogRecord:
        if self._missing_required:
            raise MappingError(f"no source field mapped for {', '.join(self._missing_required)}")
        d = self._defaults

        v = self._g_ts(raw)
        if v is None:
            v = d.get("timestamp")
            if v is None:
                raise MappingError("missing timestamp")
        ts = self._ts(v)

        cid = self._g_cid(raw)
        if cid is None:
            cid = d.get("client_id")
        if cid is None or isinstance(cid, (dict, list)):
            raise MappingError("missing client_id")
        cid = str(cid).strip()
        if not cid:
            raise MappingError("empty client_id")

        ep = self._g_ep(raw)
        if ep is None:
            ep = d.get("endpoint", "(unknown)")
        method = self._g_method(raw)
        if method is None:
            method = d.get("method", "")

        lat = self._g_lat(raw)
        if lat is None:
            lat = d.get("latency_ms")
        latency = _number(lat, self.specs.get("latency_ms"), as_int=False)
        if latency is not None and latency < 0:
            latency = None

        st = self._g_status(raw)
        if st is None:
            st = d.get("status")
        status = _number(st, self.specs.get("status"), as_int=True)

        return LogRecord(ts, cid, str(ep), str(method).upper(), latency, status)


# --------------------------------------------------------------------------- auto-detect
def flatten_keys(obj: Mapping[str, Any], prefix: str = "", depth: int = 4) -> list[str]:
    """Dotted paths of the scalar leaves of a record (nested dicts followed up to `depth`)."""
    keys: list[str] = []
    for k, v in obj.items():
        path = f"{prefix}{k}"
        if isinstance(v, dict) and depth > 1:
            keys.extend(flatten_keys(v, path + ".", depth - 1))
        elif not isinstance(v, (dict, list)):
            keys.append(path)
    return keys


def resolve_mapping(
    configured: Mapping[str, str | FieldSpec | Mapping[str, Any]] | None,
    aliases: Mapping[str, list[str]],
    samples: list[Mapping[str, Any]],
) -> dict[str, FieldSpec]:
    """Configured fields win; every other internal field is matched against the alias lists.

    An alias matches a key's full dotted path or its last segment (case-insensitive),
    so `client.key` is found via the alias `key`.
    """
    resolved: dict[str, FieldSpec] = {k: _as_spec(v) for k, v in (configured or {}).items()}
    seen: dict[str, None] = {}
    for rec in samples[:50]:
        for key in flatten_keys(rec):
            seen.setdefault(key, None)
    keys = list(seen)
    by_full = {}
    by_last = {}
    for key in keys:
        by_full.setdefault(key.lower(), key)
        by_last.setdefault(key.rsplit(".", 1)[-1].lower(), key)
    used = {spec.path for spec in resolved.values()}

    for field in INTERNAL_FIELDS:
        if field in resolved:
            continue
        for alias in aliases.get(field, []):
            a = alias.lower()
            match = by_full.get(a)
            if match is None or match in used:
                match = by_last.get(a)
            if match is not None and match not in used:
                resolved[field] = FieldSpec(path=match)
                used.add(match)
                break
    return resolved
