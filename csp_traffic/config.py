"""Settings, source-format config and rate-limit config (with hot reload)."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Callable, Generic, Literal, TypeVar

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator

from .models import INTERNAL_FIELDS

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- settings
class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input_dir: Path = Path("input")
    output_dir: Path = Path("output")
    state_dir: Path = Path("state")
    sources_file: Path = Path("config/sources.yaml")
    limits_file: Path = Path("config/limits.json")
    plugins_dir: Path | None = Path("plugins")

    timezone: str = "UTC"  # display-only IANA zone (dashboard, logs); stored timestamps stay UTC

    tick_seconds: float = 1.0
    reconcile_seconds: float = 2.0
    checkpoint_seconds: float = 5.0
    use_watchdog: bool = True

    workers: int = 0
    chunk_bytes: int = 8 * 1024 * 1024
    max_bytes_per_task: int = 64 * 1024 * 1024
    idle_flush_seconds: float = 3.0

    top_n_endpoints: int = 5
    timeseries_minutes: int = 60
    warning_ratio: float = 0.8
    max_clients_in_output: int = 1000
    error_sample_limit: int = 1000
    max_future_seconds: float = 300.0
    inactive_ttl_seconds: float = 86400.0

    ignore_patterns: list[str] = Field(default_factory=lambda: [".*", "*.tmp", "*.part", "*.swp", "*~"])

    @field_validator("timezone")
    @classmethod
    def _check_timezone(cls, v: str) -> str:
        from .timefmt import load_zone

        load_zone(v)
        return v

    def effective_workers(self) -> int:
        if self.workers > 0:
            return self.workers
        return max(1, min(4, os.cpu_count() or 1))


def load_settings(path: str | os.PathLike | None) -> Settings:
    if path is None or not Path(path).exists():
        if path is not None:
            log.warning("settings file %s not found, using defaults", path)
        return Settings()
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return Settings.model_validate(data)


# --------------------------------------------------------------------------- sources
DEFAULT_ALIASES: dict[str, list[str]] = {
    "timestamp": ["timestamp", "ts", "time", "@timestamp", "event_time", "eventTime", "datetime"],
    "client_id": ["client_id", "api_key", "apiKey", "clientId", "key", "consumer", "client"],
    "endpoint": ["endpoint", "path", "route", "uri", "url"],
    "method": ["method", "http_method", "httpMethod", "verb"],
    "latency_ms": ["latency_ms", "response_time_ms", "duration_ms", "durationMs", "latency", "elapsed_ms"],
    "status": ["status", "status_code", "statusCode", "http_status", "code"],
}


class FieldSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    scale: float | None = None
    type: Literal["str", "int", "float"] | None = None
    default: Any = None
    format: str | None = None  # timestamps: auto|iso|epoch_s|epoch_ms|epoch_us|epoch_ns|strptime pattern
    tz: str | None = None  # zone for naive datetimes (default UTC)


def _check_field_names(fields: dict[str, Any]) -> dict[str, Any]:
    unknown = set(fields) - set(INTERNAL_FIELDS)
    if unknown:
        raise ValueError(f"unknown internal field(s) {sorted(unknown)}; expected {list(INTERNAL_FIELDS)}")
    return fields


class SourceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    match: str | None = None
    parser: str | None = None  # None = auto-detect per file
    options: dict[str, Any] = Field(default_factory=dict)
    fields: dict[str, str | FieldSpec] = Field(default_factory=dict)

    @field_validator("fields")
    @classmethod
    def _check_fields(cls, v: dict[str, Any]) -> dict[str, Any]:
        return _check_field_names(v)

    def field_specs(self) -> dict[str, FieldSpec]:
        return {k: (v if isinstance(v, FieldSpec) else FieldSpec(path=v)) for k, v in self.fields.items()}

    def fingerprint(self) -> str:
        return hashlib.blake2b(self.model_dump_json().encode(), digest_size=8).hexdigest()


class Defaults(BaseModel):
    model_config = ConfigDict(extra="forbid")

    aliases: dict[str, list[str]] = Field(default_factory=lambda: {k: list(v) for k, v in DEFAULT_ALIASES.items()})

    @field_validator("aliases")
    @classmethod
    def _merge_defaults(cls, v: dict[str, list[str]]) -> dict[str, list[str]]:
        _check_field_names(v)
        merged = {k: list(a) for k, a in DEFAULT_ALIASES.items()}
        merged.update(v)
        return merged


class SourcesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    defaults: Defaults = Field(default_factory=Defaults)
    sources: dict[str, SourceSpec] = Field(default_factory=dict)

    def resolve(self, source_name: str) -> SourceSpec | None:
        """Exact folder-name match first, then the first `match` glob that fits."""
        spec = self.sources.get(source_name)
        if spec is not None:
            return spec
        for spec in self.sources.values():
            if spec.match and fnmatch.fnmatchcase(source_name, spec.match):
                return spec
        return None


def load_sources(path: str | os.PathLike) -> SourcesConfig:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return SourcesConfig.model_validate(data)


# --------------------------------------------------------------------------- limits
class LimitSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    limit: int | None = None  # explicit null = unlimited; omitted = inherit default
    window_seconds: int | None = Field(default=None, gt=0)


class LimitsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default: LimitSpec = Field(default_factory=lambda: LimitSpec(limit=1000, window_seconds=3600))
    clients: dict[str, LimitSpec] = Field(default_factory=dict)

    _cache: dict[str, tuple[int | None, int]] = PrivateAttr(default_factory=dict)

    def resolve(self, client_id: str) -> tuple[int | None, int]:
        """(limit or None for unlimited, window_seconds). Client values override the default field by field."""
        hit = self._cache.get(client_id)
        if hit is not None:
            return hit
        limit = self.default.limit
        window = self.default.window_seconds or 3600
        c = self.clients.get(client_id)
        if c is not None:
            if "limit" in c.model_fields_set:
                limit = c.limit
            if c.window_seconds:
                window = c.window_seconds
        res = (limit, window)
        self._cache[client_id] = res
        return res


def load_limits(path: str | os.PathLike) -> LimitsConfig:
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    return LimitsConfig.model_validate(data)


# --------------------------------------------------------------------------- hot reload
T = TypeVar("T")


class Reloadable(Generic[T]):
    """Holds a config object loaded from a file; `check()` reloads it when the file changes.

    An invalid edit is logged and the previous valid config stays in effect.
    """

    def __init__(self, path: str | os.PathLike, loader: Callable[[Path], T], fallback: Callable[[], T]):
        self.path = Path(path)
        self._loader = loader
        self._sig: tuple[int, int] | None = None
        self.value: T = fallback()
        if self.path.exists():
            self.value = loader(self.path)  # fail fast on startup if invalid
            self._sig = self._signature()
        else:
            log.warning("config file %s not found, using defaults", self.path)

    def _signature(self) -> tuple[int, int] | None:
        try:
            st = self.path.stat()
        except FileNotFoundError:
            return None
        return (st.st_mtime_ns, st.st_size)

    def check(self) -> bool:
        sig = self._signature()
        if sig is None or sig == self._sig:
            return False
        self._sig = sig
        try:
            self.value = self._loader(self.path)
        except Exception as exc:  # keep the last good config
            log.error("invalid config %s, keeping previous version: %s", self.path, exc)
            return False
        log.info("reloaded %s", self.path)
        return True
