"""Orchestration: discover files, dispatch parse tasks to workers, merge results,
enforce rate limits, write insights and checkpoints."""

from __future__ import annotations

import logging
import math
import multiprocessing as mp
import shutil
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import (
    LimitsConfig,
    Reloadable,
    Settings,
    SourcesConfig,
    load_limits,
    load_sources,
)
from .formats import load_plugins
from .metrics import SourceStats
from .output import GLOBAL, ErrorSink, build_insights, write_json_atomic
from .ratelimit import RateLimiter
from .state import load_checkpoint, save_checkpoint
from .timefmt import TimeFormat
from .tailer import FileResult, FileState, FileTask, process_file, worker_init
from .watcher import DirWatcher, FileStat

log = logging.getLogger(__name__)

ACTIVE, INACTIVE = "active", "inactive"


@dataclass
class SourceState:
    name: str
    first_seen: float
    last_seen: float
    status: str = ACTIVE
    inactive_since: float | None = None
    last_data_at: float | None = None
    parser: str | None = None
    parser_options: dict[str, Any] = field(default_factory=dict)
    mapping: dict[str, Any] | None = None
    configured: bool = False
    files: int = 0
    records: int = 0
    rejected: int = 0
    lag_ms: int | None = None
    pending_mtime: float | None = None  # oldest file mtime merged since the last write
    dirty: bool = True


class Engine:
    def __init__(self, settings: Settings) -> None:
        self.settings = s = settings
        self.tf = TimeFormat("UTC")  # stored timestamps are always UTC; settings.timezone is display-only
        load_plugins(s.plugins_dir)
        self.sources_cfg: Reloadable[SourcesConfig] = Reloadable(s.sources_file, load_sources, SourcesConfig)
        self.limits_cfg: Reloadable[LimitsConfig] = Reloadable(s.limits_file, load_limits, LimitsConfig)
        self.watcher = DirWatcher(s.input_dir, s.ignore_patterns, s.use_watchdog)
        self.checkpoint_path = Path(s.state_dir) / "checkpoint.pkl"

        self.files: dict[str, FileState] = {}
        self.sources: dict[str, SourceState] = {}
        self.stats: dict[str, SourceStats] = {}
        self.global_stats = SourceStats()
        self.limiter = RateLimiter(self.limits_cfg.value, s.warning_ratio)
        self.limiter.tf = self.tf
        self.errors = ErrorSink(s.output_dir, s.error_sample_limit)
        self.counters = {"records_parsed": 0, "records_rejected": 0, "tasks": 0}

        self.dirty: dict[str, bool] = {}  # path -> final (file idle) flag
        self.inflight: dict[Future, str] = {}
        self.pool: ProcessPoolExecutor | None = None
        self.workers = s.effective_workers()
        self._spec_cache: dict[str, tuple[dict | None, str | None]] = {}
        self._global_dirty = True
        self._stop = threading.Event()
        self._reserved_warned: set[str] = set()
        self._restore()

    # ================================================================= lifecycle
    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        s = self.settings
        log.info(
            "processor started: input=%s output=%s workers=%d timezone=%s sources configured=%s",
            s.input_dir, s.output_dir, self.workers, s.timezone, sorted(self.sources_cfg.value.sources),
        )
        self.watcher.start()
        self._start_pool()
        next_scan = next_tick = 0.0
        next_ckpt = time.monotonic() + s.checkpoint_seconds
        next_report = time.monotonic() + 30
        last_report = (time.monotonic(), self.counters["records_parsed"])
        try:
            while not self._stop.is_set():
                now = time.monotonic()
                if self._handle_events():
                    next_scan = 0.0
                if now >= next_scan:
                    self._scan()
                    next_scan = now + s.reconcile_seconds
                self._submit()
                self._collect(timeout=0.05)
                if now >= next_tick:
                    self._tick()
                    next_tick = now + s.tick_seconds
                if now >= next_ckpt:
                    self._checkpoint()
                    next_ckpt = now + s.checkpoint_seconds
                if now >= next_report:
                    t0, n0 = last_report
                    n = self.counters["records_parsed"]
                    log.info(
                        "throughput %.0f records/s | total parsed=%d rejected=%d | files=%d sources=%d",
                        (n - n0) / max(now - t0, 1e-9), n, self.counters["records_rejected"],
                        len(self.files), len(self.sources),
                    )
                    last_report = (now, n)
                    next_report = now + 30
                if not self.inflight and not self.dirty:
                    self._stop.wait(0.05)
        finally:
            self._shutdown()

    def run_once(self) -> None:
        """Process everything currently in the input directory, write outputs, exit."""
        self._start_pool()
        try:
            self._scan()
            while self.dirty or self.inflight:
                self._submit()
                self._collect(timeout=0.5)
            self._tick()
            self._checkpoint()
        finally:
            if self.pool is not None:
                self.pool.shutdown(wait=True, cancel_futures=True)
                self.pool = None

    def _start_pool(self) -> None:
        if self.workers > 1 and self.pool is None:
            plugins = str(self.settings.plugins_dir) if self.settings.plugins_dir else None
            self.pool = ProcessPoolExecutor(
                max_workers=self.workers,
                mp_context=mp.get_context("spawn"),
                initializer=worker_init,
                initargs=(plugins,),
            )

    def _shutdown(self) -> None:
        log.info("shutting down: draining %d in-flight task(s)", len(self.inflight))
        self.watcher.stop()
        while self.inflight:
            self._collect(timeout=1.0)
        if self.pool is not None:
            self.pool.shutdown(wait=True, cancel_futures=True)
            self.pool = None
        self._tick()
        self._checkpoint()
        log.info("stopped")

    # ================================================================= discovery
    def _handle_events(self) -> bool:
        """Mark files touched by notifications. Returns True if a full rescan is needed."""
        paths, structure = self.watcher.drain()
        now = time.time()
        for p in paths:
            source = self.watcher.source_of(p)
            if source is None:
                continue
            st = self.watcher.stat(p)
            if st is None:
                continue
            if self._ensure_source(source, now) is None:
                continue
            self._consider(p, source, st, now)
        return structure

    def _scan(self) -> None:
        snapshot = self.watcher.scan()
        now = time.time()
        seen: set[str] = set()
        for source, files in snapshot.items():
            src = self._ensure_source(source, now)
            if src is None:
                continue
            src.files = len(files)
            for p, st in files.items():
                seen.add(p)
                self._consider(p, source, st, now)

        busy = set(self.inflight.values())
        for p in [p for p in self.files if p not in seen and p not in busy]:
            del self.files[p]
            self.dirty.pop(p, None)

        for name, src in self.sources.items():
            if src.status == ACTIVE and name not in snapshot:
                src.status = INACTIVE
                src.inactive_since = now
                src.dirty = self._global_dirty = True
                log.warning("source %s disappeared; marked inactive", name)
            elif src.status == INACTIVE and src.inactive_since and now - src.inactive_since > self.settings.inactive_ttl_seconds:
                self._archive(name)
        for name in [n for n, s in self.sources.items() if s.status == "archived"]:
            del self.sources[name]
            self.stats.pop(name, None)

    def _ensure_source(self, name: str, now: float) -> SourceState | None:
        if name.startswith("_"):
            if name not in self._reserved_warned:
                self._reserved_warned.add(name)
                log.warning("ignoring input folder %s: names starting with '_' are reserved", name)
            return None
        src = self.sources.get(name)
        if src is None:
            spec = self.sources_cfg.value.resolve(name)
            src = self.sources[name] = SourceState(name=name, first_seen=now, last_seen=now, configured=spec is not None)
            log.info("new source %s (%s)", name, "configured" if spec else "auto-detected format")
            self._global_dirty = True
        elif src.status != ACTIVE:
            src.status = ACTIVE
            src.inactive_since = None
            src.dirty = self._global_dirty = True
            log.info("source %s is back", name)
        src.last_seen = now
        return src

    def _consider(self, path: str, source: str, st: FileStat, now: float) -> None:
        fs = self.files.get(path)
        if fs is None or fs.source != source:
            self.files[path] = FileState(path=path, source=source)
            self.dirty[path] = False
            return
        if (st.inode, st.size, st.mtime_ns) != (fs.inode, fs.size, fs.mtime_ns):
            self.dirty[path] = False
        elif fs.pending and now - st.mtime_ns / 1e9 >= self.settings.idle_flush_seconds:
            self.dirty.setdefault(path, True)

    # ================================================================= processing
    def _spec(self, source: str) -> tuple[dict | None, str | None]:
        hit = self._spec_cache.get(source)
        if hit is None:
            spec = self.sources_cfg.value.resolve(source)
            hit = (spec.model_dump(), spec.fingerprint()) if spec else (None, None)
            self._spec_cache[source] = hit
        return hit

    def _submit(self) -> None:
        if not self.dirty:
            return
        s = self.settings
        busy = set(self.inflight.values())
        room = self.workers * 2 - len(self.inflight) if self.pool else len(self.dirty)
        aliases = self.sources_cfg.value.defaults.aliases
        for path in list(self.dirty):
            if room <= 0:
                break
            if path in busy:
                continue
            final = self.dirty.pop(path)
            fs = self.files.get(path)
            if fs is None:
                continue
            src = self.sources.get(fs.source)
            if src is None or src.status != ACTIVE:
                continue
            spec, fp = self._spec(fs.source)
            task = FileTask(
                state=fs,
                spec=spec,
                spec_fp=fp,
                aliases=aliases,
                chunk_bytes=s.chunk_bytes,
                max_bytes=s.max_bytes_per_task,
                max_ts=time.time() + s.max_future_seconds,
                final=final,
            )
            if self.pool is not None:
                self.inflight[self.pool.submit(process_file, task)] = path
            else:
                self._apply(process_file(task))
            room -= 1

    def _collect(self, timeout: float) -> None:
        if not self.inflight:
            return
        done, _ = wait(list(self.inflight), timeout=timeout, return_when=FIRST_COMPLETED)
        for fut in done:
            path = self.inflight.pop(fut)
            try:
                res = fut.result()
            except BrokenProcessPool:
                log.error("worker pool crashed; restarting it")
                for p in list(self.inflight.values()) + [path]:
                    self.dirty[p] = False
                self.inflight.clear()
                if self.pool is not None:
                    self.pool.shutdown(wait=False, cancel_futures=True)
                self.pool = None
                self._start_pool()
                return
            except Exception:
                log.exception("task for %s failed", path)
                continue
            self._apply(res)

    def _apply(self, res: FileResult) -> None:
        self.counters["tasks"] += 1
        path = res.path
        if res.missing:
            self.files.pop(path, None)
            return
        if res.error:
            log.warning("could not process %s: %s", path, res.error)
            return
        fs = res.state
        src = self.sources.get(fs.source)
        if src is None:
            return
        self.files[path] = fs
        if res.reset:
            log.info("%s was replaced or rotated; reading it from the start", path)
        src.parser = fs.parser
        src.parser_options = fs.parser_options
        if fs.mapping is not None:
            src.mapping = fs.mapping
        if res.more:
            self.dirty[path] = False

        if res.parsed or res.rejected:
            if res.parsed:
                stats = self.stats.get(fs.source)
                if stats is None:
                    stats = self.stats[fs.source] = SourceStats()
                stats.merge(res.stats)
                self.global_stats.merge(res.stats)
                changed = self.limiter.ingest(res.client_seconds, res.stats.max_ts)
                if changed:
                    self._mark_clients(changed)
            src.records += res.parsed
            src.rejected += res.rejected
            src.last_data_at = time.time()
            src.pending_mtime = min(src.pending_mtime or math.inf, res.mtime)
            src.dirty = self._global_dirty = True
            self.counters["records_parsed"] += res.parsed
            self.counters["records_rejected"] += res.rejected
            if res.errors:
                self.errors.write(fs.source, path, res.errors)

    def _mark_clients(self, clients: set[str]) -> None:
        self._global_dirty = True
        for name, stats in self.stats.items():
            src = self.sources.get(name)
            if src is not None and not src.dirty and any(c in stats.clients for c in clients):
                src.dirty = True

    # ================================================================= outputs
    def _tick(self) -> None:
        if self.sources_cfg.check():
            self._spec_cache.clear()
            for name, src in self.sources.items():
                src.configured = self.sources_cfg.value.resolve(name) is not None
                src.dirty = True
        if self.limits_cfg.check():
            self.limiter.set_limits(self.limits_cfg.value)
            for src in self.sources.values():
                src.dirty = True
            self._global_dirty = True
        changed = self.limiter.tick()
        if changed:
            self._mark_clients(changed)

        wm = self.limiter.watermark
        if wm > 0:
            keep_from = int(wm // 60) - self.settings.timeseries_minutes
            for st in self.stats.values():
                st.trim_minutes(keep_from)
            self.global_stats.trim_minutes(keep_from)

        now = time.time()
        for src in self.sources.values():
            if src.dirty and src.status != "archived":
                self._write_source(src, now)
                src.dirty = False
        if self._global_dirty:
            self._write_global(now)
            self._global_dirty = False

    def _source_meta(self, src: SourceState) -> dict[str, Any]:
        mapping = None
        if src.mapping:
            mapping = {k: (v["path"] if set(v) == {"path"} else v) for k, v in src.mapping.items()}
        return {
            "parser": src.parser,
            "parser_options": src.parser_options or None,
            "mapping": mapping,
            "auto_detected": not src.configured,
            "first_seen_at": self.tf.iso(src.first_seen),
            "last_seen_at": self.tf.iso(src.last_seen),
            "last_data_at": self.tf.iso(src.last_data_at),
            "inactive_since": self.tf.iso(src.inactive_since),
        }

    def _write_source(self, src: SourceState, now: float) -> None:
        if src.pending_mtime is not None:
            src.lag_ms = max(0, round((now - src.pending_mtime) * 1000))
            src.pending_mtime = None
        s = self.settings
        doc = build_insights(
            source=src.name,
            status=src.status,
            stats=self.stats.get(src.name) or SourceStats(),
            limiter=self.limiter,
            watermark=self.limiter.watermark,
            now=now,
            top_n=s.top_n_endpoints,
            timeseries_minutes=s.timeseries_minutes,
            max_clients=s.max_clients_in_output,
            source_meta=self._source_meta(src),
            tf=self.tf,
            display_timezone=s.timezone,
            processing={
                "files_tracked": src.files,
                "records_parsed": src.records,
                "records_rejected": src.rejected,
                "lag_ms": src.lag_ms,
            },
        )
        write_json_atomic(Path(s.output_dir) / src.name / "insights.json", doc)

    def _write_global(self, now: float) -> None:
        s = self.settings
        sources = []
        for src in sorted(self.sources.values(), key=lambda x: x.name):
            st = self.stats.get(src.name)
            sources.append(
                {
                    "source": src.name,
                    "status": src.status,
                    "requests": st.requests if st else 0,
                    "clients": len(st.clients) if st else 0,
                    "records_rejected": src.rejected,
                    "parser": src.parser,
                    "auto_detected": not src.configured,
                    "last_data_at": self.tf.iso(src.last_data_at),
                    "lag_ms": src.lag_ms,
                }
            )
        lags = [src.lag_ms for src in self.sources.values() if src.lag_ms is not None and src.status == ACTIVE]
        doc = build_insights(
            source=GLOBAL,
            status=ACTIVE,
            stats=self.global_stats,
            limiter=self.limiter,
            watermark=self.limiter.watermark,
            now=now,
            top_n=s.top_n_endpoints,
            timeseries_minutes=s.timeseries_minutes,
            max_clients=s.max_clients_in_output,
            source_meta=None,
            tf=self.tf,
            display_timezone=s.timezone,
            processing={
                "files_tracked": len(self.files),
                "records_parsed": self.counters["records_parsed"],
                "records_rejected": self.counters["records_rejected"],
                "lag_ms": max(lags) if lags else None,
                "workers": self.workers,
            },
            extra={"sources": sources},
        )
        write_json_atomic(Path(s.output_dir) / GLOBAL / "insights.json", doc)

    def _archive(self, name: str) -> None:
        out = Path(self.settings.output_dir)
        src_dir = out / name
        if src_dir.exists():
            dest = out / "_archive" / f"{name}-{self.tf.stamp(time.time())}"
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src_dir), str(dest))
        log.info("archived outputs of inactive source %s", name)
        self.sources[name].status = "archived"
        self._global_dirty = True

    # ================================================================= checkpoint
    def _checkpoint(self) -> None:
        t0 = time.perf_counter()
        save_checkpoint(
            self.checkpoint_path,
            {
                "files": self.files,
                "sources": self.sources,
                "stats": self.stats,
                "global_stats": self.global_stats,
                "limiter": self.limiter,
                "counters": self.counters,
                "errors_written": self.errors.written,
            },
        )
        log.debug("checkpoint written in %.1f ms", (time.perf_counter() - t0) * 1000)

    def _restore(self) -> None:
        data = load_checkpoint(self.checkpoint_path)
        if data is None:
            return
        self.files = data["files"]
        self.sources = data["sources"]
        self.stats = data["stats"]
        self.global_stats = data["global_stats"]
        self.limiter = data["limiter"]
        self.limiter.warning_ratio = self.settings.warning_ratio
        self.limiter.tf = self.tf
        self.limiter.set_limits(self.limits_cfg.value)
        self.counters.update(data["counters"])
        self.errors.written = data["errors_written"]
        for src in self.sources.values():
            src.dirty = True
            src.configured = self.sources_cfg.value.resolve(src.name) is not None
        log.info(
            "restored checkpoint: %d files, %d sources, %d records",
            len(self.files), len(self.sources), self.counters["records_parsed"],
        )
