"""Incremental file reading + parsing + mapping + partial aggregation (runs in worker processes).

Per-file progress lives in `FileState`, which is checkpointed so restarts resume
without double counting.

* Stream formats (CSV, NDJSON): read from the byte offset, consume only complete
  lines; a trailing partial line waits until more bytes arrive (or the file has
  been idle for `idle_flush_seconds`).
* Document formats (JSON):
  - new documents appended after the last one -> parse only the appended bytes;
  - document rewritten with more entries (e.g. an array that grows) -> re-parse the
    file and skip the first `records_consumed` records.
* Rotation / replacement: if the inode changes or the file shrinks, the first bytes
  (`head`) decide: same head -> same logical file being rewritten (continue),
  different head -> a new file at the same path (start from zero).
"""

from __future__ import annotations

import dataclasses
import os
from dataclasses import dataclass, field
from typing import Any

import orjson

from .config import FieldSpec
from .formats import detect, get_parser, load_plugins, parser_mode
from .formats.base import Parser
from .mapping import FieldMapper, MappingError, resolve_mapping
from .metrics import SourceStats
from .models import REQUIRED_FIELDS

HEAD_BYTES = 1024
TAIL_BYTES = 32
MAX_ERRORS_PER_TASK = 100


@dataclass
class FileState:
    path: str
    source: str
    inode: int = 0
    size: int = 0
    mtime_ns: int = 0
    offset: int = 0  # bytes fully consumed
    head: bytes = b""  # first bytes of the file (identity check on rotation)
    tail: bytes = b""  # bytes just before `offset` (append check for documents)
    records_consumed: int = 0
    parser: str | None = None
    parser_options: dict[str, Any] = field(default_factory=dict)
    pstate: dict[str, Any] = field(default_factory=dict)  # parser state, e.g. CSV header
    mapping: dict[str, Any] | None = None  # resolved {internal_field: FieldSpec dict}
    spec_fp: str | None = None  # fingerprint of the source config the mapping came from

    @property
    def pending(self) -> bool:
        """Unconsumed bytes remain (partial line / incomplete document)."""
        return self.offset < self.size

    def copy(self) -> FileState:
        return dataclasses.replace(
            self,
            parser_options=dict(self.parser_options),
            pstate=dict(self.pstate),
            mapping=dict(self.mapping) if self.mapping is not None else None,
        )


@dataclass
class FileTask:
    state: FileState
    spec: dict[str, Any] | None  # SourceSpec.model_dump() or None (unconfigured source)
    spec_fp: str | None
    aliases: dict[str, list[str]]
    chunk_bytes: int
    max_bytes: int
    max_ts: float  # records later than this are rejected (clock-skew guard)
    final: bool  # file idle: treat a trailing partial line as complete


@dataclass
class FileResult:
    path: str
    state: FileState
    stats: SourceStats
    client_seconds: dict[str, dict[int, int]]
    parsed: int = 0
    rejected: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    mtime: float = 0.0
    missing: bool = False
    reset: bool = False
    more: bool = False  # byte cap reached; more data to read
    error: str | None = None


# ----------------------------------------------------------------------------- worker side
_mapper_cache: dict[bytes, FieldMapper] = {}


def worker_init(plugins_dir: str | None) -> None:
    load_plugins(plugins_dir)


def _mapper(mapping: dict[str, Any]) -> FieldMapper:
    key = orjson.dumps(mapping, option=orjson.OPT_SORT_KEYS)
    m = _mapper_cache.get(key)
    if m is None:
        if len(_mapper_cache) > 256:
            _mapper_cache.clear()
        m = _mapper_cache[key] = FieldMapper(mapping)
    return m


def process_file(task: FileTask) -> FileResult:
    fs = task.state.copy()
    res = FileResult(path=fs.path, state=fs, stats=SourceStats(), client_seconds={})
    try:
        _process(task, fs, res)
    except FileNotFoundError:
        res.missing = True
    except Exception as exc:  # never let one bad file kill the pipeline
        res.error = f"{type(exc).__name__}: {exc}"
        res.state = task.state  # keep the old position; retried on next change
    return res


def _process(task: FileTask, fs: FileState, res: FileResult) -> None:
    with open(fs.path, "rb") as f:
        st = os.fstat(f.fileno())
        size = st.st_size
        head = f.read(min(HEAD_BYTES, size))

        # --- rotation / replacement detection (checked on every read: inode numbers get reused)
        if fs.inode and fs.parser:
            if parser_mode(fs.parser) == "stream":
                # bytes before the offset never change in an append-only file
                same = size >= fs.offset and _same_prefix(fs.head, head, fs.offset)
            else:
                # a rewritten document keeps its beginning; its end (closing brackets) changes
                same = _same_prefix(fs.head, head, fs.offset // 2)
            if not same:
                fs = res.state = FileState(path=fs.path, source=fs.source)
                res.reset = True
        fs.head = head
        fs.inode = st.st_ino

        # --- parser selection
        spec = task.spec or {}
        if fs.spec_fp != task.spec_fp:
            fs.mapping = None  # source config changed: re-resolve mapping for new data
            fs.spec_fp = task.spec_fp
        if spec.get("parser"):
            if fs.parser != spec["parser"]:
                fs.parser = spec["parser"]
            fs.parser_options = dict(spec.get("options") or {})
        elif fs.parser is None:
            detected = detect(fs.path, head)
            if detected is None:
                raise ValueError("could not detect file format")
            fs.parser, fs.parser_options = detected[0], detected[1]
        parser = get_parser(fs.parser, fs.parser_options)

        if parser.mode == "stream":
            records, errors = _read_stream(f, fs, parser, size, task, res)
        else:
            records, errors = _read_document(f, fs, parser, size, task)

    fs.size = size
    fs.mtime_ns = st.st_mtime_ns
    res.mtime = st.st_mtime_ns / 1e9
    for reason, raw in errors[:MAX_ERRORS_PER_TASK]:
        res.errors.append({"reason": reason, "raw": raw})
    res.rejected += len(errors)
    if records:
        _aggregate(records, fs, task, res)


def _same_prefix(old_head: bytes, new_head: bytes, limit: int) -> bool:
    """Nothing to compare (nothing consumed yet, or file emptied mid-rewrite) counts as the same file."""
    n = min(len(old_head), len(new_head), limit)
    return n == 0 or old_head[:n] == new_head[:n]


def _read_stream(f, fs: FileState, parser: Parser, size: int, task: FileTask, res: FileResult):
    records: list[dict[str, Any]] = []
    errors: list[tuple[str, str]] = []
    start = fs.offset
    f.seek(start)
    pos = start
    carry = b""
    while True:
        remaining = size - (pos + len(carry))
        if remaining <= 0 and not carry:
            break
        data = f.read(min(task.chunk_bytes, remaining)) if remaining > 0 else b""
        buf = carry + data
        at_eof = pos + len(buf) >= size or not data
        out = parser.parse_stream(buf, fs.pstate, task.final and at_eof)
        records.extend(out.records)
        errors.extend(out.errors)
        pos += out.consumed
        carry = buf[out.consumed :]
        if at_eof:
            break
        if pos - start >= task.max_bytes:
            res.more = True
            break
    fs.offset = pos
    fs.records_consumed += len(records)
    return records, errors


def _read_document(f, fs: FileState, parser: Parser, size: int, task: FileTask):
    records: list[dict[str, Any]] = []
    appended = False
    if fs.offset > 0 and fs.tail and size >= fs.offset:
        f.seek(fs.offset - len(fs.tail))
        appended = f.read(len(fs.tail)) == fs.tail

    if appended:
        buf = f.read(size - fs.offset)
        if not buf:
            return records, []
        out = parser.parse_documents(buf, task.final)
        records = out.records
        new_offset = fs.offset + out.consumed
        fs.records_consumed += len(records)
        f.seek(max(0, new_offset - TAIL_BYTES))
        fs.tail = f.read(new_offset - max(0, new_offset - TAIL_BYTES))
        fs.offset = new_offset
        return records, out.errors

    # whole-file (re)parse: first read, or the document was rewritten in place
    f.seek(0)
    buf = f.read(size)
    out = parser.parse_documents(buf, task.final)
    total = len(out.records)
    if total < fs.records_consumed:
        return records, []  # rewrite in progress (fewer complete records than before): wait
    records = out.records[fs.records_consumed :]
    fs.records_consumed = total
    fs.offset = out.consumed
    fs.tail = buf[max(0, out.consumed - TAIL_BYTES) : out.consumed]
    return records, out.errors


def _aggregate(records: list[dict[str, Any]], fs: FileState, task: FileTask, res: FileResult) -> None:
    if fs.mapping is None:
        configured = (task.spec or {}).get("fields") or {}
        resolved = resolve_mapping(configured, task.aliases, records)
        as_dict = {k: v.model_dump(exclude_none=True) for k, v in resolved.items()}
        if all(f in resolved for f in REQUIRED_FIELDS):
            fs.mapping = as_dict  # persist only a usable mapping; otherwise retry next chunk
        mapping = as_dict
    else:
        mapping = fs.mapping
    mapper = _mapper(mapping)

    stats = res.stats
    cs = res.client_seconds
    max_ts = task.max_ts
    add = stats.add
    errors = res.errors
    rejected = 0
    for raw in records:
        try:
            r = mapper.map(raw)
        except MappingError as exc:
            rejected += 1
            if len(errors) < MAX_ERRORS_PER_TASK:
                errors.append({"reason": str(exc), "raw": raw})
            continue
        if r.ts > max_ts:
            rejected += 1
            if len(errors) < MAX_ERRORS_PER_TASK:
                errors.append({"reason": "timestamp too far in the future", "raw": raw})
            continue
        add(r)
        d = cs.get(r.client_id)
        if d is None:
            d = cs[r.client_id] = {}
        sec = int(r.ts)
        d[sec] = d.get(sec, 0) + 1
    res.parsed += len(records) - rejected
    res.rejected += rejected


def field_specs_from_state(fs: FileState) -> dict[str, FieldSpec]:
    return {k: FieldSpec.model_validate(v) for k, v in (fs.mapping or {}).items()}
