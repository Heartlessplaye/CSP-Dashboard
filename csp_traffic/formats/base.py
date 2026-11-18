"""Parser contract and registry.

A parser turns raw bytes into raw dicts (one per API request). Mapping those dicts
to internal fields is done separately by `FieldMapper`, driven by config.

Two reading modes:
  * "stream"   - line-oriented formats (CSV, NDJSON, ...). The tailer feeds bytes from
                 the last offset; the parser consumes only complete units and reports
                 how many bytes it consumed.
  * "document" - formats where a file holds whole documents (JSON). The tailer feeds
                 either the appended bytes or the whole file; the parser returns the
                 records of all *complete* documents and the byte end of the last one.

New formats: subclass `Parser`, decorate with `@register`, and drop the module in
the plugins directory (or expose it via the `csp_traffic.parsers` entry point group).
"""

from __future__ import annotations

import importlib.util
import logging
import os
from dataclasses import dataclass, field
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, ClassVar

log = logging.getLogger(__name__)


@dataclass(slots=True)
class ParseOutput:
    records: list[dict[str, Any]]
    consumed: int
    errors: list[tuple[str, str]] = field(default_factory=list)  # (reason, raw snippet)


class Parser:
    name: ClassVar[str]
    mode: ClassVar[str] = "stream"  # "stream" | "document"
    extensions: ClassVar[tuple[str, ...]] = ()
    sniff_priority: ClassVar[int] = 100  # lower = tried first when sniffing content

    def __init__(self, options: dict[str, Any] | None = None):
        self.options = dict(options or {})

    # -- stream mode
    def parse_stream(self, buf: bytes, pstate: dict[str, Any], final: bool) -> ParseOutput:
        raise NotImplementedError

    # -- document mode
    def parse_documents(self, buf: bytes, final: bool) -> ParseOutput:
        raise NotImplementedError

    # -- auto-detection
    @classmethod
    def options_for_extension(cls, ext: str) -> dict[str, Any]:
        return {}

    @classmethod
    def sniff(cls, head: bytes) -> dict[str, Any] | None:
        """Return parser options if `head` (first bytes of a file) looks like this format."""
        return None


_REGISTRY: dict[str, type[Parser]] = {}


def register(cls: type[Parser]) -> type[Parser]:
    if not getattr(cls, "name", None):
        raise ValueError(f"parser {cls!r} needs a `name`")
    if cls.mode not in ("stream", "document"):
        raise ValueError(f"parser {cls.name}: mode must be 'stream' or 'document'")
    _REGISTRY[cls.name] = cls
    return cls


def get_parser(name: str, options: dict[str, Any] | None = None) -> Parser:
    try:
        cls = _REGISTRY[name]
    except KeyError:
        raise KeyError(f"unknown parser {name!r}; available: {sorted(_REGISTRY)}") from None
    return cls(options)


def parser_mode(name: str) -> str | None:
    cls = _REGISTRY.get(name)
    return cls.mode if cls else None


def available_parsers() -> list[str]:
    return sorted(_REGISTRY)


def detect(path: str, head: bytes) -> tuple[str, dict[str, Any]] | None:
    """Pick a parser for a file of an unconfigured source: by extension, then by content."""
    ext = os.path.splitext(path)[1].lower()
    if ext:
        for cls in _REGISTRY.values():
            if ext in cls.extensions:
                return cls.name, cls.options_for_extension(ext)
    for cls in sorted(_REGISTRY.values(), key=lambda c: c.sniff_priority):
        try:
            opts = cls.sniff(head)
        except Exception:  # a buggy plugin must not break detection
            log.exception("sniff failed in parser %s", cls.name)
            continue
        if opts is not None:
            return cls.name, opts
    return None


_loaded_plugin_dirs: set[str] = set()


def load_plugins(plugins_dir: str | os.PathLike | None) -> list[str]:
    """Import parser plugins from a directory and the `csp_traffic.parsers` entry point group."""
    loaded: list[str] = []
    if plugins_dir is not None:
        d = Path(plugins_dir)
        key = str(d.resolve())
        if d.is_dir() and key not in _loaded_plugin_dirs:
            _loaded_plugin_dirs.add(key)
            for file in sorted(d.glob("*.py")):
                if file.name.startswith("_"):
                    continue
                spec = importlib.util.spec_from_file_location(f"csp_plugin_{file.stem}", file)
                if spec is None or spec.loader is None:
                    continue
                try:
                    spec.loader.exec_module(importlib.util.module_from_spec(spec))
                    loaded.append(file.name)
                except Exception:
                    log.exception("failed to load parser plugin %s", file)
    for ep in entry_points(group="csp_traffic.parsers"):
        try:
            ep.load()
            loaded.append(ep.name)
        except Exception:
            log.exception("failed to load parser entry point %s", ep.name)
    return loaded


def split_complete_lines(buf: bytes, final: bool) -> tuple[bytes, int]:
    """Return (bytes holding only complete lines, consumed byte count).

    A trailing line without a newline is left for later, unless `final`
    (the file has been idle) in which case it is taken as complete.
    """
    if final:
        return buf, len(buf)
    end = buf.rfind(b"\n")
    if end == -1:
        return b"", 0
    return buf[: end + 1], end + 1
