"""Newline-delimited JSON (one object per line)."""

from __future__ import annotations

from typing import Any

import orjson

from .base import ParseOutput, Parser, register, split_complete_lines


@register
class NdjsonParser(Parser):
    name = "ndjson"
    mode = "stream"
    extensions = (".jsonl", ".ndjson")
    sniff_priority = 10  # before "json": a one-object-per-line file is also valid concatenated JSON

    @classmethod
    def sniff(cls, head: bytes) -> dict[str, Any] | None:
        lines = [ln.strip() for ln in head.splitlines() if ln.strip()]
        complete = lines[:-1] if not head.endswith(b"\n") else lines
        if len(complete) < 2:
            return None
        for ln in complete[:5]:
            if not ln.startswith(b"{"):
                return None
            try:
                if not isinstance(orjson.loads(ln), dict):
                    return None
            except orjson.JSONDecodeError:
                return None
        return {}

    def parse_stream(self, buf: bytes, pstate: dict[str, Any], final: bool) -> ParseOutput:
        body, consumed = split_complete_lines(buf, final)
        out = ParseOutput(records=[], consumed=consumed)
        records = out.records
        loads = orjson.loads
        for line in body.splitlines():
            if not line.strip():
                continue
            try:
                obj = loads(line)
            except orjson.JSONDecodeError:
                out.errors.append(("invalid JSON line", line[:200].decode("utf-8", "replace")))
                continue
            if isinstance(obj, dict):
                records.append(obj)
            elif isinstance(obj, list):
                records.extend(o for o in obj if isinstance(o, dict))
            else:
                out.errors.append(("line is not a JSON object", line[:200].decode("utf-8", "replace")))
        return out
