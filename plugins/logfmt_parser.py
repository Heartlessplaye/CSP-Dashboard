"""Example drop-in parser plugin: logfmt lines (`key=value key2="quoted value"`).

Shows how a brand-new serialization is supported without touching the core:
this file is loaded from the plugins directory at startup and registers the
parser name "logfmt". Use it in config/sources.yaml with `parser: logfmt`, or let
auto-detection pick it for *.logfmt files or content that looks like logfmt.
"""

from __future__ import annotations

import re
import shlex
from typing import Any

from csp_traffic.formats import ParseOutput, Parser, register
from csp_traffic.formats.base import split_complete_lines

_LOOKS_LIKE = re.compile(rb"^\s*[A-Za-z_@][\w.@-]*=\S")


@register
class LogfmtParser(Parser):
    name = "logfmt"
    mode = "stream"
    extensions = (".logfmt",)
    sniff_priority = 50

    @classmethod
    def sniff(cls, head: bytes) -> dict[str, Any] | None:
        first = head.split(b"\n", 1)[0]
        if _LOOKS_LIKE.match(first) and first.count(b"=") >= 2:
            return {}
        return None

    def parse_stream(self, buf: bytes, pstate: dict[str, Any], final: bool) -> ParseOutput:
        body, consumed = split_complete_lines(buf, final)
        out = ParseOutput(records=[], consumed=consumed)
        for line in body.decode("utf-8", "replace").splitlines():
            if not line.strip():
                continue
            try:
                tokens = shlex.split(line)
            except ValueError as exc:
                out.errors.append((f"invalid logfmt: {exc}", line[:200]))
                continue
            rec = {}
            for tok in tokens:
                key, sep, value = tok.partition("=")
                if sep:
                    rec[key] = value
            if rec:
                out.records.append(rec)
            else:
                out.errors.append(("no key=value pairs", line[:200]))
        return out
