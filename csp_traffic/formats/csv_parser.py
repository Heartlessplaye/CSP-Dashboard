"""Delimited text (CSV, TSV, ';'-separated, ...).

Options:
  delimiter: a single character, or "auto" to pick the most frequent of , ; TAB | in the header
  quotechar: default '"'
  header:    true (first line is the header, default) or an explicit list of column names
Limitation: quoted fields containing newlines are not supported (log lines are single-line).
"""

from __future__ import annotations

import csv
from typing import Any

from .base import ParseOutput, Parser, register, split_complete_lines

_CANDIDATES = (",", ";", "\t", "|")


def _guess_delimiter(line: str) -> str:
    counts = {d: line.count(d) for d in _CANDIDATES}
    best = max(counts, key=lambda d: counts[d])
    return best if counts[best] > 0 else ","


@register
class CsvParser(Parser):
    name = "csv"
    mode = "stream"
    extensions = (".csv", ".tsv")
    sniff_priority = 90

    @classmethod
    def options_for_extension(cls, ext: str) -> dict[str, Any]:
        return {"delimiter": "\t"} if ext == ".tsv" else {"delimiter": "auto"}

    @classmethod
    def sniff(cls, head: bytes) -> dict[str, Any] | None:
        first = head.split(b"\n", 1)[0].decode("utf-8", "replace").strip()
        if not first or first[0] in "{[":
            return None
        if any(first.count(d) >= 1 for d in _CANDIDATES):
            return {"delimiter": "auto"}
        return None

    def parse_stream(self, buf: bytes, pstate: dict[str, Any], final: bool) -> ParseOutput:
        body, consumed = split_complete_lines(buf, final)
        out = ParseOutput(records=[], consumed=consumed)
        if not body:
            return out
        lines = body.decode("utf-8", errors="replace").splitlines()

        header: list[str] | None = pstate.get("header")
        delimiter: str | None = pstate.get("delimiter")
        start = 0
        if header is None:
            opt_header = self.options.get("header", True)
            # first non-empty line decides the delimiter (and is the header unless given)
            while start < len(lines) and not lines[start].strip():
                start += 1
            if start >= len(lines):
                return out
            delimiter = self.options.get("delimiter", ",")
            if delimiter in (None, "auto"):
                delimiter = _guess_delimiter(lines[start])
            if isinstance(opt_header, list):
                header = [str(c) for c in opt_header]
            else:
                row = next(csv.reader([lines[start]], delimiter=delimiter, quotechar=self.options.get("quotechar", '"')))
                header = [c.strip().lstrip("﻿") for c in row]
                start += 1
            pstate["header"] = header
            pstate["delimiter"] = delimiter

        ncols = len(header)
        records = out.records
        reader = csv.reader(lines[start:], delimiter=delimiter, quotechar=self.options.get("quotechar", '"'))
        for row in reader:
            if not row or (len(row) == 1 and not row[0].strip()):
                continue
            if len(row) != ncols:
                out.errors.append(
                    (f"expected {ncols} columns, got {len(row)}", (delimiter or ",").join(row)[:200])
                )
                continue
            records.append(dict(zip(header, row)))
        return out
