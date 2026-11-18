"""JSON documents: a single object/array per file, or several concatenated documents.

Options:
  records_path: dotted path to the list of records inside each document, "$" when the
                document itself is the record list (or a single record). If omitted, an
                array document is taken as the records, and for an object the first key
                holding a list of objects is used (falling back to the object itself).
  inherit:      {record_field: envelope_path} copied from the document onto each record
                (e.g. a batch-level timestamp). With an auto-detected records list, all
                scalar envelope fields are inherited.
"""

from __future__ import annotations

import json
from typing import Any

import orjson

from .base import ParseOutput, Parser, register

_WS = b" \t\r\n"
_decoder = json.JSONDecoder()


def get_path(obj: Any, path: str) -> Any:
    if isinstance(obj, dict) and path in obj:
        return obj[path]
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


@register
class JsonParser(Parser):
    name = "json"
    mode = "document"
    extensions = (".json",)
    sniff_priority = 20

    @classmethod
    def sniff(cls, head: bytes) -> dict[str, Any] | None:
        s = head.lstrip(_WS)
        if s[:1] in (b"{", b"["):
            return {}
        return None

    # ------------------------------------------------------------------ parsing
    def parse_documents(self, buf: bytes, final: bool) -> ParseOutput:
        out = ParseOutput(records=[], consumed=0)
        if not buf.strip(_WS):
            out.consumed = len(buf)
            return out
        # Fast path: the buffer is exactly one complete document.
        try:
            doc = orjson.loads(buf)
        except orjson.JSONDecodeError:
            return self._parse_concatenated(buf, final)
        self._extract(doc, out)
        out.consumed = len(buf)
        return out

    def _parse_concatenated(self, buf: bytes, final: bool) -> ParseOutput:
        out = ParseOutput(records=[], consumed=0)
        text = buf.decode("utf-8", errors="replace")
        n = len(text)
        pos = 0
        last_good = 0
        while True:
            while pos < n and text[pos] in " \t\r\n":
                pos += 1
            if pos >= n:
                last_good = n
                break
            try:
                doc, end = _decoder.raw_decode(text, pos)
            except json.JSONDecodeError as exc:
                if final:
                    out.errors.append((f"invalid JSON: {exc.msg}", text[pos : pos + 200]))
                    last_good = n
                # else: incomplete document still being written; wait for more bytes
                break
            self._extract(doc, out)
            pos = end
            last_good = end
        out.consumed = len(text[:last_good].encode("utf-8", errors="replace")) if last_good < n else len(buf)
        return out

    def _extract(self, doc: Any, out: ParseOutput) -> None:
        rp = self.options.get("records_path")
        inherit: dict[str, str] = self.options.get("inherit") or {}
        envelope: dict[str, Any] = {}

        if rp == "$":
            items = doc if isinstance(doc, list) else [doc]
        elif rp:
            items = get_path(doc, rp)
            if items is None:
                out.errors.append((f"records_path {rp!r} not found", _snippet(doc)))
                return
            if isinstance(items, dict):
                items = [items]
        elif isinstance(doc, list):
            items = doc
        elif isinstance(doc, dict):
            key = next(
                (k for k, v in doc.items() if isinstance(v, list) and v and all(isinstance(i, dict) for i in v)),
                None,
            )
            if key is None:
                items = [doc]
            else:
                items = doc[key]
                if not inherit:
                    envelope = {k: v for k, v in doc.items() if k != key and not isinstance(v, (dict, list))}
        else:
            out.errors.append(("document is not an object or array", _snippet(doc)))
            return

        if isinstance(doc, dict) and inherit:
            envelope = {field: get_path(doc, path) for field, path in inherit.items()}
            envelope = {k: v for k, v in envelope.items() if v is not None}

        if not isinstance(items, list):
            out.errors.append(("records are not a list", _snippet(items)))
            return
        records = out.records
        for item in items:
            if not isinstance(item, dict):
                out.errors.append(("record is not an object", _snippet(item)))
                continue
            if envelope:
                for k, v in envelope.items():
                    item.setdefault(k, v)
            records.append(item)


def _snippet(obj: Any) -> str:
    try:
        return orjson.dumps(obj).decode()[:200]
    except TypeError:
        return repr(obj)[:200]
