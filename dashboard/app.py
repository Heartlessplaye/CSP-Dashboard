"""Read-only dashboard over the processor's output directory.

The dashboard never talks to the processor; it only reads `output/*/insights.json`,
so it can run in its own container with the output volume mounted read-only.
"""

from __future__ import annotations

import os
import re
import time
from pathlib import Path

import orjson
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response

OUTPUT_DIR = Path(os.environ.get("CSP_OUTPUT_DIR", "output"))
STATIC = Path(__file__).parent / "static"
_SAFE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

app = FastAPI(title="CSP Traffic Dashboard", docs_url="/api/docs", redoc_url=None)


def _read(source: str) -> bytes | None:
    try:
        return (OUTPUT_DIR / source / "insights.json").read_bytes()
    except (FileNotFoundError, NotADirectoryError):
        return None


def _json(data: bytes) -> Response:
    return Response(content=data, media_type="application/json", headers={"Cache-Control": "no-store"})


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-store"})


@app.get("/api/sources")
def list_sources() -> Response:
    """Per-source summaries (name, status, request/client counts, violations)."""
    out = []
    if OUTPUT_DIR.is_dir():
        for d in sorted(OUTPUT_DIR.iterdir()):
            if not d.is_dir() or d.name.startswith("_"):
                continue
            raw = _read(d.name)
            if raw is None:
                continue
            try:
                doc = orjson.loads(raw)
            except orjson.JSONDecodeError:
                continue
            out.append(
                {
                    "source": d.name,
                    "status": doc.get("status"),
                    "generated_at": doc.get("generated_at"),
                    "requests": doc.get("totals", {}).get("requests"),
                    "clients": doc.get("totals", {}).get("clients"),
                    "violations": len(doc.get("rate_limit_violations") or []),
                    "parser": (doc.get("source_meta") or {}).get("parser"),
                    "auto_detected": (doc.get("source_meta") or {}).get("auto_detected"),
                }
            )
    return _json(orjson.dumps(out))


@app.get("/api/sources/{source}")
def source_insights(source: str) -> Response:
    if not _SAFE.match(source) or source.startswith("_"):
        raise HTTPException(status_code=404, detail="unknown source")
    raw = _read(source)
    if raw is None:
        raise HTTPException(status_code=404, detail="no insights for this source yet")
    return _json(raw)


@app.get("/api/global")
def global_insights() -> Response:
    raw = _read("_global")
    if raw is None:
        raise HTTPException(status_code=404, detail="no insights yet; is the processor running?")
    return _json(raw)


@app.get("/healthz")
def healthz() -> dict:
    p = OUTPUT_DIR / "_global" / "insights.json"
    try:
        age = time.time() - p.stat().st_mtime
    except FileNotFoundError:
        return {"ok": False, "reason": "no global insights yet"}
    return {"ok": True, "global_insights_age_seconds": round(age, 1)}
