"""Checkpointing of offsets + aggregates + limiter state (one atomic snapshot)."""

from __future__ import annotations

import logging
import os
import pickle
import tempfile
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

CHECKPOINT_VERSION = 1


def save_checkpoint(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = pickle.dumps({"version": CHECKPOINT_VERSION, **payload}, protocol=pickle.HIGHEST_PROTOCOL)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def load_checkpoint(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with open(path, "rb") as fh:
            data = pickle.load(fh)
    except Exception as exc:
        log.error("could not read checkpoint %s (%s); starting fresh", path, exc)
        return None
    if not isinstance(data, dict) or data.get("version") != CHECKPOINT_VERSION:
        log.warning("checkpoint %s has an incompatible version; starting fresh", path)
        return None
    return data
