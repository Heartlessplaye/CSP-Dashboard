from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import Any

import pytest

from csp_traffic.config import Settings, load_sources
from csp_traffic.tailer import FileResult, FileState, FileTask, process_file

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples" / "input"
SOURCES_FILE = ROOT / "config" / "sources.yaml"
SOURCE_NAMES = ["EdgeServer", "Microservice", "APIGateway", "LoadBalancer", "MobileBackend"]


@pytest.fixture
def sources_cfg():
    return load_sources(SOURCES_FILE)


@pytest.fixture
def make_settings(tmp_path):
    def _make(**overrides: Any) -> Settings:
        limits = tmp_path / "limits.json"
        if not limits.exists():
            limits.write_text('{"default": {"limit": 1000, "window_seconds": 3600}, "clients": {}}')
        base = dict(
            input_dir=tmp_path / "input",
            output_dir=tmp_path / "output",
            state_dir=tmp_path / "state",
            sources_file=SOURCES_FILE,
            limits_file=limits,
            plugins_dir=ROOT / "plugins",
            workers=1,
            use_watchdog=False,
            tick_seconds=0.2,
            reconcile_seconds=0.3,
            checkpoint_seconds=1.0,
            idle_flush_seconds=0.5,
        )
        base.update(overrides)
        (tmp_path / "input").mkdir(exist_ok=True)
        return Settings(**base)

    return _make


@pytest.fixture
def copy_samples():
    def _copy(dest: Path, names: list[str] | None = None) -> None:
        for name in names or SOURCE_NAMES:
            shutil.copytree(SAMPLES / name, dest / name, dirs_exist_ok=True)

    return _copy


def run_file(
    path: Path,
    source: str = "src",
    spec: dict | None = None,
    state: FileState | None = None,
    final: bool = False,
    aliases: dict | None = None,
    chunk_bytes: int = 1 << 20,
) -> FileResult:
    from csp_traffic.config import DEFAULT_ALIASES

    st = state or FileState(path=str(path), source=source)
    task = FileTask(
        state=st,
        spec=spec,
        spec_fp=None if spec is None else "fp",
        aliases=aliases or DEFAULT_ALIASES,
        chunk_bytes=chunk_bytes,
        max_bytes=1 << 30,
        max_ts=time.time() + 300,
        final=final,
    )
    res = process_file(task)
    assert res.error is None, res.error
    return res


def spec_for(sources_cfg, name: str) -> dict:
    return sources_cfg.resolve(name).model_dump()


def wait_for(predicate, timeout: float = 5.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()
