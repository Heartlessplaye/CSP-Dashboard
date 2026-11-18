"""End-to-end: the engine over a real directory tree."""

import json
import shutil
import threading
import time
from pathlib import Path

import orjson

from csp_traffic.engine import Engine

from .conftest import SOURCE_NAMES, wait_for

EXPECTED_CLIENTS = {  # across the 5 sample files
    "abc123": (5, 137.0),
    "ghi789": (3, 120.0),
    "xyz789": (2, 172.5),
    "def456": (2, 190.0),
}


def load(out: Path, source: str) -> dict | None:
    p = out / source / "insights.json"
    try:
        return orjson.loads(p.read_bytes())
    except (FileNotFoundError, orjson.JSONDecodeError):
        return None


def clients(doc: dict) -> dict:
    return {c["client_id"]: (c["requests"], c["latency_ms"]["avg"]) for c in doc["clients"]}


def test_run_once_on_all_five_sources(make_settings, copy_samples):
    s = make_settings()
    copy_samples(s.input_dir)
    Engine(s).run_once()
    out = s.output_dir
    g = load(out, "_global")
    assert g["totals"]["requests"] == 12
    assert clients(g) == EXPECTED_CLIENTS
    assert g["top_endpoints"][0] == {"endpoint": "/v1/users", "count": 6, "share": 0.5}
    assert {x["source"] for x in g["sources"]} == set(SOURCE_NAMES)
    assert g["watermark"] == "2025-04-04T10:00:06Z"
    for name in SOURCE_NAMES:
        doc = load(out, name)
        assert doc["schema_version"] == "1.0" and doc["status"] == "active"
        assert doc["source_meta"]["auto_detected"] is False
        assert doc["processing"]["records_rejected"] == 0
        assert len(doc["top_endpoints"]) <= 5
    edge = load(out, "EdgeServer")
    assert clients(edge) == {"abc123": (1, 120.0), "xyz789": (1, 250.0)}
    # per-source files carry the client's global rate-limit status
    assert edge["clients"][0]["rate_limit"]["current"] == 5


def test_checkpoint_restart_does_not_double_count(make_settings, copy_samples):
    s = make_settings()
    copy_samples(s.input_dir)
    Engine(s).run_once()
    Engine(s).run_once()  # restart: restored from checkpoint, nothing new to read
    assert load(s.output_dir, "_global")["totals"]["requests"] == 12
    with open(s.input_dir / "Microservice" / "log1.csv", "a") as fh:
        fh.write("2025-04-04T10:00:07Z,def456,/v1/auth,POST,20,200\n")
    Engine(s).run_once()
    g = load(s.output_dir, "_global")
    assert g["totals"]["requests"] == 13
    assert clients(g)["def456"] == (3, round((180 + 200 + 20) / 3, 2))


def test_rate_limit_flagged_in_outputs(make_settings, copy_samples, tmp_path):
    (tmp_path / "limits.json").write_text(
        json.dumps({"default": {"limit": 1000, "window_seconds": 3600}, "clients": {"abc123": {"limit": 3}}})
    )
    s = make_settings()
    copy_samples(s.input_dir)
    Engine(s).run_once()
    g = load(s.output_dir, "_global")
    assert [v["client_id"] for v in g["rate_limit_violations"]] == ["abc123"]
    assert load(s.output_dir, "EdgeServer")["rate_limit_violations"][0]["client_id"] == "abc123"
    assert load(s.output_dir, "Microservice")["rate_limit_violations"] == []  # abc123 not in this source


def test_rejected_records_go_to_dead_letter(make_settings):
    s = make_settings()
    d = s.input_dir / "Microservice"
    d.mkdir(parents=True)
    (d / "bad.csv").write_text(
        "timestamp,client_id,endpoint,method,response_time_ms,status_code\n"
        "2025-04-04T10:00:02Z,,/v1/auth,POST,180,200\n"
        "garbage-time,c1,/v1/auth,POST,180,200\n"
        "2025-04-04T10:00:02Z,c2,/v1/auth,POST,180,200\n"
        "too,few\n"
    )
    Engine(s).run_once()
    doc = load(s.output_dir, "Microservice")
    assert doc["totals"]["requests"] == 1 and doc["processing"]["records_rejected"] == 3
    lines = (s.output_dir / "_errors" / "Microservice.jsonl").read_text().splitlines()
    assert len(lines) == 3


def _start(engine: Engine) -> threading.Thread:
    t = threading.Thread(target=engine.run, daemon=True)
    t.start()
    return t


def _live(make_settings, copy_samples, **overrides):
    s = make_settings(**overrides)
    engine = Engine(s)
    thread = _start(engine)
    try:
        t0 = time.monotonic()
        copy_samples(s.input_dir)
        ok = wait_for(lambda: (load(s.output_dir, "_global") or {}).get("totals", {}).get("requests") == 12, 5.0)
        assert ok, load(s.output_dir, "_global")
        assert time.monotonic() - t0 < 5.0
        assert clients(load(s.output_dir, "_global")) == EXPECTED_CLIENTS

        # a new source with an unseen format appears -> auto-detected and processed
        new = s.input_dir / "PaymentService"
        new.mkdir()
        (new / "pay.tsv").write_text(
            "time\tapiKey\tpath\tverb\tduration_ms\thttp_status\n2025-04-04T10:00:07Z\tnew1\t/pay\tPOST\t42\t201\n"
        )
        doc = wait_for(lambda: load(s.output_dir, "PaymentService"), 5.0)
        assert doc and doc["totals"]["requests"] == 1 and doc["source_meta"]["auto_detected"] is True
        assert doc["source_meta"]["mapping"]["client_id"] == "apiKey"

        # appended lines are picked up
        with open(s.input_dir / "LoadBalancer" / "log1.csv", "a") as fh:
            fh.write("1743760808;xyz789;/v1/users;GET;50;200\n")
        assert wait_for(lambda: load(s.output_dir, "LoadBalancer")["totals"]["requests"] == 4, 5.0)

        # a source disappears -> marked inactive, its numbers kept
        shutil.rmtree(s.input_dir / "MobileBackend")
        assert wait_for(lambda: load(s.output_dir, "MobileBackend")["status"] == "inactive", 5.0)
        assert load(s.output_dir, "MobileBackend")["totals"]["requests"] == 2

        # limits.json hot reload flags a client without new traffic
        (Path(s.limits_file)).write_text(
            json.dumps({"default": {"limit": 1000, "window_seconds": 3600}, "clients": {"ghi789": {"limit": 1}}})
        )
        assert wait_for(
            lambda: [v["client_id"] for v in load(s.output_dir, "_global")["rate_limit_violations"]] == ["ghi789"], 5.0
        )
    finally:
        engine.stop()
        thread.join(timeout=20)
    assert not thread.is_alive()


def test_live_inline_worker(make_settings, copy_samples):
    _live(make_settings, copy_samples)


def test_live_process_pool_and_watchdog(make_settings, copy_samples):
    _live(make_settings, copy_samples, workers=2, use_watchdog=True, reconcile_seconds=2.0)
