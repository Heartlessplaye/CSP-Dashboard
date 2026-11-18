import json
import os
import random
import time

from csp_traffic.config import LimitsConfig, Reloadable, load_limits
from csp_traffic.metrics import ClientStats, SourceStats
from csp_traffic.models import LogRecord


def test_percentiles_are_within_histogram_error():
    rng = random.Random(1)
    values = [rng.lognormvariate(4.5, 0.8) for _ in range(20_000)]
    c = ClientStats()
    for v in values:
        c.add(v, 200)
    values.sort()
    for q in (0.5, 0.95, 0.99):
        exact = values[int(q * len(values)) - 1]
        assert abs(c.percentile(q) - exact) / exact < 0.06
    assert abs(c.lat_sum / c.lat_n - sum(values) / len(values)) < 1e-6


def test_merge_equals_single_pass():
    rng = random.Random(2)
    recs = [
        LogRecord(1_743_760_800 + rng.randint(0, 3600), f"c{rng.randint(0, 9)}", f"/e{rng.randint(0, 6)}", "GET",
                  rng.uniform(1, 500), rng.choice([200, 201, 404, 500, None]))
        for _ in range(2000)
    ]
    whole = SourceStats()
    for r in recs:
        whole.add(r)
    parts = [SourceStats() for _ in range(4)]
    for i, r in enumerate(recs):
        parts[i % 4].add(r)
    merged = SourceStats()
    for p in parts:
        merged.merge(p)
    assert merged.requests == whole.requests and merged.endpoints == whole.endpoints
    assert merged.minutes == whole.minutes and merged.status == whole.status
    for cid, c in whole.clients.items():
        m = merged.clients[cid]
        assert (m.requests, m.lat_n, m.hist, m.status) == (c.requests, c.lat_n, c.hist, c.status)
        assert abs(m.lat_sum - c.lat_sum) < 1e-6


def test_limits_resolution_field_by_field():
    cfg = LimitsConfig.model_validate(
        {"default": {"limit": 1000, "window_seconds": 3600},
         "clients": {"a": {"limit": 5}, "b": {"window_seconds": 60}, "c": {"limit": None}}}
    )
    assert cfg.resolve("a") == (5, 3600)
    assert cfg.resolve("b") == (1000, 60)
    assert cfg.resolve("c") == (None, 3600)
    assert cfg.resolve("zzz") == (1000, 3600)


def test_reloadable_keeps_last_good_config(tmp_path):
    p = tmp_path / "limits.json"
    p.write_text(json.dumps({"default": {"limit": 1, "window_seconds": 60}}))
    r = Reloadable(p, load_limits, LimitsConfig)
    assert r.value.default.limit == 1
    p.write_text("{ not json")
    os.utime(p, (time.time() + 1, time.time() + 1))
    assert r.check() is False and r.value.default.limit == 1
    p.write_text(json.dumps({"default": {"limit": 2, "window_seconds": 60}}))
    os.utime(p, (time.time() + 2, time.time() + 2))
    assert r.check() is True and r.value.default.limit == 2


def test_sources_config_resolution(sources_cfg):
    assert sources_cfg.resolve("EdgeServer").parser == "json"
    assert sources_cfg.resolve("EdgeServer-eu-west").parser == "json"  # glob match
    assert sources_cfg.resolve("Brand-new") is None
    assert len(sources_cfg.sources) >= 5
