import json
import logging

import orjson
import pytest
from pydantic import ValidationError

from csp_traffic.config import Settings
from csp_traffic.engine import Engine
from csp_traffic.timefmt import TimeFormat, ZoneFormatter, load_zone

T = 1743760806  # 2025-04-04T10:00:06Z


def test_time_format_in_configured_zone():
    assert TimeFormat("UTC").iso(T) == "2025-04-04T10:00:06Z"
    ist = TimeFormat("Asia/Kolkata")
    assert ist.iso(T) == "2025-04-04T15:30:06+05:30"
    assert ist.stamp(T) == "20250404T153006"
    assert TimeFormat("UTC").stamp(T) == "20250404T100006"
    assert ist.iso(None) is None and ist.iso(0) is None


def test_dst_zone_offset_follows_date():
    ny = TimeFormat("America/New_York")
    assert ny.iso(1735689600) == "2024-12-31T19:00:00-05:00"  # winter
    assert ny.iso(1751328000) == "2025-06-30T20:00:00-04:00"  # summer


def test_invalid_timezone_rejected_by_settings():
    with pytest.raises(ValidationError):
        Settings(timezone="Asia/Koltkata")
    assert Settings(timezone="Asia/Kolkata").timezone == "Asia/Kolkata"
    assert Settings().timezone == "UTC"


def test_log_formatter_uses_zone():
    rec = logging.LogRecord("x", logging.INFO, __file__, 1, "hello", None, None)
    rec.created, rec.msecs = T, 0
    line = ZoneFormatter("%(asctime)s %(message)s", load_zone("Asia/Kolkata")).format(rec)
    assert line == "2025-04-04 15:30:06,000+0530 hello"


def test_insights_store_utc_and_carry_display_timezone(make_settings, copy_samples, tmp_path):
    """Storage is UTC regardless of the setting; the zone is only a display hint for viewers."""
    (tmp_path / "limits.json").write_text(
        json.dumps({"default": {"limit": 1000, "window_seconds": 3600}, "clients": {"abc123": {"limit": 3}}})
    )
    s = make_settings(timezone="Asia/Kolkata")
    copy_samples(s.input_dir)
    Engine(s).run_once()
    for name in ("_global", "EdgeServer"):
        doc = orjson.loads((s.output_dir / name / "insights.json").read_bytes())
        assert doc["display_timezone"] == "Asia/Kolkata"
        assert doc["watermark"] == "2025-04-04T10:00:06Z"
        assert doc["generated_at"].endswith("Z")
        assert doc["totals"]["first_event_at"].endswith("Z")
        assert doc["timeseries"]["requests"][-1][0] == "2025-04-04T10:00:00Z"
        v = doc["rate_limit_violations"][0]
        assert v["client_id"] == "abc123" and v["since"].endswith("Z")
        abc = next(c for c in doc["clients"] if c["client_id"] == "abc123")
        assert abc["rate_limit"]["last_exceeded_at"].endswith("Z")
    meta = orjson.loads((s.output_dir / "EdgeServer" / "insights.json").read_bytes())["source_meta"]
    assert meta["first_seen_at"].endswith("Z") and meta["last_data_at"].endswith("Z")


def test_display_timezone_defaults_to_utc(make_settings, copy_samples):
    s = make_settings()
    copy_samples(s.input_dir, ["EdgeServer"])
    Engine(s).run_once()
    assert orjson.loads((s.output_dir / "_global" / "insights.json").read_bytes())["display_timezone"] == "UTC"
