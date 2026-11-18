from datetime import datetime, timezone

import pytest

from csp_traffic.config import DEFAULT_ALIASES
from csp_traffic.mapping import FieldMapper, MappingError, make_timestamp_parser, resolve_mapping

from .conftest import SAMPLES, run_file, spec_for


def ts(s: str) -> float:
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp()


def records_of(res):
    """(client, endpoint, method, latency, status, ts) per client from a FileResult's stats."""
    return {cid: (c.requests, round(c.lat_sum / c.lat_n, 3)) for cid, c in res.stats.clients.items()}


@pytest.mark.parametrize(
    "source,file,expected_clients,expected_secs",
    [
        ("EdgeServer", "log1.json", {"abc123": (1, 120.0), "xyz789": (1, 250.0)}, ["2025-04-04T10:00:01"]),
        ("Microservice", "log1.csv", {"def456": (1, 180.0), "ghi789": (1, 90.0)},
         ["2025-04-04T10:00:02", "2025-04-04T10:00:03"]),
        ("APIGateway", "log1.jsonl", {"abc123": (2, 180.0), "def456": (1, 200.0)},
         ["2025-04-04T10:00:03", "2025-04-04T10:00:04", "2025-04-04T10:00:05"]),
        ("LoadBalancer", "log1.csv", {"xyz789": (1, 95.0), "abc123": (1, 105.0), "ghi789": (1, 130.0)},
         ["2025-04-04T10:00:04", "2025-04-04T10:00:05", "2025-04-04T10:00:06"]),
        ("MobileBackend", "log1.json", {"ghi789": (1, 140.0), "abc123": (1, 100.0)},
         ["2025-04-04T10:00:05", "2025-04-04T10:00:06"]),
    ],
)
def test_each_configured_source_maps_correctly(sources_cfg, source, file, expected_clients, expected_secs):
    res = run_file(SAMPLES / source / file, source, spec=spec_for(sources_cfg, source))
    assert res.rejected == 0, res.errors
    assert records_of(res) == expected_clients
    secs = sorted({s for d in res.client_seconds.values() for s in d})
    assert secs == [int(ts(s)) for s in expected_secs]


def test_status_and_method_and_endpoint_values(sources_cfg):
    res = run_file(SAMPLES / "APIGateway" / "log1.jsonl", "APIGateway", spec=spec_for(sources_cfg, "APIGateway"))
    st = res.stats
    assert dict(st.endpoints) == {"/v1/orders": 1, "/v1/users": 1, "/v1/auth": 1}
    assert st.status == [0, 2, 0, 0, 1, 0]  # 2xx x2, 5xx x1


def test_configured_fields_fall_back_to_aliases(sources_cfg):
    # Microservice config maps only client_id/latency/status; timestamp/endpoint/method come from aliases
    res = run_file(SAMPLES / "Microservice" / "log1.csv", "Microservice", spec=spec_for(sources_cfg, "Microservice"))
    assert set(res.state.mapping) == {"timestamp", "client_id", "endpoint", "method", "latency_ms", "status"}
    assert res.state.mapping["endpoint"] == {"path": "endpoint"}


def test_auto_detect_unknown_semicolon_csv(tmp_path):
    f = tmp_path / "PaymentService" / "p.txt"
    f.parent.mkdir()
    f.write_text("time;apiKey;route;verb;duration_ms;http_status\n2025-04-04T10:00:00Z;k1;/pay;POST;42;201\n")
    res = run_file(f, "PaymentService")
    assert res.state.parser == "csv" and res.state.pstate["delimiter"] == ";"
    assert res.state.mapping == {
        "timestamp": {"path": "time"},
        "client_id": {"path": "apiKey"},
        "endpoint": {"path": "route"},
        "method": {"path": "verb"},
        "latency_ms": {"path": "duration_ms"},
        "status": {"path": "http_status"},
    }
    assert records_of(res) == {"k1": (1, 42.0)}


def test_auto_detect_nested_ndjson(tmp_path):
    f = tmp_path / "Unknown" / "x.jsonl"
    f.parent.mkdir()
    f.write_text(
        '{"meta": {"client": {"id": "c9"}, "time": 1743760800}, "http": {"url": "/a", "method": "get", "status": 200,'
        ' "latency_ms": 7}}\n'
    )
    res = run_file(f, "Unknown", aliases={**DEFAULT_ALIASES, "client_id": ["id", *DEFAULT_ALIASES["client_id"]]})
    assert res.rejected == 0
    assert res.state.mapping["client_id"] == {"path": "meta.client.id"}
    assert res.state.mapping["endpoint"] == {"path": "http.url"}
    assert records_of(res) == {"c9": (1, 7.0)}


def test_transforms_scale_type_default_and_epochs():
    m = FieldMapper(
        {
            "timestamp": {"path": "t", "format": "epoch_ms"},
            "client_id": "c",
            "latency_ms": {"path": "d", "scale": 1000},
            "status": {"path": "s", "default": 200},
            "endpoint": {"path": "e", "default": "/none"},
        }
    )
    r = m.map({"t": 1743760800500, "c": 7, "d": "0.25"})
    assert (r.ts, r.client_id, r.latency_ms, r.status, r.endpoint) == (1743760800.5, "7", 250.0, 200, "/none")


def test_timestamp_formats():
    assert make_timestamp_parser(None, None)("2025-04-04T10:00:01Z") == ts("2025-04-04T10:00:01")
    assert make_timestamp_parser(None, None)(1743760801) == 1743760801
    assert make_timestamp_parser(None, None)(1743760801000) == 1743760801
    assert make_timestamp_parser("epoch_s", None)("1743760801") == 1743760801
    p = make_timestamp_parser("%d/%m/%Y %H:%M", "Asia/Kolkata")
    assert p("04/04/2025 15:30") == ts("2025-04-04T10:00:00")


def test_missing_required_fields_are_rejected():
    m = FieldMapper({"timestamp": "t", "client_id": "c"})
    with pytest.raises(MappingError):
        m.map({"t": "2025-04-04T10:00:00Z"})
    with pytest.raises(MappingError):
        m.map({"c": "x"})
    with pytest.raises(MappingError):
        m.map({"c": "x", "t": "not a time"})
    assert m.map({"c": "x", "t": 1743760800, "lat": "bad"}).latency_ms is None


def test_resolve_mapping_prefers_configured_and_avoids_reuse():
    r = resolve_mapping({"client_id": "key"}, DEFAULT_ALIASES, [{"key": "k", "ts": 1, "path": "/p", "code": 200}])
    assert r["client_id"].path == "key"
    assert r["timestamp"].path == "ts"
    assert r["endpoint"].path == "path"
    assert r["status"].path == "code"
    assert "latency_ms" not in r
