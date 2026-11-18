from fastapi.testclient import TestClient

import dashboard.app as dash
from csp_traffic.engine import Engine


def test_dashboard_serves_engine_output(make_settings, copy_samples, monkeypatch):
    s = make_settings()
    copy_samples(s.input_dir)
    Engine(s).run_once()
    monkeypatch.setattr(dash, "OUTPUT_DIR", s.output_dir)
    client = TestClient(dash.app)

    r = client.get("/")
    assert r.status_code == 200 and "CSP Traffic Dashboard" in r.text

    g = client.get("/api/global").json()
    assert g["totals"]["requests"] == 12

    sources = client.get("/api/sources").json()
    assert [x["source"] for x in sources] == sorted(["APIGateway", "EdgeServer", "LoadBalancer", "Microservice",
                                                       "MobileBackend"])
    assert client.get("/api/sources/EdgeServer").json()["source"] == "EdgeServer"
    assert client.get("/api/sources/_global").status_code == 404
    assert client.get("/api/sources/..%2Fetc").status_code == 404
    assert client.get("/healthz").json()["ok"] is True


def test_dashboard_before_any_output(tmp_path, monkeypatch):
    monkeypatch.setattr(dash, "OUTPUT_DIR", tmp_path / "nothing")
    client = TestClient(dash.app)
    assert client.get("/api/global").status_code == 404
    assert client.get("/api/sources").json() == []
    assert client.get("/healthz").json()["ok"] is False
