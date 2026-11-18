import json
import os

from .conftest import run_file, spec_for

CSV_SPEC = {"parser": "csv", "options": {"delimiter": ","}, "fields": {}}
HEADER = "timestamp,client_id,endpoint,method,latency_ms,status\n"


def row(i: int, client: str = "c1") -> str:
    return f"2025-04-04T10:00:{i % 60:02d}Z,{client},/v1/x,GET,{10 + i},200\n"


def test_csv_append_reads_only_new_lines(tmp_path):
    f = tmp_path / "a.csv"
    f.write_text(HEADER + row(1) + row(2))
    r1 = run_file(f, spec=CSV_SPEC)
    assert r1.parsed == 2
    with open(f, "a") as fh:
        fh.write(row(3))
    r2 = run_file(f, spec=CSV_SPEC, state=r1.state)
    assert r2.parsed == 1 and r2.stats.requests == 1
    r3 = run_file(f, spec=CSV_SPEC, state=r2.state)
    assert r3.parsed == 0


def test_partial_line_waits_until_completed(tmp_path):
    f = tmp_path / "a.csv"
    f.write_text(HEADER + row(1) + "2025-04-04T10:00:02Z,c1,/v1")
    r1 = run_file(f, spec=CSV_SPEC)
    assert r1.parsed == 1 and r1.state.pending
    with open(f, "a") as fh:
        fh.write("/x,GET,5,200\n")
    r2 = run_file(f, spec=CSV_SPEC, state=r1.state)
    assert r2.parsed == 1 and not r2.state.pending
    assert list(r2.stats.endpoints) == ["/v1/x"]


def test_partial_line_flushed_when_file_idle(tmp_path):
    f = tmp_path / "a.csv"
    f.write_text(HEADER + row(1).rstrip("\n"))
    assert run_file(f, spec=CSV_SPEC).parsed == 0
    assert run_file(f, spec=CSV_SPEC, final=True).parsed == 1


def test_small_chunks_give_same_result(tmp_path):
    f = tmp_path / "a.csv"
    f.write_text(HEADER + "".join(row(i, f"c{i % 3}") for i in range(200)))
    res = run_file(f, spec=CSV_SPEC, chunk_bytes=64)
    assert res.parsed == 200 and res.state.offset == f.stat().st_size


def test_rewritten_json_document_is_not_double_counted(tmp_path, sources_cfg):
    spec = spec_for(sources_cfg, "MobileBackend")
    f = tmp_path / "m.json"

    def rec(i):
        return {"eventTime": f"2025-04-04 10:00:{i:02d}", "clientId": f"c{i}", "route": "/r", "httpMethod": "GET",
                "durationMs": i, "statusCode": 200}

    f.write_text(json.dumps([rec(1), rec(2)]))
    r1 = run_file(f, spec=spec)
    assert r1.parsed == 2
    # writer rewrites the whole array with one more entry, via temp file + rename (new inode)
    tmp = tmp_path / "m.json.tmp"
    tmp.write_text(json.dumps([rec(1), rec(2), rec(3)]))
    os.replace(tmp, f)
    r2 = run_file(f, spec=spec, state=r1.state)
    assert r2.parsed == 1 and set(r2.stats.clients) == {"c3"} and not r2.reset
    # half-written rewrite: fewer complete records than already counted -> wait, count nothing
    f.write_text(json.dumps([rec(1), rec(2), rec(3)])[:-20])
    r3 = run_file(f, spec=spec, state=r2.state)
    assert r3.parsed == 0
    f.write_text(json.dumps([rec(1), rec(2), rec(3), rec(4)]))
    r4 = run_file(f, spec=spec, state=r3.state)
    assert r4.parsed == 1 and set(r4.stats.clients) == {"c4"}


def test_appended_json_documents_parse_only_new_bytes(tmp_path, sources_cfg):
    spec = spec_for(sources_cfg, "EdgeServer")
    f = tmp_path / "e.json"

    def doc(i):
        return json.dumps({"timestamp": f"2025-04-04T10:00:{i:02d}Z",
                           "requests": [{"api_key": f"k{i}", "endpoint": "/e", "method": "GET", "latency_ms": 1,
                                         "status": 200}]}) + "\n"

    f.write_text(doc(1))
    r1 = run_file(f, spec=spec)
    with open(f, "a") as fh:
        fh.write(doc(2) + doc(3))
    r2 = run_file(f, spec=spec, state=r1.state)
    assert r2.parsed == 2 and set(r2.stats.clients) == {"k2", "k3"}


def test_truncation_with_new_content_restarts_from_zero(tmp_path):
    f = tmp_path / "a.csv"
    f.write_text(HEADER + row(1) + row(2) + row(3))
    r1 = run_file(f, spec=CSV_SPEC)
    # copytruncate-style rotation: same inode, shorter file with different content
    with open(f, "w") as fh:
        fh.write("timestamp,client_id,endpoint,method,latency_ms,status\n" + row(30, "new"))
    r2 = run_file(f, spec=CSV_SPEC, state=r1.state)
    assert r2.reset and r2.parsed == 1 and set(r2.stats.clients) == {"new"}


def test_replaced_file_with_different_content_restarts(tmp_path):
    f = tmp_path / "a.csv"
    f.write_text(HEADER + row(1, "old"))
    r1 = run_file(f, spec=CSV_SPEC)
    os.unlink(f)
    f.write_text("timestamp,client_id,endpoint,method,latency_ms,status\n" + row(9, "fresh") + row(10, "fresh"))
    r2 = run_file(f, spec=CSV_SPEC, state=r1.state)
    assert r2.reset and r2.parsed == 2


def test_deleted_file_reports_missing(tmp_path):
    f = tmp_path / "gone.csv"
    f.write_text(HEADER)
    r1 = run_file(f, spec=CSV_SPEC)
    f.unlink()
    from csp_traffic.tailer import FileTask, process_file

    res = process_file(FileTask(r1.state, CSV_SPEC, "fp", {}, 1024, 1 << 20, 2e9, False))
    assert res.missing
