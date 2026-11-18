from csp_traffic.formats import detect, get_parser, load_plugins

from .conftest import ROOT


def test_csv_header_kept_across_chunks():
    p = get_parser("csv", {"delimiter": ","})
    pstate: dict = {}
    out1 = p.parse_stream(b"a,b\n1,2\n3,", pstate, final=False)
    assert out1.records == [{"a": "1", "b": "2"}]
    assert out1.consumed == len(b"a,b\n1,2\n")
    out2 = p.parse_stream(b"3,4\n", pstate, final=False)
    assert out2.records == [{"a": "3", "b": "4"}]


def test_csv_auto_delimiter_and_column_mismatch():
    p = get_parser("csv", {"delimiter": "auto"})
    pstate: dict = {}
    out = p.parse_stream(b"x;y;z\n1;2;3\n1;2\n", pstate, final=False)
    assert pstate["delimiter"] == ";"
    assert out.records == [{"x": "1", "y": "2", "z": "3"}]
    assert len(out.errors) == 1 and "expected 3 columns" in out.errors[0][0]


def test_csv_partial_line_flushed_when_final():
    p = get_parser("csv")
    pstate: dict = {}
    assert p.parse_stream(b"a,b\n1,2", pstate, final=False).records == []
    pstate = {}
    out = p.parse_stream(b"a,b\n1,2", pstate, final=True)
    assert out.records == [{"a": "1", "b": "2"}] and out.consumed == 7


def test_ndjson_skips_bad_lines():
    p = get_parser("ndjson")
    out = p.parse_stream(b'{"a":1}\nnot json\n{"a":2}\n{"a":', {}, final=False)
    assert [r["a"] for r in out.records] == [1, 2]
    assert len(out.errors) == 1
    assert out.consumed == len(b'{"a":1}\nnot json\n{"a":2}\n')


def test_json_envelope_records_path_and_inherit():
    p = get_parser("json", {"records_path": "requests", "inherit": {"timestamp": "timestamp"}})
    out = p.parse_documents(b'{"timestamp": "T", "requests": [{"k": 1}, {"k": 2, "timestamp": "U"}]}', False)
    assert out.records == [{"k": 1, "timestamp": "T"}, {"k": 2, "timestamp": "U"}]


def test_json_auto_envelope_inherits_scalars():
    p = get_parser("json")
    out = p.parse_documents(b'{"ts": 5, "host": "h", "events": [{"k": 1}]}', False)
    assert out.records == [{"k": 1, "ts": 5, "host": "h"}]


def test_json_concatenated_and_incomplete_documents():
    p = get_parser("json", {"records_path": "$"})
    buf = b'[{"a": 1}]\n{"a": 2}\n[{"a": 3}, {"a":'
    out = p.parse_documents(buf, final=False)
    assert [r["a"] for r in out.records] == [1, 2]
    assert out.consumed == len(b'[{"a": 1}]\n{"a": 2}')
    out_final = p.parse_documents(buf, final=True)
    assert len(out_final.errors) == 1 and out_final.consumed == len(buf)


def test_detect_by_extension_and_content():
    assert detect("x/a.csv", b"")[0] == "csv"
    assert detect("x/a.tsv", b"") == ("csv", {"delimiter": "\t"})
    assert detect("x/a.jsonl", b"")[0] == "ndjson"
    assert detect("x/a.log", b'{"a":1}\n{"a":2}\n{"a":3}\n')[0] == "ndjson"
    assert detect("x/a.log", b'{\n  "requests": []\n}')[0] == "json"
    assert detect("x/a.log", b"time|key|path\n1|2|3\n")[0] == "csv"


def test_plugin_parser_is_loaded_from_plugins_dir():
    load_plugins(ROOT / "plugins")
    p = get_parser("logfmt")
    out = p.parse_stream(b'ts=1 apiKey=k1 path=/v1/x duration_ms=12 msg="hello world"\n', {}, final=False)
    assert out.records == [{"ts": "1", "apiKey": "k1", "path": "/v1/x", "duration_ms": "12", "msg": "hello world"}]
    assert detect("x/a.log", b"ts=1 apiKey=k1 path=/v1/x\n")[0] == "logfmt"
