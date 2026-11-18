# CSP Traffic Dashboard

Real-time API usage insights and rate-limit monitoring for a cloud service provider.

The processor watches `input/<sourceId>/` for API request logs (new files, appended lines and rewritten
documents, in any format a source happens to use). It maps every record to one internal schema through
config and computes per-client and per-source metrics. It flags clients over their rate limits and keeps
`output/<sourceId>/insights.json` up to date within about a second. A small web dashboard displays those
files.

```
input/EdgeServer/*.json ┐                                         ┌ output/EdgeServer/insights.json
input/Microservice/*.csv├─► processor ─ parse ─ map ─ aggregate ─┤ output/…/insights.json
input/<new source>/*    ┘   (watch)     (config)   (rate limits) └ output/_global/insights.json ─► dashboard :8000
```

Everything runs in Docker; nothing needs to be installed on the host.

## Quick start

```bash
make demo        # processor + dashboard + synthetic traffic for all 5 sources
open http://localhost:8000
make logs        # follow the logs
make down        # stop
```

| Command | What it does |
|---|---|
| `make up` | Processor + dashboard only. Put your own logs in `input/<sourceId>/`. |
| `make demo` | Same plus `tools/loadgen.py`. After 30 s a new, unconfigured source (`PaymentService`, TSV) appears. After 60 s `LoadBalancer` disappears. Set `LOADGEN_RATE` (default 2000 req/s) and `LOADGEN_SCENARIO` (`steady` or `evolve`). |
| `make test` | Runs the pytest suite in a container. |
| `make bench` | Throughput and lag benchmark (1M records). |
| `make clean-data` | Empties `input/`, `output/` and `state/`. |

`docker compose` runs as your UID, so files in `input/`, `output/` and `state/` stay owned by you. The
Makefile exports `HOST_UID` and `HOST_GID`. If you call `docker compose` directly, export them yourself.

## Sources and formats

Five sources are configured in [config/sources.yaml](config/sources.yaml). Each has its own layout:

| Source | Format | What's different |
|---|---|---|
| EdgeServer | JSON object, records under `requests` | Timestamp is on the envelope and inherited by each record |
| Microservice | CSV, `,` | `client_id`, `response_time_ms`, `status_code` |
| APIGateway | JSON lines | Nested `client.key` / `request.path`, epoch-ms timestamp, latency in **seconds** |
| LoadBalancer | CSV, `;` | `apiKey`, `latency`, `http_status`, epoch-seconds timestamp |
| MobileBackend | JSON array | camelCase fields, `2025-04-04 10:00:05` timestamps |

Sample files are in [samples/input/](samples/input/). To try them: `cp -r samples/input/* input/`.

### Adding a source or format

1. **Known serialization (JSON / NDJSON / CSV-like), new field names.** Add an entry to `sources.yaml`.
   The file is hot-reloaded, so no restart is needed:
   ```yaml
   sources:
     BillingAPI:
       parser: csv
       options: { delimiter: "|" }
       fields:
         client_id: tenant
         latency_ms: { path: took_s, scale: 1000 }
         timestamp:  { path: at, format: epoch_ms }
   ```
   A field spec is a dotted path (`meta.client.id`) or `{path, scale, type, default, format, tz}`.
   Timestamp formats are `auto`, `iso`, `epoch_s`, `epoch_ms`, `epoch_us`, `epoch_ns`, or a `strptime`
   pattern. Fields you leave out are matched through the alias lists under `defaults.aliases`.
2. **No config at all.** A new folder is picked up automatically. The parser comes from the file extension
   or by sniffing the content. Fields are matched through the aliases, including nested keys by their last
   segment (`client.key` matches alias `key`). The mapping it chose appears in
   `insights.json → source_meta.mapping`, so you can copy it into `sources.yaml` to pin it.
3. **New serialization (XML, Parquet, logfmt, …).** Drop a parser plugin into [plugins/](plugins/). See
   [plugins/logfmt_parser.py](plugins/logfmt_parser.py), a complete 50-line example. Plugins load at
   startup, so restart the processor once.

Records that can't be mapped don't stop processing. A sample of them goes to `output/_errors/<source>.jsonl`,
and they are counted in `processing.records_rejected`.

## Rate limits

[config/limits.json](config/limits.json) is hot-reloaded:

```json
{
  "default": { "limit": 1000, "window_seconds": 3600 },
  "clients": {
    "abc123": { "limit": 5000 },
    "xyz789": { "limit": 100, "window_seconds": 60 },
    "internal-batch": { "limit": null }
  }
}
```

- **Overrides:** client entries override the default field by field. `"limit": null` means unlimited.
- **Counting:** each client is counted across **all sources**, over a sliding window measured on the
  **log timestamps**. "Now" is the newest event seen, so replayed or late logs are judged by when the
  requests actually happened.
- **Status:** `OK` below 80 % of the limit, then `WARNING`, then `EXCEEDED` once over it.
- **Flag only:** this system flags clients from logs; it does not block traffic.

## Output

Each source gets `output/<sourceId>/insights.json`. `output/_global/insights.json` has the same shape
across all sources, plus a `sources` list:

```jsonc
{
  "schema_version": "1.0",
  "source": "EdgeServer",
  "status": "active",                 // "inactive" once its folder disappears
  "display_timezone": "Asia/Kolkata",  // settings.yaml `timezone`; all timestamps here are UTC
  "generated_at": "2025-04-04T10:00:07Z", "watermark": "2025-04-04T10:00:06Z",
  "source_meta": { "parser": "json", "mapping": { "client_id": "api_key", … }, "auto_detected": false },
  "totals": { "requests": 1209, "clients": 268, "avg_latency_ms": 161.4, "error_rate": 0.07 },
  "top_endpoints": [ { "endpoint": "/v1/users", "count": 371, "share": 0.31 } ],
  "clients": [ {
      "client_id": "abc123", "requests": 52, "window_requests": 230,
      "latency_ms": { "avg": 140.2, "min": 12, "max": 911, "p50": 118.4, "p95": 402.1, "p99": 640.0 },
      "status_classes": { "2xx": 47, "4xx": 4, "5xx": 1 }, "error_rate": 0.0962,
      "rate_limit": { "limit": 5000, "window_seconds": 3600, "current": 230, "utilization": 0.046,
                      "status": "OK", "peak": 230, "exceeded_since": null, "last_exceeded_at": null } } ],
  "rate_limit_violations": [ { "client_id": "client-0000", "current": 1412, "limit": 1000, "since": "…" } ],
  "timeseries": { "bucket_seconds": 60, "requests": [ ["2025-04-04T10:00:00Z", 812], … ] },
  "processing": { "files_tracked": 12, "records_parsed": 1209, "records_rejected": 0, "lag_ms": 640 }
}
```

- `requests` is the all-time count. `window_requests` / `rate_limit.current` count the client's current
  rate-limit window across all sources.
- `clients` lists the busiest `max_clients_in_output` clients (default 1000) and every client over its
  limit. `totals.clients` is always the full count.
- `lag_ms` is the time from the last write of a processed file to the insights write (target < 5 s).

## Configuration

All processor settings are in [config/settings.yaml](config/settings.yaml): directories, tick and scan
intervals, worker count, the output client cap, the inactive-source TTL, and ignore patterns. The defaults
match the problem's 5 s requirement with a wide margin.

**Timezone.** Everything is **stored in UTC**: `insights.json` (`…Z` timestamps), checkpoints and
archive folder names. `timezone: Asia/Kolkata` (any IANA name, or `UTC`) is **for display only**:

- The dashboard converts every time to that zone, using `display_timezone` from the insights files, so it
  needs no setting of its own.
- Processor log lines are printed in that zone, with the offset (`+0530`).

An invalid name stops the processor at startup with a clear error. Restart the processor after changing
it. The setting does not change how input logs are read: timestamps without an offset are interpreted
with the per-field `tz` in `sources.yaml` (default UTC).

## Development

The code lives in `csp_traffic/`; [docs/DESIGN.md](docs/DESIGN.md) explains how it works.

```bash
make test                                                     # all tests, in Docker
docker compose --profile test run --rm test pytest -k tailer  # a subset
docker compose --profile test run --rm test python -m csp_traffic validate-config
```
