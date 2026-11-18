# Design

## Goals

- Turn API request logs, arriving as files in per-source folders, into per-client and per-source usage
  insights. Each insight must be on disk within 5 s of the log line being written.
- Accept new sources and new formats without code changes.
- Handle millions of requests per hour on one machine, and survive restarts without double counting.

## Pipeline

```
           ┌──────────── main process ────────────────────────────────────────────┐
input/ ──► │ DirWatcher ── dirty files ──► Engine ──► FileTask ─┐                  │
 inotify + │ (watchdog + 2 s rescan)        ▲                   │                  │
 rescan    │                                │ FileResult         ▼                  │
           │   merge SourceStats ◄──────────┴──── worker pool: read → parse →       │
           │   RateLimiter.ingest                  map → aggregate (per file)       │
           │   every 1 s: write dirty insights, tick limiter                       │
           │   every 5 s: checkpoint                                                │
           └────────────────────────────────────────────────────────────────────────┘
```

| Module | Role |
|---|---|
| `watcher.py` | Watchdog (inotify) events give low latency. A full `scandir` every `reconcile_seconds` is the safety net and the source of truth for which files and sources exist. |
| `tailer.py` | Runs **in workers**. It reads a file from its `FileState`, parses it, maps the records and aggregates them into a `SourceStats` partial. It returns the partial and the new `FileState`, never raw records. |
| `formats/` | The parser registry: `json` (documents), `ndjson`, `csv` (any delimiter), plus plugins. |
| `mapping.py` | `FieldMapper` compiles a `{internal_field: FieldSpec}` mapping into getters and converters. `resolve_mapping` fills unmapped fields from alias lists. |
| `metrics.py` | Mergeable aggregates: per-client counts, latency sum/min/max and log histogram, status classes; per-source endpoint counter and per-minute series. |
| `ratelimit.py` | Per-client sliding window on event time. |
| `engine.py` | Orchestration, source lifecycle, outputs, checkpoints. |
| `output.py` | Insights document builder and atomic writer. |
| `dashboard/` | FastAPI app that only reads `output/`. |

### Why map-reduce partials

Workers return compact aggregates. A partial holds one entry per client and endpoint seen in the chunk,
plus per-client per-second counts for the rate limiter, not one entry per record. Pickling cost between
processes therefore scales with the number of distinct clients, not with the number of requests. Merging
partials is associative, so the result doesn't depend on how files are split across workers. A test checks
that merged and single-pass statistics are identical.

## Reading files incrementally

Each file has a `FileState`: inode, size, mtime, byte `offset`, the first 1 KiB (`head`), the 32 bytes
before the offset (`tail`), `records_consumed`, parser and options, parser state (e.g. the CSV header), and
the resolved mapping.

- **Line formats (CSV, NDJSON, plugins in stream mode).** The worker reads from `offset` in
  `chunk_bytes` chunks and consumes complete lines only. A trailing partial line waits for the rest of it.
  If the file stays idle for `idle_flush_seconds`, the partial line is parsed as final. That handles writers
  that never end the file with a newline.
- **JSON documents.** Two cases:
  - If the bytes before `offset` are unchanged (`tail` matches), new documents were appended, and only the
    appended bytes are parsed.
  - Otherwise the document was rewritten, typically a JSON array that grew. The file is re-parsed and the
    first `records_consumed` records are skipped. A half-written rewrite has fewer complete records than
    already counted, so it waits.
- **Rotation and replacement.** On every read, the first bytes of the file are compared with the stored
  `head`: the consumed part for line formats, the first half for documents, whose closing brackets move.
  A mismatch, or a line-format file that shrank, means a new file at the same path, which is read from
  zero. Inode numbers are reused on Linux, so they're not trusted on their own.
- **Large files.** One task reads at most `max_bytes_per_task` (64 MiB) per file, so one huge file can't
  hold up the others. The rest is read in the next task.

## Format and mapping extensibility

There are three levels, and none of them needs a code change to the core:

1. **Config entry.** `parser`, `options` and `fields` in `sources.yaml`, hot-reloaded. A changed entry
   changes its fingerprint, and the stored mappings of that source's files are re-resolved for new data.
2. **Auto-detection.** Unconfigured folders get a parser by extension, then by content sniffing, in
   priority order: ndjson, json, plugins, csv. Fields come from alias matching on full dotted paths, then
   on the last path segment. A mapping is stored only if it covers the required fields (`timestamp`,
   `client_id`); otherwise detection is retried on the next chunk.
3. **Plugins.** A `Parser` subclass registered with `@register`, dropped into `plugins/` or exposed through
   the `csp_traffic.parsers` entry-point group. It declares `mode = "stream"` (implement `parse_stream`) or
   `mode = "document"` (implement `parse_documents`), plus optional `extensions` and `sniff`.

Mapping rules:

- A missing or invalid `timestamp` or `client_id` rejects the record.
- Optional fields are lenient: an unparseable latency becomes `null`, and the request still counts.
- Records more than `max_future_seconds` ahead of the wall clock are rejected. A bogus future timestamp
  would otherwise move the watermark and empty every rate-limit window.

## Metrics

- **Average latency** is exact (sum / count).
- **Percentiles** (p50, p95, p99) come from a log-bucket histogram with growth factor 1.1: about 5 %
  relative error, around 150 buckets at most, and mergeable.
- **Error rate** is the share of requests with status ≥ 400, among those that have a status.
- **Top endpoints** are an exact `Counter`, reduced with `most_common(5)` when output is written.
- **Requests per minute** is kept for the last `timeseries_minutes` before the watermark.

## Rate limiting

- **Limits.** Each client has one limit and window from `limits.json`, with client values overriding the
  default field by field.
- **Ring buffer.** The window is split into 60 buckets (bucket = window / 60: 60 s buckets for 1 h, 1 s
  buckets for 1 min). They are held in an `array('I')` ring with the index of the newest bucket.
- **Ingest.** Per-second counts from a partial are applied in time order. Buckets that slide out of the
  window are zeroed as the ring advances. The window total is the sum of the 60 buckets, and it's
  re-evaluated each time a bucket boundary is crossed, so a spike that starts and ends inside one batch is
  still caught (`peak`, `last_exceeded_at`).
- **Tick.** Every second, active clients' rings are advanced to the **watermark** (the newest event
  timestamp across all sources) and re-evaluated, so idle clients recover. Clients with an empty window
  leave the active set, so per-tick cost follows active clients rather than all clients ever seen.
- **Precision.** The trailing edge of the window is exact to one bucket.
- **Reloads.** A reload that changes a client's window size restarts that client's window. A reload that
  only changes the limit re-evaluates it immediately.

## Output and checkpoints

- **Insights writes.** Insights are rebuilt only for sources whose data or clients' limit status changed.
  They are written to a temp file, then `os.replace`d, so readers never see a partial file.
- **Checkpoint.** `state/checkpoint.pkl` holds file states, aggregates and limiter state together as one
  snapshot, taken in the main process between merges. It is written atomically every 5 s and on
  SIGTERM/SIGINT. In-flight tasks are in neither the offsets nor the aggregates, so after a crash they are
  simply re-read: at-least-once reading, exactly-once counting.
- **Source lifecycle.** When a folder disappears, the source is marked `inactive` and its numbers are kept.
  After `inactive_ttl_seconds`, its output folder moves to `output/_archive/`. If the folder reappears, the
  source becomes active again with its history intact.

## Performance

Measured with `make bench` (Docker, 1M records spread over the five formats):

| Setup | Throughput |
|---|---|
| 1 worker (inline) | ~280k records/s (~1 billion/hour) |
| 4 worker processes | ~720k records/s |
| Live: 20k records/s appended | lag (file write → insights) median ~0.7 s, p99 ~1.2 s |

Throughput comes mostly from the following:

- orjson and the C `csv` module;
- precompiled field getters;
- an LRU cache on timestamp strings (log lines share timestamps heavily);
- aggregating in the worker.

## Limits and next steps

- **Memory** grows with the number of distinct clients (a few hundred bytes per client per source, plus
  the limiter ring). For tens of millions of clients:
  - shard clients across processor instances by hash;
  - or move the per-client state to Redis, keeping the same ring-buffer model with `INCRBY` on bucket keys.
- **Endpoints with IDs** in the path (`/users/123`) inflate the endpoint counter. A normalisation rule
  (regex → template) in `sources.yaml` would be the next config feature.
- **CSV fields with embedded newlines** aren't supported; log lines are assumed single-line.
- **Scaling out:** sources are independent apart from the global per-client limit, so several processors
  can split the source folders between them. The only shared piece would then be the rate-limit counters,
  which would move to the shared store above. Replacing the input directory with Kafka would change only
  `watcher.py` and `tailer.py`: partitions and offsets instead of files and byte offsets.
