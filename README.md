# motel

In-memory OpenTelemetry server with querying and TUI visualization.

motel receives traces, logs, and metrics via standard OTLP gRPC/HTTP, stores them in memory with FIFO eviction, and provides a SQL query engine plus an interactive terminal UI.

## Install

```bash
cargo install --path .
```

## Quick Start

```bash
# Start server with TUI
motel server

# Or headless
motel server --no-tui
```

Point your app's OTLP exporter at motel:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
```

Default ports:
- **4317** — gRPC OTLP ingestion
- **4318** — HTTP OTLP ingestion
- **4319** — Query service

## Query

```bash
# CLI queries
motel traces --service myapp --since 5m
motel logs --severity ERROR
motel metrics --name http.request.duration

# SQL (DataFusion)
motel sql "SELECT span_name, COUNT(*) as n, AVG(duration_ns)/1e6 as avg_ms
           FROM traces GROUP BY span_name ORDER BY avg_ms DESC"

motel sql "SELECT * FROM logs WHERE body LIKE '%error%' LIMIT 20"
```

Three tables available: `traces`, `logs`, `metrics`.

## TUI

```bash
# Embedded with server
motel server

# Attach to running server
motel view
motel view --addr http://remote-host:4319
```

| Key | Action |
|-----|--------|
| Tab / 1-3 | Switch tabs |
| j/k / arrows | Navigate |
| Enter | Detail view |
| PgUp/PgDn | Page |
| q / Esc | Quit |

## Output Formats

All query commands support `--output` (`-o`): `text`, `table`, `jsonl`, `csv`.

```bash
motel traces -o jsonl | jq .
motel sql "SELECT * FROM traces" -o csv > export.csv
```

## Self-Instrumentation

motel can report its own traces to itself:

```bash
motel server --no-tui --otlp-endpoint http://localhost:4317

# Then query its own performance
motel sql "SELECT span_name, AVG(duration_ns)/1e6 as avg_ms
           FROM traces WHERE service_name = 'motel'
           GROUP BY span_name ORDER BY avg_ms DESC"
```

## Other Commands

```bash
motel status                  # Trace/log/metric counts
motel clear                   # Clear all data
motel clear traces            # Clear only traces
motel shutdown                # Remote shutdown
motel skill-install           # Install Claude Code skill
```

## Architecture

```
OTLP gRPC/HTTP → Store (in-memory, FIFO eviction) → Query API (gRPC) / TUI
                                                    → DataFusion SQL engine
```

Built with: [tonic](https://github.com/hyperium/tonic), [axum](https://github.com/tokio-rs/axum), [DataFusion](https://github.com/apache/datafusion), [ratatui](https://github.com/ratatui/ratatui)
