# motel — Memory OTel

motel is a CLI tool that runs an in-memory OpenTelemetry (OTLP) server with querying and TUI visualization. It receives traces, logs, and metrics via standard OTLP gRPC/HTTP, stores them in memory, and provides SQL querying plus an interactive terminal UI.

## Install

```bash
brew install jasonwmiller/tap/motel
# or: cargo install --git https://github.com/jasonwmiller/motel
```

## Starting the Server

```bash
# Start with interactive TUI
motel server

# Start headless (no TUI)
motel server --no-tui

# Custom addresses
motel server --grpc-addr 0.0.0.0:4317 --http-addr 0.0.0.0:4318 --query-addr 0.0.0.0:4319
```

Default ports:
- **4317** - gRPC OTLP ingestion
- **4318** - HTTP OTLP ingestion
- **4319** - Query service (gRPC)

## Configuring Your App to Send Telemetry

Point your application's OTLP exporter at motel:
- gRPC: `http://localhost:4317`
- HTTP: `http://localhost:4318`

Standard OpenTelemetry SDKs work with no special configuration. Set `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317` in your app's environment.

## Querying Data

### Traces

```bash
motel traces                              # All traces
motel traces --service myapp              # Filter by service
motel traces --span-name "HTTP GET"       # Filter by span name
motel traces --trace-id abc123...         # Lookup specific trace
motel traces --since 5m                   # Last 5 minutes
motel traces --attribute "http.method=GET" # Filter by attribute
motel traces --limit 50                   # Limit results
```

### Logs

```bash
motel logs                                # All logs
motel logs --service myapp                # Filter by service
motel logs --severity ERROR               # Filter by severity
motel logs --body "connection failed"     # Filter by body content
motel logs --since 1h                     # Last hour
```

### Metrics

```bash
motel metrics                             # All metrics
motel metrics --service myapp             # Filter by service
motel metrics --name http.request.duration # Filter by metric name
```

### SQL Queries

motel includes a SQL engine (DataFusion) with three tables: `traces`, `logs`, `metrics`.

```bash
motel sql "SELECT * FROM traces LIMIT 10"
motel sql "SELECT * FROM logs WHERE severity = 'ERROR'"
motel sql "SELECT * FROM metrics WHERE metric_name = 'http.request.duration'"
```

Access attributes and resource labels with bracket syntax:

```bash
motel sql "SELECT span_name, attributes['http.method'] FROM traces"
motel sql "SELECT * FROM traces WHERE resource['service.name'] = 'myapp'"
```

### Useful Debugging Queries

```bash
# Slowest spans
motel sql "SELECT service_name, span_name, duration_ns/1e6 as ms FROM traces ORDER BY duration_ns DESC LIMIT 20"

# Error rate by service
motel sql "SELECT service_name, COUNT(*) as total, SUM(CASE WHEN status_code = 'Error' THEN 1 ELSE 0 END) as errors FROM traces GROUP BY service_name"

# Log volume by severity
motel sql "SELECT severity, COUNT(*) as n FROM logs GROUP BY severity ORDER BY n DESC"

# Per-operation latency stats
motel sql "SELECT span_name, COUNT(*) as n, AVG(duration_ns)/1e6 as avg_ms, MAX(duration_ns)/1e6 as max_ms FROM traces GROUP BY span_name ORDER BY avg_ms DESC"

# Trace waterfall (parent-child relationships)
motel sql "SELECT t1.span_name as parent, t2.span_name as child, t2.duration_ns/1e6 as child_ms FROM traces t1 JOIN traces t2 ON t1.span_id = t2.parent_span_id ORDER BY t2.duration_ns DESC LIMIT 20"
```

## Output Formats

All query commands support `--output` (`-o`):

```bash
motel traces --output text    # Human-readable (default for traces/logs/metrics)
motel traces --output table   # Tabular (default for sql)
motel traces --output jsonl   # JSON Lines (one JSON object per line)
motel traces --output csv     # CSV
```

## Replay

Replay stored data from a motel server to another OTLP endpoint:

```bash
motel replay --target http://other:4317              # Replay all data
motel replay --target http://other:4317 --signal traces --since 1h  # Traces from last hour
motel replay --target http://other:4317 --service myapp  # Filter by service
motel replay --target http://other:4317 --dry-run    # Preview without sending
```

## Other Commands

```bash
motel view                    # Attach TUI to a running server
motel status                  # Show trace/log/metric counts
motel clear                   # Clear all stored data
motel clear traces            # Clear only traces
motel shutdown                # Remotely shutdown a running server
motel init                    # Generate .env with OTEL env vars (stdout)
motel init -o .env            # Write .env file to disk
motel init --lang node        # Node.js OTLP setup snippet
motel init --lang python      # Python OTLP setup snippet
motel init --lang rust        # Rust OTLP setup snippet
motel init --lang go          # Go OTLP setup snippet
motel init --lang java        # Java agent setup snippet
motel init --endpoint http://collector:4317 --service-name myapp
```

## Time Filters

The `--since` and `--until` flags accept:
- Relative: `30s`, `5m`, `1h`, `2d` (seconds, minutes, hours, days ago)
- Absolute: RFC3339 format like `2024-01-15T10:30:00Z`

## Self-Instrumentation

motel can report its own traces to itself or another instance:

```bash
motel server --no-tui --otlp-endpoint http://localhost:4317
```

Then query motel's own performance with `service_name = 'motel'`.
