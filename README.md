# motel — Memory OTel

In-memory OpenTelemetry server with querying and TUI visualization.

motel receives traces, logs, and metrics via standard OTLP gRPC/HTTP, stores them in memory with FIFO eviction, and provides a SQL query engine plus an interactive terminal UI.

## Install

```bash
# Pre-built binary (Linux x86_64 shown; see Releases for all platforms)
curl -fsSL https://github.com/jasonwmiller/motel/releases/latest/download/motel-x86_64-unknown-linux-gnu.tar.gz | tar xz
sudo mv motel /usr/local/bin/

# Homebrew
brew install jasonwmiller/tap/motel

# Cargo
cargo install --git https://github.com/jasonwmiller/motel
```

Pre-built binaries are available for Linux (x86_64, aarch64), macOS (x86_64, aarch64), and Windows (x86_64) on the [Releases](https://github.com/jasonwmiller/motel/releases) page.

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

Or generate config for your project:

```bash
motel init                              # Print .env with OTEL env vars
motel init -o .env                      # Write .env file to disk
motel init --lang node                  # Node.js OTLP setup snippet
motel init --lang python                # Python OTLP setup snippet
motel init --lang rust                  # Rust OTLP setup snippet
motel init --lang go                    # Go OTLP setup snippet
motel init --lang java                  # Java agent setup snippet
motel init --endpoint http://collector:4317 --service-name myapp
```

Default ports:
- **4317** — gRPC OTLP ingestion
- **4318** — HTTP OTLP ingestion
- **4319** — Query service

## Persistence

By default motel stores everything in memory. Use `--persist` to write data to disk so it survives server restarts:

```bash
# SQLite (default format) — single file, simple
motel server --persist /tmp/motel.db

# Parquet — directory of .parquet files
motel server --persist ./motel-data/ --persist-format parquet
```

The in-memory store remains the primary data path; persistence is write-through. On startup, persisted data is reloaded automatically. FIFO eviction in memory does not affect persisted data (disk retains full history). `motel clear` clears both in-memory and persisted data.

## Query

```bash
# CLI queries
motel traces --service myapp --since 5m
motel logs --severity ERROR
motel metrics --name http.request.duration

# Latency histogram
motel latency GET /api/users --service myapp --since 1h
motel latency db.query --buckets 10 -o csv

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

# Attach to running server (loads existing data, then follows new)
motel view
motel view --addr http://remote-host:4319

# View multiple servers merged into one TUI
motel view --addr http://host1:4319 --addr http://host2:4319
```

Three tabs: **Logs**, **Traces**, **Metrics** — each with a master-detail layout.

| Key | Action |
|-----|--------|
| 1 / 2 / 3 | Switch to Logs / Traces / Metrics |
| Tab | Cycle tabs |
| j/k | Navigate list |
| Enter | Open trace timeline (Traces tab) |
| Esc | Back from timeline / quit |
| PgUp/PgDn | Scroll detail pane |
| f | Toggle follow mode (auto-scroll to newest) |
| g | Toggle metric graph view (Metrics tab, 5+ data points) |
| q | Quit |

**Traces tab** has two views: a trace list grouped by trace ID, and a timeline waterfall view showing the span tree with colored timing bars. Press Enter on a trace to drill into the timeline.

**Metrics tab** shows aggregated metrics with summed values. Press `g` to toggle a bar chart graph of values over time.

## Live Tail Mode

Stream new data as it arrives, like `tail -f`:

```bash
motel logs --follow
motel logs --follow --service myapp --severity ERROR
motel traces --follow --service myapp
motel metrics --follow --name http.request.duration
```

Combine with output formats for piping:

```bash
motel logs --follow -o jsonl | jq 'select(.severity == "ERROR")'
motel traces --follow -o csv >> traces.csv
```

Short flag: `-F` (e.g. `motel logs -F`). Runs until interrupted with Ctrl+C.

## Output Formats

All query commands support `--output` (`-o`): `text`, `table`, `jsonl`, `csv`.

```bash
motel traces -o jsonl | jq .
motel sql "SELECT * FROM traces" -o csv > export.csv
```

## Claude Code Telemetry

Use motel to collect and visualize Claude Code's OpenTelemetry data (metrics, logs/events):

```bash
# Terminal 1: Start motel headless
motel server --no-tui

# Terminal 2: Run Claude Code with telemetry pointed at motel
export CLAUDE_CODE_ENABLE_TELEMETRY=1
export OTEL_METRICS_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
claude

# Terminal 3: View telemetry in TUI
motel view
```

Claude Code exports these metrics (service name: `claude-code`):

| Metric | Description |
|--------|-------------|
| `claude_code.token.usage` | Tokens used (attributes: `type`=input/output/cacheRead, `model`) |
| `claude_code.cost.usage` | Session cost in USD (attribute: `model`) |
| `claude_code.session.count` | Sessions started |
| `claude_code.lines_of_code.count` | Lines added/removed (attribute: `type`) |
| `claude_code.commit.count` | Git commits created |
| `claude_code.pull_request.count` | PRs created |
| `claude_code.active_time.total` | Active time in seconds |
| `claude_code.code_edit_tool.decision` | Tool permission decisions |

And these log events: `claude_code.user_prompt`, `claude_code.tool_result`, `claude_code.api_request`, `claude_code.api_error`, `claude_code.tool_decision`.

Query examples:

```bash
# Token usage by model
motel sql "SELECT resource['model'] as model, SUM(CAST(value AS DOUBLE)) as tokens
           FROM metrics WHERE metric_name = 'claude_code.token.usage'
           GROUP BY model"

# Cost tracking
motel sql "SELECT * FROM metrics WHERE metric_name = 'claude_code.cost.usage'"

# Tool usage from events
motel sql "SELECT body FROM logs WHERE body LIKE '%tool_result%' LIMIT 20"

# API request durations
motel sql "SELECT body FROM logs WHERE body LIKE '%api_request%' ORDER BY timestamp DESC LIMIT 10"
```

Optional env vars for more detail:
- `OTEL_LOG_USER_PROMPTS=1` — include prompt content in events
- `OTEL_LOG_TOOL_DETAILS=1` — include MCP server/tool names
- `OTEL_METRIC_EXPORT_INTERVAL=10000` — faster metric export (default 60s)
- `OTEL_LOGS_EXPORT_INTERVAL=5000` — log export interval (default 5s)

## OpenAI Codex CLI Telemetry

motel also works with [Codex CLI](https://github.com/openai/codex). Configure via `~/.codex/config.toml`:

```toml
[otel]
exporter = { otlp-grpc = {
  endpoint = "http://localhost:4317"
}}
```

Or for HTTP:

```toml
[otel]
exporter = { otlp-http = {
  endpoint = "http://localhost:4318/v1/logs",
  protocol = "binary"
}}
```

Codex emits structured log events:

| Event | Description |
|-------|-------------|
| `codex.conversation_starts` | Model and policy configuration |
| `codex.api_request` | Status, duration, errors |
| `codex.sse_event` | Stream processing with token counts |
| `codex.user_prompt` | Prompt length (content redacted by default) |
| `codex.tool_decision` | Tool permission decisions |
| `codex.tool_result` | Tool execution results |

Set `log_user_prompt = true` in `[otel]` to include prompt content.

```bash
# View Codex events
motel view  # switch to Logs tab

# Query Codex events
motel sql "SELECT * FROM logs WHERE body LIKE '%codex%' ORDER BY timestamp DESC LIMIT 20"
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

## Service Map

Visualize service dependencies extracted from trace parent-child relationships:

```bash
# ASCII output (default)
motel service-map

# Mermaid diagram format
motel service-map --format mermaid

# Filter to recent traces
motel service-map --since 5m
```

Example ASCII output:
```
Service Dependency Map
========  frontend --(120 calls, avg 45.2ms)--> api-gateway
  api-gateway --(85 calls, avg 12.3ms)--> user-service
  api-gateway --(42 calls, avg 8.7ms)--> payment-service
  payment-service --(42 calls, avg 3.1ms)--> database

Services: api-gateway, database, frontend, payment-service, user-service
```

Example Mermaid output:
```
graph LR
    frontend["frontend"] -->|120 calls, 45.2ms avg| api_gateway["api-gateway"]
    api_gateway["api-gateway"] -->|85 calls, 12.3ms avg| user_service["user-service"]
## Replay

Replay stored data from one motel server to another OTLP endpoint:

```bash
# Replay all data from local motel to a remote collector
motel replay --target http://collector.example.com:4317

# Replay only traces from the last hour
motel replay --target http://other:4317 --signal traces --since 1h

# Replay only data from a specific service
motel replay --target http://other:4317 --service myapp

# Dry run to see what would be replayed
motel replay --target http://other:4317 --dry-run
```
## Import

Load telemetry data from files into a running motel server. Supports JSONL (motel's own export format) and OTLP protobuf binary.

```bash
# Import JSONL traces (from motel's own export)
motel import traces.jsonl --signal traces

# Import OTLP protobuf binary
motel import traces.pb --format otlp-proto --signal traces

# Import with auto-detection (filename and extension based)
motel import traces.jsonl

# Import multiple files
motel import traces-1.jsonl traces-2.jsonl

# Export then re-import workflow
motel traces --output jsonl > traces.jsonl
motel import traces.jsonl --signal traces

# Import to a non-default server
motel import data.jsonl --signal logs --addr http://localhost:4317
```

Format is auto-detected from file extension (`.jsonl`/`.json`/`.ndjson` for JSONL, `.pb`/`.proto`/`.bin` for protobuf). Signal type is auto-detected from filename (e.g., `traces.jsonl` -> traces, `logs.jsonl` -> logs). Use `--format` and `--signal` to override.
## Export

Bulk dump stored data for offline analysis or backup:

```bash
motel export traces -o jsonl > traces.jsonl
motel export logs -o csv > logs.csv
motel export metrics -o text
motel export all -o jsonl > everything.jsonl
motel export traces -o proto > traces.binpb   # binary protobuf (length-delimited)
```

Supported formats: `text`, `jsonl`, `csv`, `proto`. Default is `jsonl`.

## Other Commands

```bash
motel status                  # Trace/log/metric counts
motel clear                   # Clear all data
motel clear traces            # Clear only traces
motel shutdown                # Remote shutdown
motel init                    # Generate OTLP config (.env or language-specific)
motel skill-install           # Install Claude Code skill
```

## Architecture

```
OTLP gRPC/HTTP → Store (in-memory, FIFO eviction) → Query API (gRPC) / TUI
                                                    → DataFusion SQL engine
```

Built with: [tonic](https://github.com/hyperium/tonic), [axum](https://github.com/tokio-rs/axum), [DataFusion](https://github.com/apache/datafusion), [ratatui](https://github.com/ratatui/ratatui)
