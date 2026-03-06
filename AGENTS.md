# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

motel (Memory OTel) is a Rust CLI tool that acts as an in-memory OpenTelemetry (OTLP) server with querying and TUI visualization. It receives traces, logs, and metrics via standard OTLP gRPC/HTTP, stores them in memory with FIFO eviction, and provides a custom gRPC query API (including a DataFusion-based SQL engine) plus an interactive terminal UI.

## Build & Development Commands

```bash
cargo build                          # Build
cargo test                           # Run all tests
cargo test <test_name>               # Run a single test by name
cargo test --test e2e_client         # Run a single integration test file
cargo clippy                         # Lint
cargo fmt                            # Format
cargo run -- server                  # Start OTLP server with TUI (gRPC:4317, HTTP:4318, Query:4319)
cargo run -- server --no-tui         # Start headless server
cargo run -- server --persist /tmp/motel.db  # Start with SQLite persistence
cargo run -- server --persist ./data/ --persist-format parquet  # Parquet persistence
cargo run -- view                    # Attach TUI to a running server (default: localhost:4319)
cargo run -- view --addr http://h1:4319 --addr http://h2:4319  # Multi-server aggregated view
cargo run -- traces                  # Query traces
cargo run -- traces --follow         # Stream new traces (tail -f style)
cargo run -- logs                    # Query logs
cargo run -- logs --follow           # Stream new logs (tail -f style)
cargo run -- metrics                 # Query metrics
cargo run -- metrics --follow        # Stream new metrics (tail -f style)
cargo run -- sql "SELECT * FROM traces"  # Run SQL query
cargo run -- service-map             # Show service dependency graph (ASCII)
cargo run -- service-map --format mermaid  # Show service dependency graph (Mermaid)
cargo run -- latency <span_name>         # Show latency histogram for a span
cargo run -- status                  # Check server status (trace/log/metric counts)
cargo run -- shutdown                # Remotely shutdown a running server
cargo run -- init                    # Generate .env with OTEL env vars
cargo run -- init --lang node        # Node.js OTLP setup snippet
cargo run -- init --lang python      # Python OTLP setup snippet
cargo run -- init --lang rust        # Rust OTLP setup snippet
cargo run -- init --lang go          # Go OTLP setup snippet
cargo run -- init --lang java        # Java agent setup snippet
cargo run -- import traces.jsonl     # Import data from files (JSONL or OTLP protobuf)
cargo run -- skill-install           # Install Claude Code skill for current project
cargo run -- skill-install --global  # Install skill globally
cargo run -- mcp                     # Start MCP server (stdio) for AI tool integration
cargo run -- mcp --addr http://localhost:4319  # MCP server with custom query address
cargo run -- config init             # Generate default config file (~/.config/motel/config.toml)
cargo run -- config path             # Print config file path
cargo run -- config show             # Print resolved config as TOML
```

## Architecture

**Core data flow**: OTLP ingestion (gRPC/HTTP) → `Store` (in-memory, `Arc<RwLock>`) → Query API (gRPC) / TUI (broadcast channels)

- **`src/store.rs`** — Central in-memory store (`SharedStore = Arc<RwLock<Store>>`). All signal types use `VecDeque` with FIFO eviction. Traces are evicted by `trace_id` when `max_traces` is exceeded. Supports optional write-through persistence via `SharedPersistBackend`.
- **`src/persist/`** — Optional persistence backends:
  - `mod.rs` — `PersistBackend` trait and `SharedPersistBackend` type alias.
  - `sqlite.rs` — SQLite backend: stores each `Resource*` item as a protobuf-encoded BLOB. Uses WAL mode and `synchronous=NORMAL` for performance.
  - `parquet.rs` — Parquet backend: stores protobuf-encoded BLOBs in Parquet files (one per signal type). Buffers writes in memory and flushes to disk on each insert.
- **`src/server/`** — Three listeners: `otlp_grpc.rs` (standard OTLP TraceService/LogsService/MetricsService), `otlp_http.rs` (Axum `/v1/traces`, `/v1/logs`, `/v1/metrics`), `query_grpc.rs` (custom QueryService with streaming follow support and SQL query execution).
- **`src/client/`** — CLI query commands. Each submodule (trace, log, metrics, sql, latency, clear) builds gRPC requests and formats output (Text/JSONL/CSV). `view.rs` supports connecting to multiple servers simultaneously (`--addr` can be specified multiple times), queries all existing data on connect, then subscribes to Follow streams for new data, piping both into a local Store to drive the TUI. Each item is tagged with a `motel.source` resource attribute identifying its origin server. `import.rs` reads files (JSONL or OTLP protobuf) and sends data to a server via standard OTLP gRPC clients. `mod.rs` contains shared utilities.
- **`src/query/`** — SQL query engine built on DataFusion:
  - `datafusion_ctx.rs` — Creates a `SessionContext` with three registered tables (`traces`, `logs`, `metrics`). One context is created per server lifetime and reused across queries.
  - `table_provider.rs` — `OtelTable` implements DataFusion's `TableProvider`. On each `scan()`, it acquires a read lock on the store, converts data to Arrow `RecordBatch` via `arrow_convert`, then releases the lock before query execution.
  - `arrow_convert.rs` — Converts protobuf store data (ResourceSpans/ResourceLogs/ResourceMetrics) to Arrow RecordBatch format. This is called on every query and is O(total items).
  - `arrow_schema.rs` — Arrow schema definitions for traces (13 columns), logs (9 columns), and metrics (9 columns).
  - `sql/mod.rs` — `execute()` runs SQL via DataFusion and converts Arrow RecordBatches to protobuf `Row` responses.
  - `sql/convert.rs` — Converts CLI flags (`--service`, `--attribute`, etc.) into SQL query strings.
- **`src/tui/`** — ratatui-based interactive UI with three tabs (1:Logs, 2:Traces, 3:Metrics):
  - `app.rs` — Data model: `App` state, `TraceGroup` (spans grouped by trace_id), `SpanTreeNode` (depth-first tree for waterfall), `AggregatedMetric` (grouped by name+service), `LogRow`. Includes `build_span_tree()`, `group_traces()`, `aggregate_metrics()`, follow mode, service color palette (One Dark-inspired RGB), metric graph mode toggle.
  - `ui.rs` — Rendering: master-detail 60/40 side panels for Logs and Metrics tabs, trace list + timeline waterfall view with service-colored timing bars and `├─` tree indentation, metric bar chart graph (Unicode block chars `▁▂▃▄▅▆▇█`), `▶` selection marker. `draw()` takes `&mut App` for service color computation (pre-populated via `ensure_service_colors()`).
  - `event.rs` — Key handling: `f` toggles follow mode, `g` toggles metric graph, `Enter` opens trace timeline, `Esc` goes back from timeline, PgUp/PgDn scrolls detail pane, tab numbers 1/2/3.
  - `mod.rs` — Main event loop, terminal setup/teardown.
  - Uses broadcast channel events for real-time updates with dirty tracking for efficient refresh.
- **`src/mcp.rs`** — MCP (Model Context Protocol) server over stdio. Connects to the query gRPC service as a client and exposes 5 tools (`query_traces`, `query_logs`, `query_metrics`, `run_sql`, `get_status`) for AI assistants. Uses `rmcp` crate with `#[tool]` macros and `ServerHandler` trait.
- **`src/install.rs`** — `skill-install` subcommand logic. Embeds `skills/motel/SKILL.md` via `include_str!`.
- **`src/client/init.rs`** — `init` subcommand: generates OTLP config files (.env or language-specific snippets for Node, Python, Rust, Go, Java). Local-only, no server connection.
- **`src/client/service_map.rs`** — `service-map` subcommand: generates service dependency graph from trace data via SQL self-join.
- **`src/cli.rs`** — clap derive command definitions (Server, View, Traces, Logs, Metrics, Sql, ServiceMap, Export, Latency, Clear, Status, Shutdown, Replay, Import, SkillInstall, Init, Mcp, Config). Output formats: `Text`, `Table`, `Jsonl`, `Csv`. Import formats: `Jsonl`, `OtlpProto`. Signal types: `Traces`, `Logs`, `Metrics`. Each args struct has a `resolve()` method that merges with config.
- **`src/config.rs`** — TOML config file support (`~/.config/motel/config.toml` or `$XDG_CONFIG_HOME/motel/config.toml`). Defines `Config`, `ServerConfig`, `TuiConfig`, `DefaultsConfig` structs. Loaded at startup in `main.rs`; CLI args are resolved against config with precedence: CLI flag > config file > hardcoded default. Includes `config init/path/show` subcommand logic.
- **`proto/query.proto`** — Custom query/follow/clear/status/shutdown/SQL gRPC API. Standard OTLP protos are vendored in `proto/opentelemetry-proto/` (originally from OpenTelemetry v1.9.0, Apache 2.0 licensed).
- **`build.rs`** — Compiles protobuf files via `tonic_prost_build`.

## Installation

```bash
# Homebrew
brew install jasonwmiller/tap/motel

# Cargo (from source)
cargo install --git https://github.com/jasonwmiller/motel
```

Proto files are vendored directly in the repo (not a git submodule), so `cargo install --git` works without any extra steps. Homebrew formula is in `jasonwmiller/homebrew-tap`. License: MIT.

## TUI Layout Reference

### Logs Tab (master-detail 60/40)

```
┌ OTLP Viewer ─────────────────────────────────────────────────────────────┐
│ 1:Logs(42)  |  2:Traces(18 spans (5 traces))  |  3:Metrics(6)          │
├──────────────────────────────────────────┬────────────────────────────────┤
│ Time          Service    Severity  Body  │ Detail                        │
│ 14:23:01.123  auth-svc   INFO      User  │ Time: 2026-03-06 14:23:01 UTC │
│ 14:23:01.456  api-gw     WARN      Rate  │ Service: auth-svc             │
│▶14:23:02.789  auth-svc   ERROR     Auth  │ Severity: ERROR (17)          │
│ 14:23:03.012  payments   INFO      Proc  │ Body: Auth token expired      │
│ 14:23:03.234  api-gw     DEBUG     Rout  │ Trace ID: a1b2c3d4e5f6...    │
│                                          │ Scope: auth.middleware        │
│                                          │                               │
│                                          │ Attributes                    │
│                                          │   user.id: 12345             │
│                                          │   http.status: 401           │
├──────────────────────────────────────────┴────────────────────────────────┤
│ Tab:switch  j/k:nav  Enter:select  PgUp/Dn:scroll  f:follow  [FOLLOW]  │
└──────────────────────────────────────────────────────────────────────────┘
```

### Traces Tab — List View

```
┌ Traces ──────────────────────────────────────────────────────────────────┐
│ Trace ID    Service      Root Span           Spans  Duration            │
│ a1b2c3d4    api-gw       GET /api/users      5      234.56ms            │
│▶e5f6a7b8    auth-svc     authenticate        3      45.12ms             │
│ c9d0e1f2    payments     process_payment     8      1.234s              │
└──────────────────────────────────────────────────────────────────────────┘
```

### Traces Tab — Timeline Waterfall (Enter on a trace)

```
┌ Timeline (Esc:back) ─────────────────────────────────────────────────────┐
│ Span                              Timeline (234.56ms)                    │
│▶── GET /api/users                 ████████████████████████████████        │
│  ├─ auth.validate                   ████████                             │
│  ├─ db.query                              ██████████████                 │
│  │  ├─ db.connect                         ███                            │
│  │  ├─ db.execute                            ███████████                 │
│  ├─ serialize                                            ████            │
└──────────────────────────────────────────────────────────────────────────┘
```

### Metrics Tab (master-detail 60/40, 'g' toggles graph)

```
┌ Metrics ──────────────────────────┬──────────────────────────────────────┐
│ Metric Name    Service  Type  Val │ Detail                               │
│▶http.dur       api-gw   hist  c=  │ Metric Name: http.request.duration   │
│ http.req       api-gw   sum   42  │ Service: api-gw                      │
│ cpu.usage      motel    gauge 0.  │ Type: histogram                      │
│                                   │ Unit: ms                             │
│                                   │ Data Points: 24                      │
│                                   │   Press 'g' for graph view           │
│                                   │                                      │
│                                   │ Recent Data Points                   │
│                                   │   14:23:03  count=5 sum=234.500      │
│                                   │   14:23:02  count=3 sum=123.200      │
├───────────────────────────────────┴──────────────────────────────────────┤
│ Tab:switch  j/k:nav  PgUp/Dn:scroll  f:follow  g:graph  q:quit [FOLLOW]│
└──────────────────────────────────────────────────────────────────────────┘
```

### Metrics Tab — Graph View ('g' to toggle)

```
┌ http.request.duration (ms) - press 'g' for detail ──────────────────────┐
│234.5                                                                     │
│          █                                                               │
│          █     █                                                         │
│    █     █     █           █                                             │
│    █  █  █     █  █        █  █                                          │
│    █  █  █  █  █  █  █     █  █     █                                    │
│ █  █  █  █  █  █  █  █  █  █  █  █  █                                   │
│ █  █  █  █  █  █  █  █  █  █  █  █  █  █                                │
│45.1                                                                      │
└──────────────────────────────────────────────────────────────────────────┘
```

## Code Patterns

- Error handling: `anyhow::Result<T>` for server/CLI, `Result<_, String>` for SQL query internals.
- Shared state: `Arc<RwLock<Store>>` (`SharedStore`) passed to all server handlers.
- Event notifications: `broadcast::Sender<StoreEvent>` for TUI and follow-stream updates.
- Trace IDs: stored as `Vec<u8>`, displayed as hex strings via `hex_encode()`/`hex_decode()`.
- Timestamps: nanoseconds since epoch internally, formatted as RFC3339 for display.
- Time specs in queries: relative (`30s`, `5m`, `1h`, `2d`) or RFC3339 absolute.
- SQL: DataFusion `SessionContext` is created once and reused. Supports standard SQL including aggregation, subqueries, and joins. Attribute access via bracket syntax: `attributes['key']`, `resource['key']`.
- CLI flag queries are internally converted to SQL via `src/query/sql/convert.rs`.

## Self-Instrumentation

motel can instrument itself using `--otlp-endpoint` (or `OTEL_EXPORTER_OTLP_ENDPOINT` env var). Self-reporting (sending traces to itself) works well for most use cases:

```bash
# Self-reporting: sends own traces to itself
cargo run -- server --no-tui --otlp-endpoint http://localhost:4317

# Two-server setup (avoids feedback loop when needed):
# Terminal 1: cargo run -- server --no-tui --grpc-addr 0.0.0.0:14317 --http-addr 0.0.0.0:14318 --query-addr 0.0.0.0:14319
# Terminal 2: cargo run -- server --no-tui --otlp-endpoint http://localhost:14317
```

The batch exporter flushes every ~5 seconds. Wait ~6 seconds after activity before querying self-instrumentation data.

### Key Spans and Attributes

Performance-critical spans (filter by `service_name = 'motel'`):

| Span | Key Attributes | Notes |
|---|---|---|
| `sql.execute` | `db.statement` (the SQL text) | Arrow RecordBatch is rebuilt from Store on every call (O(total items)) |
| `store.insert_traces` | `count` (batch size) | Sorted insertion into VecDeque + eviction can be costly at scale |
| `store.insert_logs` | `count` | |
| `store.insert_metrics` | `count` | |
| `otlp.grpc.export_traces` | | Parent of `store.insert_traces` |
| `otlp.http.export_traces` | | Parent of `store.insert_traces` |
| `query.sql_query` | | Parent of `sql.execute` |

Other spans: `otlp.{grpc,http}.export_{logs,metrics}`, `query.follow_*`, `query.clear_*`, `query.status`, `query.shutdown`, `store.clear_*`.

All async spans include `busy_ns` and `idle_ns` attributes from tracing's tokio instrumentation.

**Multi-server view**: When `motel view` connects to multiple servers (`--addr` specified multiple times), each item receives a `motel.source` resource attribute containing the server address (e.g., `host1:4319`). This is queryable via SQL: `SELECT * FROM traces WHERE resource['motel.source'] = 'host1:4319'`.

### Retrieving Query Trace IDs

Use `--show-trace-id` to get the trace ID of the query request itself. This lets you inspect the server-side execution trace (e.g., `query.sql_query` → `sql.execute`) for that specific request:

```bash
motel sql "SELECT * FROM traces LIMIT 10" --show-trace-id
# stderr: trace_id: abcdef1234567890...

# Then look up the query's own execution trace
motel sql "SELECT span_name, duration_ns/1e6 as ms FROM traces WHERE trace_id = 'abcdef1234567890...'"
```

### Useful Analysis Queries

```bash
# Per-operation average performance
motel sql "SELECT span_name, COUNT(*) as n, AVG(duration_ns)/1e6 as avg_ms, MAX(duration_ns)/1e6 as max_ms FROM traces WHERE service_name = 'motel' GROUP BY span_name ORDER BY avg_ms DESC"

# SQL query performance with actual statements
motel sql "SELECT attributes['db.statement'] as stmt, duration_ns/1e6 as ms FROM traces WHERE service_name = 'motel' AND span_name = 'sql.execute' ORDER BY duration_ns DESC LIMIT 10"

# Parent-child span relationships (identify where time is spent)
motel sql "SELECT t1.span_name as parent, t2.span_name as child, t2.duration_ns/1e6 as child_ms FROM traces t1 JOIN traces t2 ON t1.span_id = t2.parent_span_id WHERE t1.service_name = 'motel' ORDER BY t2.duration_ns DESC LIMIT 20"

# Store insertion throughput
motel sql "SELECT span_name, attributes['count'] as batch_size, duration_ns/1e6 as ms FROM traces WHERE service_name = 'motel' AND span_name LIKE 'store.insert%' ORDER BY duration_ns DESC LIMIT 10"

# Latency histogram (equivalent to: motel latency sql.execute --service motel)
motel sql "SELECT duration_ns FROM traces WHERE span_name = 'sql.execute' AND service_name = 'motel' ORDER BY duration_ns ASC"
```

## Commit Convention

This project uses [Conventional Commits](https://www.conventionalcommits.org/). All commit messages must follow the format `<type>: <description>` (e.g. `feat: add histogram support`, `fix: correct hex encoding`, `docs: update README`). Common types: `feat`, `fix`, `docs`, `chore`, `refactor`, `test`.

## Testing

- Unit tests are inline in modules (especially `store.rs`, `client/mod.rs`, `query/sql/`).
- Integration tests in `tests/`: `e2e_client.rs`, `integration_otlp_grpc.rs`, `integration_otlp_http.rs`, `integration_query.rs`.
- Tests use dynamic port binding (`get_available_port()` via OS port 0) and `#[tokio::test]`.
- Helper constructors like `make_resource_spans()`, `make_resource_logs()` build test protobuf data.
