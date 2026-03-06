use clap::{Parser, Subcommand, ValueEnum};

use crate::config::{self, Config};

#[derive(Parser)]
#[command(
    name = "motel",
    about = "In-memory OpenTelemetry server with querying and TUI"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Start OTLP server with TUI
    Server(ServerArgs),
    /// Attach TUI to a running server
    View(ViewArgs),
    /// Query traces
    Traces(TracesArgs),
    /// Query logs
    Logs(LogsArgs),
    /// Query metrics
    Metrics(MetricsArgs),
    /// Run SQL query
    Sql(SqlArgs),
    /// Show service dependency map
    ServiceMap(ServiceMapArgs),
    /// Export stored data (bulk dump)
    Export(ExportArgs),
    /// Clear stored data
    Clear(ClearArgs),
    /// Check server status
    Status(StatusArgs),
    /// Remotely shutdown a running server
    Shutdown(ShutdownArgs),
    /// Replay stored data to another OTLP endpoint
    Replay(ReplayArgs),
    /// Import telemetry data from files
    Import(ImportArgs),
    /// Show latency histogram for a span name
    Latency(LatencyArgs),
    /// Compare two traces side-by-side
    Diff(DiffArgs),
    /// Install Claude Code skill
    SkillInstall(SkillInstallArgs),
    /// Generate OTLP configuration for your project
    Init(InitArgs),
    /// Start MCP server (stdio transport) for AI tool integration
    Mcp(McpArgs),
    /// Manage configuration
    Config(ConfigCommand),
}

#[derive(clap::Args, Clone)]
pub struct ServerArgs {
    /// Disable TUI (headless mode)
    #[arg(long)]
    pub no_tui: bool,
    /// gRPC OTLP listen address
    #[arg(long)]
    pub grpc_addr: Option<String>,
    /// HTTP OTLP listen address
    #[arg(long)]
    pub http_addr: Option<String>,
    /// Query service listen address
    #[arg(long)]
    pub query_addr: Option<String>,
    /// OTLP endpoint for self-instrumentation
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    pub otlp_endpoint: Option<String>,
    /// Maximum number of traces to keep (by unique trace ID)
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub max_traces: Option<u64>,
    /// Maximum number of log record batches to keep
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub max_logs: Option<u64>,
    /// Maximum number of metric batches to keep
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub max_metrics: Option<u64>,
    /// Path for optional data persistence (file for SQLite, directory for Parquet)
    #[arg(long)]
    pub persist: Option<String>,
    /// Persistence format: sqlite (default) or parquet
    #[arg(long, default_value = "sqlite")]
    pub persist_format: PersistFormat,
    /// Fraction of traces to store (0.0-1.0). 1.0 = store all, 0.1 = store ~10%.
    /// Sampling is by trace_id hash so all spans of a sampled trace are kept.
    #[arg(long, default_value = "1.0", value_parser = parse_sample_rate)]
    pub sample_rate: f64,
    /// Service names that should never be sampled (always kept)
    #[arg(long)]
    pub sample_always: Vec<String>,
    /// Enable web UI dashboard
    #[arg(long)]
    pub web: bool,
    /// Web UI listen address
    #[arg(long, default_value = "0.0.0.0:4320")]
    pub web_addr: String,
    /// Forward received OTLP data to upstream endpoint(s). Can be specified multiple times for fan-out.
    #[arg(long, value_name = "URL")]
    pub forward_to: Vec<String>,
    /// Headers to include in forwarded requests (key=value). Can be specified multiple times.
    #[arg(long, value_name = "KEY=VALUE")]
    pub forward_headers: Vec<String>,
    /// Timeout for forwarding requests in seconds
    #[arg(long, default_value = "10")]
    pub forward_timeout: u64,
    /// Write incoming OTLP data to rotating files in this directory
    #[arg(long)]
    pub sink: Option<String>,
    /// Sink file format
    #[arg(long, default_value = "jsonl", value_enum)]
    pub sink_format: SinkFormat,
    /// Maximum sink file size in bytes before rotation (default: 100MB)
    #[arg(long, default_value = "104857600")]
    pub sink_max_size: u64,
    /// Maximum sink file age before rotation (e.g., 1h, 30m, 24h)
    #[arg(long, default_value = "1h")]
    pub sink_rotate_interval: String,
    /// Enable Prometheus scrape endpoint
    #[arg(long)]
    pub prometheus: bool,
    /// Prometheus scrape endpoint listen address (implies --prometheus)
    #[arg(long, default_value = "0.0.0.0:9090")]
    pub prom_addr: String,
}

/// Resolved server args with all defaults applied (config file + hardcoded).
#[derive(Clone)]
pub struct ResolvedServerArgs {
    pub no_tui: bool,
    pub grpc_addr: String,
    pub http_addr: String,
    pub query_addr: String,
    pub otlp_endpoint: Option<String>,
    pub max_traces: u64,
    pub max_logs: u64,
    pub max_metrics: u64,
    pub persist: Option<String>,
    pub persist_format: PersistFormat,
    pub sample_rate: f64,
    pub sample_always: Vec<String>,
    pub web: bool,
    pub web_addr: String,
    pub forward_to: Vec<String>,
    pub forward_headers: Vec<String>,
    pub forward_timeout: u64,
    pub sink: Option<String>,
    pub sink_format: SinkFormat,
    pub sink_max_size: u64,
    pub sink_rotate_interval: String,
    pub prometheus: bool,
    pub prom_addr: String,
}

/// Validate sample_rate is in [0.0, 1.0]
fn parse_sample_rate(s: &str) -> Result<f64, String> {
    let rate: f64 = s.parse().map_err(|e| format!("invalid number: {e}"))?;
    if !(0.0..=1.0).contains(&rate) {
        return Err("sample-rate must be between 0.0 and 1.0".into());
    }
    Ok(rate)
}

impl ServerArgs {
    /// Resolve CLI args with config file fallbacks and hardcoded defaults.
    /// Precedence: CLI flag > config file > hardcoded default.
    pub fn resolve(self, config: &config::ServerConfig) -> ResolvedServerArgs {
        ResolvedServerArgs {
            no_tui: self.no_tui,
            grpc_addr: self
                .grpc_addr
                .or_else(|| config.grpc_addr.clone())
                .unwrap_or_else(|| "0.0.0.0:4317".to_string()),
            http_addr: self
                .http_addr
                .or_else(|| config.http_addr.clone())
                .unwrap_or_else(|| "0.0.0.0:4318".to_string()),
            query_addr: self
                .query_addr
                .or_else(|| config.query_addr.clone())
                .unwrap_or_else(|| "0.0.0.0:4319".to_string()),
            otlp_endpoint: self.otlp_endpoint.or_else(|| config.otlp_endpoint.clone()),
            max_traces: self.max_traces.or(config.max_traces).unwrap_or(10000),
            max_logs: self.max_logs.or(config.max_logs).unwrap_or(100000),
            max_metrics: self.max_metrics.or(config.max_metrics).unwrap_or(100000),
            persist: self.persist,
            persist_format: self.persist_format,
            sample_rate: self.sample_rate,
            sample_always: self.sample_always,
            web: self.web,
            web_addr: self.web_addr,
            forward_to: self.forward_to,
            forward_headers: self.forward_headers,
            forward_timeout: self.forward_timeout,
            sink: self.sink,
            sink_format: self.sink_format,
            sink_max_size: self.sink_max_size,
            sink_rotate_interval: self.sink_rotate_interval,
            prometheus: self.prometheus,
            prom_addr: self.prom_addr,
        }
    }
}

#[derive(clap::Args, Clone)]
pub struct ViewArgs {
    /// Query service address(es) to connect to (can be specified multiple times)
    #[arg(long)]
    pub addr: Vec<String>,
}

impl ViewArgs {
    pub fn resolve(self, config: &Config) -> ResolvedViewArgs {
        let addr = if self.addr.is_empty() {
            vec![
                config
                    .defaults
                    .addr
                    .clone()
                    .unwrap_or_else(|| "http://localhost:4319".to_string()),
            ]
        } else {
            self.addr
        };
        ResolvedViewArgs { addr }
    }
}

#[derive(Clone)]
pub struct ResolvedViewArgs {
    pub addr: Vec<String>,
}

#[derive(clap::Args, Clone)]
pub struct TracesArgs {
    /// Filter by service name
    #[arg(long)]
    pub service: Option<String>,
    /// Stream new traces as they arrive (like tail -f)
    #[arg(long, short = 'F')]
    pub follow: bool,
    /// Filter by span name
    #[arg(long)]
    pub span_name: Option<String>,
    /// Filter by trace ID (hex)
    #[arg(long)]
    pub trace_id: Option<String>,
    /// Filter by start time (relative: 30s, 5m, 1h, 2d or RFC3339)
    #[arg(long)]
    pub since: Option<String>,
    /// Filter by end time
    #[arg(long)]
    pub until: Option<String>,
    /// Maximum number of results
    #[arg(long)]
    pub limit: Option<i64>,
    /// Filter by attribute (key=value)
    #[arg(long, short = 'a')]
    pub attribute: Vec<String>,
    /// Output format
    #[arg(long, short = 'o')]
    pub output: Option<OutputFormat>,
    /// Show the trace ID of this query request
    #[arg(long)]
    pub show_trace_id: bool,
    /// Query service address
    #[arg(long)]
    pub addr: Option<String>,
}

impl TracesArgs {
    pub fn resolve(self, config: &Config) -> ResolvedTracesArgs {
        ResolvedTracesArgs {
            service: self.service,
            follow: self.follow,
            span_name: self.span_name,
            trace_id: self.trace_id,
            since: self.since,
            until: self.until,
            limit: self.limit,
            attribute: self.attribute,
            output: self
                .output
                .or_else(|| resolve_output_format(&config.defaults.output_format))
                .unwrap_or(OutputFormat::Text),
            show_trace_id: self.show_trace_id,
            addr: self
                .addr
                .or_else(|| config.defaults.addr.clone())
                .unwrap_or_else(|| "http://localhost:4319".to_string()),
        }
    }
}

#[derive(Clone)]
pub struct ResolvedTracesArgs {
    pub service: Option<String>,
    pub follow: bool,
    pub span_name: Option<String>,
    pub trace_id: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub limit: Option<i64>,
    pub attribute: Vec<String>,
    pub output: OutputFormat,
    pub show_trace_id: bool,
    pub addr: String,
}

#[derive(clap::Args, Clone)]
pub struct LogsArgs {
    /// Filter by service name
    #[arg(long)]
    pub service: Option<String>,
    /// Stream new logs as they arrive (like tail -f)
    #[arg(long, short = 'F')]
    pub follow: bool,
    /// Filter by severity
    #[arg(long)]
    pub severity: Option<String>,
    /// Filter by body content
    #[arg(long)]
    pub body: Option<String>,
    /// Filter by start time
    #[arg(long)]
    pub since: Option<String>,
    /// Filter by end time
    #[arg(long)]
    pub until: Option<String>,
    /// Maximum number of results
    #[arg(long)]
    pub limit: Option<i64>,
    /// Filter by attribute (key=value)
    #[arg(long, short = 'a')]
    pub attribute: Vec<String>,
    /// Output format
    #[arg(long, short = 'o')]
    pub output: Option<OutputFormat>,
    /// Show the trace ID of this query request
    #[arg(long)]
    pub show_trace_id: bool,
    /// Query service address
    #[arg(long)]
    pub addr: Option<String>,
}

impl LogsArgs {
    pub fn resolve(self, config: &Config) -> ResolvedLogsArgs {
        ResolvedLogsArgs {
            service: self.service,
            follow: self.follow,
            severity: self.severity,
            body: self.body,
            since: self.since,
            until: self.until,
            limit: self.limit,
            attribute: self.attribute,
            output: self
                .output
                .or_else(|| resolve_output_format(&config.defaults.output_format))
                .unwrap_or(OutputFormat::Text),
            show_trace_id: self.show_trace_id,
            addr: self
                .addr
                .or_else(|| config.defaults.addr.clone())
                .unwrap_or_else(|| "http://localhost:4319".to_string()),
        }
    }
}

#[derive(Clone)]
pub struct ResolvedLogsArgs {
    pub service: Option<String>,
    pub follow: bool,
    pub severity: Option<String>,
    pub body: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub limit: Option<i64>,
    pub attribute: Vec<String>,
    pub output: OutputFormat,
    pub show_trace_id: bool,
    pub addr: String,
}

#[derive(clap::Args, Clone)]
pub struct MetricsArgs {
    /// Filter by service name
    #[arg(long)]
    pub service: Option<String>,
    /// Stream new metrics as they arrive (like tail -f)
    #[arg(long, short = 'F')]
    pub follow: bool,
    /// Filter by metric name
    #[arg(long)]
    pub name: Option<String>,
    /// Filter by start time
    #[arg(long)]
    pub since: Option<String>,
    /// Filter by end time
    #[arg(long)]
    pub until: Option<String>,
    /// Maximum number of results
    #[arg(long)]
    pub limit: Option<i64>,
    /// Output format
    #[arg(long, short = 'o')]
    pub output: Option<OutputFormat>,
    /// Show the trace ID of this query request
    #[arg(long)]
    pub show_trace_id: bool,
    /// Query service address
    #[arg(long)]
    pub addr: Option<String>,
}

impl MetricsArgs {
    pub fn resolve(self, config: &Config) -> ResolvedMetricsArgs {
        ResolvedMetricsArgs {
            service: self.service,
            follow: self.follow,
            name: self.name,
            since: self.since,
            until: self.until,
            limit: self.limit,
            output: self
                .output
                .or_else(|| resolve_output_format(&config.defaults.output_format))
                .unwrap_or(OutputFormat::Text),
            show_trace_id: self.show_trace_id,
            addr: self
                .addr
                .or_else(|| config.defaults.addr.clone())
                .unwrap_or_else(|| "http://localhost:4319".to_string()),
        }
    }
}

#[derive(Clone)]
pub struct ResolvedMetricsArgs {
    pub service: Option<String>,
    pub follow: bool,
    pub name: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub limit: Option<i64>,
    pub output: OutputFormat,
    pub show_trace_id: bool,
    pub addr: String,
}

#[derive(clap::Args, Clone)]
pub struct SqlArgs {
    /// SQL query to execute
    pub query: String,
    /// Output format
    #[arg(long, short = 'o')]
    pub output: Option<OutputFormat>,
    /// Show the trace ID of this query request
    #[arg(long)]
    pub show_trace_id: bool,
    /// Query service address
    #[arg(long)]
    pub addr: Option<String>,
}

impl SqlArgs {
    pub fn resolve(self, config: &Config) -> ResolvedSqlArgs {
        ResolvedSqlArgs {
            query: self.query,
            output: self
                .output
                .or_else(|| resolve_output_format(&config.defaults.output_format))
                .unwrap_or(OutputFormat::Table),
            show_trace_id: self.show_trace_id,
            addr: self
                .addr
                .or_else(|| config.defaults.addr.clone())
                .unwrap_or_else(|| "http://localhost:4319".to_string()),
        }
    }
}

#[derive(Clone)]
pub struct ResolvedSqlArgs {
    pub query: String,
    pub output: OutputFormat,
    pub show_trace_id: bool,
    pub addr: String,
}

#[derive(clap::Args, Clone)]
pub struct ServiceMapArgs {
    /// Output format: ascii or mermaid
    #[arg(long, default_value = "ascii", value_enum)]
    pub format: ServiceMapFormat,
    /// Filter by time window (relative: 30s, 5m, 1h, 2d or RFC3339)
    #[arg(long)]
    pub since: Option<String>,
    /// Show the trace ID of this query request
    #[arg(long)]
    pub show_trace_id: bool,
    /// Query service address
    #[arg(long, default_value = "http://localhost:4319")]
    pub addr: String,
}

#[derive(Clone, ValueEnum)]
pub enum ServiceMapFormat {
    Ascii,
    Mermaid,
}

#[derive(clap::Args, Clone)]
pub struct ClearArgs {
    /// What to clear
    #[arg(value_enum, default_value = "all")]
    pub target: ClearTarget,
    /// Query service address
    #[arg(long)]
    pub addr: Option<String>,
}

impl ClearArgs {
    pub fn resolve(self, config: &Config) -> ResolvedClearArgs {
        ResolvedClearArgs {
            target: self.target,
            addr: self
                .addr
                .or_else(|| config.defaults.addr.clone())
                .unwrap_or_else(|| "http://localhost:4319".to_string()),
        }
    }
}

#[derive(Clone)]
pub struct ResolvedClearArgs {
    pub target: ClearTarget,
    pub addr: String,
}

#[derive(Clone, ValueEnum)]
pub enum ClearTarget {
    Traces,
    Logs,
    Metrics,
    All,
}

#[derive(clap::Args, Clone)]
pub struct StatusArgs {
    /// Query service address
    #[arg(long)]
    pub addr: Option<String>,
}

impl StatusArgs {
    pub fn resolve(self, config: &Config) -> ResolvedStatusArgs {
        ResolvedStatusArgs {
            addr: self
                .addr
                .or_else(|| config.defaults.addr.clone())
                .unwrap_or_else(|| "http://localhost:4319".to_string()),
        }
    }
}

#[derive(Clone)]
pub struct ResolvedStatusArgs {
    pub addr: String,
}

#[derive(clap::Args, Clone)]
pub struct ShutdownArgs {
    /// Query service address
    #[arg(long)]
    pub addr: Option<String>,
}

impl ShutdownArgs {
    pub fn resolve(self, config: &Config) -> ResolvedShutdownArgs {
        ResolvedShutdownArgs {
            addr: self
                .addr
                .or_else(|| config.defaults.addr.clone())
                .unwrap_or_else(|| "http://localhost:4319".to_string()),
        }
    }
}

#[derive(Clone)]
pub struct ResolvedShutdownArgs {
    pub addr: String,
}

#[derive(clap::Args, Clone)]
pub struct LatencyArgs {
    /// Span name to analyze
    pub span_name: String,
    /// Filter by service name
    #[arg(long)]
    pub service: Option<String>,
    /// Filter by start time (relative: 30s, 5m, 1h, 2d or RFC3339)
    #[arg(long)]
    pub since: Option<String>,
    /// Number of histogram buckets
    #[arg(long, default_value = "20")]
    pub buckets: usize,
    /// Output format (text shows histogram, jsonl/csv export raw data)
    #[arg(long, short = 'o', default_value = "text")]
    pub output: OutputFormat,
    /// Show the trace ID of this query request
    #[arg(long)]
    pub show_trace_id: bool,
    /// Query service address
    #[arg(long, default_value = "http://localhost:4319")]
    pub addr: String,
}

#[derive(clap::Args, Clone)]
pub struct SkillInstallArgs {
    /// Install globally instead of for current project
    #[arg(long)]
    pub global: bool,
}

#[derive(clap::Args, Clone)]
pub struct InitArgs {
    /// Language-specific config (node, python, rust, go, java)
    #[arg(long)]
    pub lang: Option<InitLang>,

    /// OTLP endpoint to use in generated config
    #[arg(long, default_value = "http://localhost:4317")]
    pub endpoint: String,

    /// Service name to use in generated config
    #[arg(long, default_value = "my-service")]
    pub service_name: String,

    /// Write to file instead of stdout (only for .env mode)
    #[arg(long, short = 'o')]
    pub output: Option<String>,
}

#[derive(Clone, ValueEnum)]
pub enum InitLang {
    Node,
    Python,
    Rust,
    Go,
    Java,
}

#[derive(clap::Args, Clone)]
pub struct McpArgs {
    /// Query service address to connect to
    #[arg(long, default_value = "http://localhost:4319")]
    pub addr: String,
}

#[derive(clap::Args, Clone)]
pub struct ReplayArgs {
    /// Target OTLP gRPC endpoint to send data to
    #[arg(long)]
    pub target: String,

    /// Signal type(s) to replay (default: all)
    #[arg(long, value_enum, default_value = "all")]
    pub signal: ReplaySignal,

    /// Only replay data newer than this (relative: 30s, 5m, 1h or RFC3339)
    #[arg(long)]
    pub since: Option<String>,

    /// Filter by service name
    #[arg(long)]
    pub service: Option<String>,

    /// Dry run -- show what would be sent without sending
    #[arg(long)]
    pub dry_run: bool,

    /// Query service address (source motel server)
    #[arg(long, default_value = "http://localhost:4319")]
    pub addr: String,
}

#[derive(clap::Args, Clone)]
pub struct ExportArgs {
    /// What to export
    #[arg(value_enum)]
    pub target: ExportTarget,
    /// Output format
    #[arg(long, short = 'o', default_value = "jsonl")]
    pub output: ExportFormat,
    /// Query service address
    #[arg(long, default_value = "http://localhost:4319")]
    pub addr: String,
}

#[derive(Clone, ValueEnum)]
pub enum ReplaySignal {
    Traces,
    Logs,
    Metrics,
    All,
}

#[derive(Clone, ValueEnum)]
pub enum ExportTarget {
    Traces,
    Logs,
    Metrics,
    All,
}

#[derive(clap::Args, Clone)]
pub struct ImportArgs {
    /// File path(s) to import
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Data format (auto-detected from extension if not specified)
    #[arg(long, short = 'f', value_enum)]
    pub format: Option<ImportFormat>,

    /// Signal type (auto-detected from filename if not specified)
    #[arg(long, short = 't', value_enum)]
    pub signal: Option<SignalType>,

    /// OTLP gRPC endpoint to send imported data to
    #[arg(long, default_value = "http://localhost:4317")]
    pub addr: String,

    /// Batch size for sending (number of records per request)
    #[arg(long, default_value = "100")]
    pub batch_size: usize,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum ImportFormat {
    /// JSONL format (motel's export format from --output jsonl)
    Jsonl,
    /// OTLP protobuf binary
    OtlpProto,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum SignalType {
    Traces,
    Logs,
    Metrics,
}

#[derive(Clone, ValueEnum)]
pub enum ExportFormat {
    Text,
    Jsonl,
    Csv,
    Proto,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum PersistFormat {
    Sqlite,
    Parquet,
}

#[derive(clap::Args, Clone)]
pub struct ConfigCommand {
    #[command(subcommand)]
    pub action: ConfigAction,
}

#[derive(Subcommand, Clone)]
pub enum ConfigAction {
    /// Generate default config file
    Init,
    /// Print current config file path
    Path,
    /// Print resolved config (merged file + defaults)
    Show,
}

#[derive(clap::Args, Clone)]
pub struct DiffArgs {
    /// First trace ID (hex)
    pub trace_id_a: String,
    /// Second trace ID (hex)
    pub trace_id_b: String,
    /// Output format
    #[arg(long, short = 'o', default_value = "text")]
    pub output: OutputFormat,
    /// Query service address
    #[arg(long, default_value = "http://localhost:4319")]
    pub addr: String,
    /// Duration change threshold for highlighting (percentage)
    #[arg(long, default_value = "20")]
    pub threshold: u32,
}

#[derive(Clone, ValueEnum)]
pub enum SinkFormat {
    /// One JSON object per line (newline-delimited)
    Jsonl,
    /// Length-prefixed protobuf binary
    Proto,
}

#[derive(Clone, ValueEnum)]
pub enum OutputFormat {
    Text,
    Table,
    Jsonl,
    Csv,
}

/// Parse an output format string from config into an OutputFormat enum.
fn resolve_output_format(format: &Option<String>) -> Option<OutputFormat> {
    format.as_deref().and_then(|s| match s {
        "text" => Some(OutputFormat::Text),
        "table" => Some(OutputFormat::Table),
        "jsonl" => Some(OutputFormat::Jsonl),
        "csv" => Some(OutputFormat::Csv),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_args_resolve_uses_cli_over_config() {
        let args = ServerArgs {
            no_tui: false,
            grpc_addr: Some("1.2.3.4:5555".to_string()),
            http_addr: None,
            query_addr: None,
            otlp_endpoint: None,
            max_traces: Some(999),
            max_logs: None,
            max_metrics: None,
            persist: None,
            persist_format: PersistFormat::Sqlite,
            sample_rate: 1.0,
            sample_always: vec![],
            web: false,
            web_addr: "0.0.0.0:4320".to_string(),
            forward_to: vec![],
            forward_headers: vec![],
            forward_timeout: 10,
            sink: None,
            sink_format: SinkFormat::Jsonl,
            sink_max_size: 104857600,
            sink_rotate_interval: "1h".to_string(),
            prometheus: false,
            prom_addr: "0.0.0.0:9090".to_string(),
        };
        let config = config::ServerConfig {
            grpc_addr: Some("9.9.9.9:1111".to_string()),
            http_addr: Some("9.9.9.9:2222".to_string()),
            query_addr: None,
            otlp_endpoint: None,
            max_traces: Some(5000),
            max_logs: Some(50000),
            max_metrics: None,
        };
        let resolved = args.resolve(&config);
        // CLI wins over config
        assert_eq!(resolved.grpc_addr, "1.2.3.4:5555");
        assert_eq!(resolved.max_traces, 999);
        // Config wins over hardcoded default
        assert_eq!(resolved.http_addr, "9.9.9.9:2222");
        assert_eq!(resolved.max_logs, 50000);
        // Hardcoded default
        assert_eq!(resolved.query_addr, "0.0.0.0:4319");
        assert_eq!(resolved.max_metrics, 100000);
    }

    #[test]
    fn test_server_args_resolve_all_defaults() {
        let args = ServerArgs {
            no_tui: false,
            grpc_addr: None,
            http_addr: None,
            query_addr: None,
            otlp_endpoint: None,
            max_traces: None,
            max_logs: None,
            max_metrics: None,
            persist: None,
            persist_format: PersistFormat::Sqlite,
            sample_rate: 1.0,
            sample_always: vec![],
            web: false,
            web_addr: "0.0.0.0:4320".to_string(),
            forward_to: vec![],
            forward_headers: vec![],
            forward_timeout: 10,
            sink: None,
            sink_format: SinkFormat::Jsonl,
            sink_max_size: 104857600,
            sink_rotate_interval: "1h".to_string(),
            prometheus: false,
            prom_addr: "0.0.0.0:9090".to_string(),
        };
        let config = config::ServerConfig::default();
        let resolved = args.resolve(&config);
        assert_eq!(resolved.grpc_addr, "0.0.0.0:4317");
        assert_eq!(resolved.http_addr, "0.0.0.0:4318");
        assert_eq!(resolved.query_addr, "0.0.0.0:4319");
        assert_eq!(resolved.max_traces, 10000);
        assert_eq!(resolved.max_logs, 100000);
        assert_eq!(resolved.max_metrics, 100000);
    }

    #[test]
    fn test_resolve_output_format() {
        assert!(resolve_output_format(&None).is_none());
        assert!(resolve_output_format(&Some("invalid".to_string())).is_none());
        assert!(matches!(
            resolve_output_format(&Some("jsonl".to_string())),
            Some(OutputFormat::Jsonl)
        ));
        assert!(matches!(
            resolve_output_format(&Some("table".to_string())),
            Some(OutputFormat::Table)
        ));
    }

    #[test]
    fn test_client_args_resolve_addr_from_config() {
        let config = Config {
            defaults: config::DefaultsConfig {
                addr: Some("http://custom:9999".to_string()),
                output_format: Some("csv".to_string()),
            },
            ..Config::default()
        };

        let args = TracesArgs {
            service: None,
            follow: false,
            span_name: None,
            trace_id: None,
            since: None,
            until: None,
            limit: None,
            attribute: vec![],
            output: None,
            show_trace_id: false,
            addr: None,
        };
        let resolved = args.resolve(&config);
        assert_eq!(resolved.addr, "http://custom:9999");
        assert!(matches!(resolved.output, OutputFormat::Csv));
    }

    #[test]
    fn test_client_args_cli_overrides_config() {
        let config = Config {
            defaults: config::DefaultsConfig {
                addr: Some("http://custom:9999".to_string()),
                output_format: Some("csv".to_string()),
            },
            ..Config::default()
        };

        let args = SqlArgs {
            query: "SELECT 1".to_string(),
            output: Some(OutputFormat::Jsonl),
            show_trace_id: false,
            addr: Some("http://explicit:1234".to_string()),
        };
        let resolved = args.resolve(&config);
        assert_eq!(resolved.addr, "http://explicit:1234");
        assert!(matches!(resolved.output, OutputFormat::Jsonl));
    }
}
