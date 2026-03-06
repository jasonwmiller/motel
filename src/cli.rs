use clap::{Parser, Subcommand, ValueEnum};

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
    /// Clear stored data
    Clear(ClearArgs),
    /// Check server status
    Status(StatusArgs),
    /// Remotely shutdown a running server
    Shutdown(ShutdownArgs),
    /// Install Claude Code skill
    SkillInstall(SkillInstallArgs),
    /// Generate OTLP configuration for your project
    Init(InitArgs),
}

#[derive(clap::Args, Clone)]
pub struct ServerArgs {
    /// Disable TUI (headless mode)
    #[arg(long)]
    pub no_tui: bool,
    /// gRPC OTLP listen address
    #[arg(long, default_value = "0.0.0.0:4317")]
    pub grpc_addr: String,
    /// HTTP OTLP listen address
    #[arg(long, default_value = "0.0.0.0:4318")]
    pub http_addr: String,
    /// Query service listen address
    #[arg(long, default_value = "0.0.0.0:4319")]
    pub query_addr: String,
    /// OTLP endpoint for self-instrumentation
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    pub otlp_endpoint: Option<String>,
    /// Maximum number of traces to keep (by unique trace ID)
    #[arg(long, default_value = "10000", value_parser = clap::value_parser!(u64).range(1..))]
    pub max_traces: u64,
    /// Maximum number of log record batches to keep
    #[arg(long, default_value = "100000", value_parser = clap::value_parser!(u64).range(1..))]
    pub max_logs: u64,
    /// Maximum number of metric batches to keep
    #[arg(long, default_value = "100000", value_parser = clap::value_parser!(u64).range(1..))]
    pub max_metrics: u64,
}

#[derive(clap::Args, Clone)]
pub struct ViewArgs {
    /// Query service address to connect to
    #[arg(long, default_value = "http://localhost:4319")]
    pub addr: String,
}

#[derive(clap::Args, Clone)]
pub struct TracesArgs {
    /// Filter by service name
    #[arg(long)]
    pub service: Option<String>,
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
pub struct LogsArgs {
    /// Filter by service name
    #[arg(long)]
    pub service: Option<String>,
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
pub struct MetricsArgs {
    /// Filter by service name
    #[arg(long)]
    pub service: Option<String>,
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
pub struct SqlArgs {
    /// SQL query to execute
    pub query: String,
    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
    /// Show the trace ID of this query request
    #[arg(long)]
    pub show_trace_id: bool,
    /// Query service address
    #[arg(long, default_value = "http://localhost:4319")]
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
    #[arg(long, default_value = "http://localhost:4319")]
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
    #[arg(long, default_value = "http://localhost:4319")]
    pub addr: String,
}

#[derive(clap::Args, Clone)]
pub struct ShutdownArgs {
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

#[derive(Clone, ValueEnum)]
pub enum OutputFormat {
    Text,
    Table,
    Jsonl,
    Csv,
}
