use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub tui: TuiConfig,
    #[serde(default)]
    pub defaults: DefaultsConfig,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ServerConfig {
    pub grpc_addr: Option<String>,
    pub http_addr: Option<String>,
    pub query_addr: Option<String>,
    pub otlp_endpoint: Option<String>,
    pub max_traces: Option<u64>,
    pub max_logs: Option<u64>,
    pub max_metrics: Option<u64>,
    /// Maximum age of stored data (e.g., "30s", "5m", "1h", "2d")
    pub max_age: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TuiConfig {
    /// Placeholder for future theme/color settings
    pub theme: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DefaultsConfig {
    /// Default output format: "text", "table", "jsonl", "csv"
    pub output_format: Option<String>,
    /// Default query service address for all client commands
    pub addr: Option<String>,
}

/// Returns the config file path: $XDG_CONFIG_HOME/motel/config.toml or ~/.config/motel/config.toml
pub fn config_path() -> Option<PathBuf> {
    dirs::config_dir().map(|d| d.join("motel").join("config.toml"))
}

/// Load config from disk. Returns Default if file doesn't exist.
pub fn load() -> anyhow::Result<Config> {
    match config_path() {
        Some(path) if path.exists() => {
            let contents = std::fs::read_to_string(&path)?;
            let config: Config = toml::from_str(&contents)
                .map_err(|e| anyhow::anyhow!("invalid config file at {}: {}", path.display(), e))?;
            Ok(config)
        }
        _ => Ok(Config::default()),
    }
}

/// Generate default config file content with comments.
pub fn generate_default() -> String {
    r#"# motel configuration file
# CLI flags always override values set here.

[server]
# gRPC OTLP listen address
# grpc_addr = "0.0.0.0:4317"

# HTTP OTLP listen address
# http_addr = "0.0.0.0:4318"

# Query service listen address
# query_addr = "0.0.0.0:4319"

# OTLP endpoint for self-instrumentation (also settable via OTEL_EXPORTER_OTLP_ENDPOINT)
# otlp_endpoint = "http://localhost:4317"

# Maximum number of traces to keep (by unique trace ID)
# max_traces = 10000

# Maximum number of log record batches to keep
# max_logs = 100000

# Maximum number of metric batches to keep
# max_metrics = 100000

# Maximum age of stored data (e.g., "30s", "5m", "1h", "2d")
# max_age = "1h"

[tui]
# Placeholder for future theme/color settings
# theme = "default"

[defaults]
# Default output format for query commands: "text", "table", "jsonl", "csv"
# output_format = "text"

# Default query service address for all client commands
# addr = "http://localhost:4319"
"#
    .to_string()
}

/// Write the default config file. Creates parent dirs if needed. Errors if file already exists.
pub fn init() -> anyhow::Result<PathBuf> {
    let path = config_path().ok_or_else(|| anyhow::anyhow!("cannot determine config directory"))?;
    if path.exists() {
        anyhow::bail!("config file already exists at {}", path.display());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, generate_default())?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_config() {
        let config: Config = toml::from_str("").unwrap();
        assert!(config.server.grpc_addr.is_none());
        assert!(config.server.max_traces.is_none());
        assert!(config.defaults.output_format.is_none());
        assert!(config.defaults.addr.is_none());
    }

    #[test]
    fn test_parse_full_config() {
        let toml_str = r#"
            [server]
            grpc_addr = "0.0.0.0:5317"
            http_addr = "0.0.0.0:5318"
            query_addr = "0.0.0.0:5319"
            otlp_endpoint = "http://localhost:5317"
            max_traces = 5000
            max_logs = 50000
            max_metrics = 50000

            [tui]
            theme = "dark"

            [defaults]
            output_format = "jsonl"
            addr = "http://localhost:5319"
        "#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.server.grpc_addr.as_deref(), Some("0.0.0.0:5317"));
        assert_eq!(config.server.http_addr.as_deref(), Some("0.0.0.0:5318"));
        assert_eq!(config.server.query_addr.as_deref(), Some("0.0.0.0:5319"));
        assert_eq!(
            config.server.otlp_endpoint.as_deref(),
            Some("http://localhost:5317")
        );
        assert_eq!(config.server.max_traces, Some(5000));
        assert_eq!(config.server.max_logs, Some(50000));
        assert_eq!(config.server.max_metrics, Some(50000));
        assert_eq!(config.tui.theme.as_deref(), Some("dark"));
        assert_eq!(config.defaults.output_format.as_deref(), Some("jsonl"));
        assert_eq!(
            config.defaults.addr.as_deref(),
            Some("http://localhost:5319")
        );
    }

    #[test]
    fn test_parse_partial_config() {
        let toml_str = "[server]\nmax_traces = 500\n";
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.server.max_traces, Some(500));
        assert!(config.server.grpc_addr.is_none());
        assert!(config.defaults.output_format.is_none());
    }

    #[test]
    fn test_generate_default_is_valid_toml() {
        let default = generate_default();
        let config: Config = toml::from_str(&default).unwrap();
        // All values should be None since defaults are commented out
        assert!(config.server.grpc_addr.is_none());
        assert!(config.server.max_traces.is_none());
        assert!(config.defaults.output_format.is_none());
    }

    #[test]
    fn test_config_path_returns_some() {
        // On most systems, config_dir() should return Some
        let path = config_path();
        if let Some(p) = path {
            assert!(p.ends_with("motel/config.toml"));
        }
    }
}
