use anyhow::Result;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{Implementation, ServerCapabilities, ServerInfo};
use rmcp::{ServerHandler, ServiceExt, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::cli::McpArgs;
use crate::client::hex_encode;
use crate::client::trace::format_timestamp_ns;
use crate::query_proto::query_service_client::QueryServiceClient;
use crate::query_proto::{
    QueryLogsRequest, QueryMetricsRequest, QueryTracesRequest, SqlQueryRequest, StatusRequest,
};

type GrpcClient = QueryServiceClient<tonic::transport::Channel>;

/// MCP server that exposes motel query capabilities as tools.
#[derive(Clone)]
pub struct MotelMcpServer {
    client: Arc<Mutex<GrpcClient>>,
    tool_router: ToolRouter<Self>,
}

impl MotelMcpServer {
    pub fn new(client: GrpcClient) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
            tool_router: Self::tool_router(),
        }
    }
}

// -- Tool parameter types --

/// Parameters for querying traces.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct QueryTracesParams {
    /// Filter by service name.
    #[schemars(description = "Filter by service name")]
    pub service: Option<String>,
    /// Filter by span name.
    #[schemars(description = "Filter by span name")]
    pub span_name: Option<String>,
    /// Filter by trace ID (hex).
    #[schemars(description = "Filter by trace ID (hex string)")]
    pub trace_id: Option<String>,
    /// Maximum number of results.
    #[schemars(description = "Maximum number of results to return")]
    pub limit: Option<i64>,
}

/// Parameters for querying logs.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct QueryLogsParams {
    /// Filter by service name.
    #[schemars(description = "Filter by service name")]
    pub service: Option<String>,
    /// Filter by severity level (e.g., ERROR, WARN, INFO).
    #[schemars(description = "Filter by severity level (e.g., ERROR, WARN, INFO)")]
    pub severity: Option<String>,
    /// Filter by body content (substring match).
    #[schemars(description = "Filter by body content (substring match)")]
    pub body: Option<String>,
    /// Maximum number of results.
    #[schemars(description = "Maximum number of results to return")]
    pub limit: Option<i64>,
}

/// Parameters for querying metrics.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct QueryMetricsParams {
    /// Filter by service name.
    #[schemars(description = "Filter by service name")]
    pub service: Option<String>,
    /// Filter by metric name.
    #[schemars(description = "Filter by metric name")]
    pub name: Option<String>,
    /// Maximum number of results.
    #[schemars(description = "Maximum number of results to return")]
    pub limit: Option<i64>,
}

/// Parameters for running SQL queries.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RunSqlParams {
    /// SQL query to execute against traces, logs, and metrics tables.
    #[schemars(
        description = "SQL query to execute. Available tables: traces, logs, metrics. Supports standard SQL including aggregation, subqueries, joins. Access attributes via bracket syntax: attributes['key'], resource['key']."
    )]
    pub query: String,
}

// -- Response formatting helpers --

fn format_traces_response(resource_spans: &[crate::otel::trace::v1::ResourceSpans]) -> String {
    let mut lines = Vec::new();
    let mut count = 0;
    for rs in resource_spans {
        let service_name = rs
            .resource
            .as_ref()
            .and_then(|r| {
                r.attributes
                    .iter()
                    .find(|kv| kv.key == "service.name")
                    .and_then(|kv| kv.value.as_ref())
                    .and_then(|v| match &v.value {
                        Some(crate::otel::common::v1::any_value::Value::StringValue(s)) => {
                            Some(s.clone())
                        }
                        _ => None,
                    })
            })
            .unwrap_or_default();
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                let start_ns = span.start_time_unix_nano;
                let end_ns = span.end_time_unix_nano;
                let duration_ns = end_ns.saturating_sub(start_ns);
                let duration_ms = duration_ns as f64 / 1_000_000.0;
                lines.push(format!(
                    "{} {} {} {:.3}ms trace_id={}",
                    format_timestamp_ns(start_ns),
                    service_name,
                    span.name,
                    duration_ms,
                    hex_encode(&span.trace_id),
                ));
                count += 1;
            }
        }
    }
    if lines.is_empty() {
        "No traces found.".to_string()
    } else {
        format!("{} span(s) found:\n{}", count, lines.join("\n"))
    }
}

fn format_logs_response(resource_logs: &[crate::otel::logs::v1::ResourceLogs]) -> String {
    let mut lines = Vec::new();
    let mut count = 0;
    for rl in resource_logs {
        let service_name = rl
            .resource
            .as_ref()
            .and_then(|r| {
                r.attributes
                    .iter()
                    .find(|kv| kv.key == "service.name")
                    .and_then(|kv| kv.value.as_ref())
                    .and_then(|v| match &v.value {
                        Some(crate::otel::common::v1::any_value::Value::StringValue(s)) => {
                            Some(s.clone())
                        }
                        _ => None,
                    })
            })
            .unwrap_or_default();
        for sl in &rl.scope_logs {
            for lr in &sl.log_records {
                let body = lr
                    .body
                    .as_ref()
                    .and_then(|v| match &v.value {
                        Some(crate::otel::common::v1::any_value::Value::StringValue(s)) => {
                            Some(s.clone())
                        }
                        Some(other) => Some(format!("{:?}", other)),
                        None => None,
                    })
                    .unwrap_or_default();
                lines.push(format!(
                    "{} {} [{:?}] {}",
                    format_timestamp_ns(lr.time_unix_nano),
                    service_name,
                    lr.severity_number(),
                    body,
                ));
                count += 1;
            }
        }
    }
    if lines.is_empty() {
        "No logs found.".to_string()
    } else {
        format!("{} log(s) found:\n{}", count, lines.join("\n"))
    }
}

fn format_metrics_response(
    resource_metrics: &[crate::otel::metrics::v1::ResourceMetrics],
) -> String {
    let mut lines = Vec::new();
    let mut count = 0;
    for rm in resource_metrics {
        let service_name = rm
            .resource
            .as_ref()
            .and_then(|r| {
                r.attributes
                    .iter()
                    .find(|kv| kv.key == "service.name")
                    .and_then(|kv| kv.value.as_ref())
                    .and_then(|v| match &v.value {
                        Some(crate::otel::common::v1::any_value::Value::StringValue(s)) => {
                            Some(s.clone())
                        }
                        _ => None,
                    })
            })
            .unwrap_or_default();
        for sm in &rm.scope_metrics {
            for metric in &sm.metrics {
                let data_type = match &metric.data {
                    Some(crate::otel::metrics::v1::metric::Data::Gauge(_)) => "Gauge",
                    Some(crate::otel::metrics::v1::metric::Data::Sum(_)) => "Sum",
                    Some(crate::otel::metrics::v1::metric::Data::Histogram(_)) => "Histogram",
                    Some(crate::otel::metrics::v1::metric::Data::ExponentialHistogram(_)) => {
                        "ExponentialHistogram"
                    }
                    Some(crate::otel::metrics::v1::metric::Data::Summary(_)) => "Summary",
                    None => "Unknown",
                };
                lines.push(format!(
                    "{} {} ({}) [{}] {}",
                    service_name, metric.name, metric.unit, data_type, metric.description,
                ));
                count += 1;
            }
        }
    }
    if lines.is_empty() {
        "No metrics found.".to_string()
    } else {
        format!("{} metric(s) found:\n{}", count, lines.join("\n"))
    }
}

fn format_sql_response(
    columns: &[crate::query_proto::Column],
    rows: &[crate::query_proto::Row],
) -> String {
    if rows.is_empty() {
        if columns.is_empty() {
            return "No results.".to_string();
        }
        let headers: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        return format!("Columns: {}\n(0 rows)", headers.join(", "));
    }

    let col_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();

    // Calculate column widths
    let widths: Vec<usize> = col_names
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let max_data = rows
                .iter()
                .map(|r| r.values.get(i).map(|v| v.len()).unwrap_or(0))
                .max()
                .unwrap_or(0);
            h.len().max(max_data)
        })
        .collect();

    let mut output = Vec::new();

    // Header
    let header_line: Vec<String> = col_names
        .iter()
        .enumerate()
        .map(|(i, h)| format!("{:<width$}", h, width = widths[i]))
        .collect();
    output.push(header_line.join("  "));

    // Separator
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    output.push(sep.join("  "));

    // Rows
    for row in rows {
        let line: Vec<String> = row
            .values
            .iter()
            .enumerate()
            .map(|(i, val)| format!("{:<width$}", val, width = widths[i]))
            .collect();
        output.push(line.join("  "));
    }

    output.push(format!("({} row(s))", rows.len()));
    output.join("\n")
}

// -- Tool implementations --

#[tool_router]
impl MotelMcpServer {
    /// Query traces stored in motel with optional filters for service, span name, trace ID, and result limit.
    #[tool(name = "query_traces")]
    async fn query_traces(&self, params: Parameters<QueryTracesParams>) -> Result<String, String> {
        let request = QueryTracesRequest {
            service_name: params.0.service.unwrap_or_default(),
            span_name: params.0.span_name.unwrap_or_default(),
            trace_id: params.0.trace_id.unwrap_or_default(),
            limit: params.0.limit.unwrap_or(0),
            ..Default::default()
        };
        let mut client = self.client.lock().await;
        let response = client
            .query_traces(request)
            .await
            .map_err(|e| format!("gRPC error: {}", e))?;
        let resp = response.into_inner();
        Ok(format_traces_response(&resp.resource_spans))
    }

    /// Query logs stored in motel with optional filters for service, severity, body content, and result limit.
    #[tool(name = "query_logs")]
    async fn query_logs(&self, params: Parameters<QueryLogsParams>) -> Result<String, String> {
        let request = QueryLogsRequest {
            service_name: params.0.service.unwrap_or_default(),
            severity: params.0.severity.unwrap_or_default(),
            body_contains: params.0.body.unwrap_or_default(),
            limit: params.0.limit.unwrap_or(0),
            ..Default::default()
        };
        let mut client = self.client.lock().await;
        let response = client
            .query_logs(request)
            .await
            .map_err(|e| format!("gRPC error: {}", e))?;
        let resp = response.into_inner();
        Ok(format_logs_response(&resp.resource_logs))
    }

    /// Query metrics stored in motel with optional filters for service, metric name, and result limit.
    #[tool(name = "query_metrics")]
    async fn query_metrics(
        &self,
        params: Parameters<QueryMetricsParams>,
    ) -> Result<String, String> {
        let request = QueryMetricsRequest {
            service_name: params.0.service.unwrap_or_default(),
            metric_name: params.0.name.unwrap_or_default(),
            limit: params.0.limit.unwrap_or(0),
            ..Default::default()
        };
        let mut client = self.client.lock().await;
        let response = client
            .query_metrics(request)
            .await
            .map_err(|e| format!("gRPC error: {}", e))?;
        let resp = response.into_inner();
        Ok(format_metrics_response(&resp.resource_metrics))
    }

    /// Execute an arbitrary SQL query against motel's traces, logs, and metrics tables. Supports standard SQL including aggregation, subqueries, joins, and attribute access via bracket syntax (e.g., attributes['key']).
    #[tool(name = "run_sql")]
    async fn run_sql(&self, params: Parameters<RunSqlParams>) -> Result<String, String> {
        let request = SqlQueryRequest {
            query: params.0.query,
        };
        let mut client = self.client.lock().await;
        let response = client
            .sql_query(request)
            .await
            .map_err(|e| format!("gRPC error: {}", e))?;
        let resp = response.into_inner();
        Ok(format_sql_response(&resp.columns, &resp.rows))
    }

    /// Get the current status of the motel server, showing counts of stored traces, logs, and metrics.
    #[tool(name = "get_status")]
    async fn get_status(&self) -> Result<String, String> {
        let mut client = self.client.lock().await;
        let response = client
            .status(StatusRequest {})
            .await
            .map_err(|e| format!("gRPC error: {}", e))?;
        let resp = response.into_inner();
        Ok(format!(
            "Traces:  {} ({} spans)\nLogs:    {}\nMetrics: {}",
            resp.trace_count, resp.span_count, resp.log_count, resp.metric_count,
        ))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for MotelMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                "motel-mcp",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "motel is an in-memory OpenTelemetry server. Use these tools to query traces, logs, and metrics stored in motel. \
                 The run_sql tool is the most powerful — it supports full SQL against traces, logs, and metrics tables."
                    .to_string(),
            )
    }
}

/// Run the MCP server over stdio, connecting to motel's query service.
pub async fn run(args: McpArgs) -> Result<()> {
    let client = QueryServiceClient::connect(args.addr.clone()).await?;
    let server = MotelMcpServer::new(client);

    let transport = (tokio::io::stdin(), tokio::io::stdout());
    let running = server.serve(transport).await?;
    running.waiting().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_definitions() {
        // Verify all 5 tools are defined with correct names
        let tool_names = [
            "query_traces",
            "query_logs",
            "query_metrics",
            "run_sql",
            "get_status",
        ];

        for name in &tool_names {
            let attr_fn = match *name {
                "query_traces" => MotelMcpServer::query_traces_tool_attr(),
                "query_logs" => MotelMcpServer::query_logs_tool_attr(),
                "query_metrics" => MotelMcpServer::query_metrics_tool_attr(),
                "run_sql" => MotelMcpServer::run_sql_tool_attr(),
                "get_status" => MotelMcpServer::get_status_tool_attr(),
                _ => unreachable!(),
            };
            assert_eq!(
                attr_fn.name.as_ref(),
                *name,
                "Tool name mismatch for {}",
                name
            );
            // Verify input_schema has type "object"
            assert_eq!(
                attr_fn.input_schema.get("type"),
                Some(&serde_json::Value::String("object".to_string())),
                "Tool {} should have object input schema",
                name
            );
        }
    }

    #[test]
    fn test_query_traces_schema_has_optional_fields() {
        let attr = MotelMcpServer::query_traces_tool_attr();
        let props = attr.input_schema.get("properties").unwrap();
        // All fields should be present
        assert!(props.get("service").is_some());
        assert!(props.get("span_name").is_some());
        assert!(props.get("trace_id").is_some());
        assert!(props.get("limit").is_some());
        // No required fields (all optional)
        let required = attr.input_schema.get("required");
        assert!(
            required.is_none() || required.unwrap().as_array().map_or(true, |a| a.is_empty()),
            "query_traces should have no required fields"
        );
    }

    #[test]
    fn test_run_sql_schema_requires_query() {
        let attr = MotelMcpServer::run_sql_tool_attr();
        let props = attr.input_schema.get("properties").unwrap();
        assert!(props.get("query").is_some());
        // query should be required
        let required = attr.input_schema.get("required").and_then(|r| r.as_array());
        assert!(required.is_some(), "run_sql should have required fields");
        let required = required.unwrap();
        assert!(
            required.contains(&serde_json::Value::String("query".to_string())),
            "run_sql should require 'query' field"
        );
    }

    #[test]
    fn test_format_traces_empty() {
        assert_eq!(format_traces_response(&[]), "No traces found.");
    }

    #[test]
    fn test_format_logs_empty() {
        assert_eq!(format_logs_response(&[]), "No logs found.");
    }

    #[test]
    fn test_format_metrics_empty() {
        assert_eq!(format_metrics_response(&[]), "No metrics found.");
    }

    #[test]
    fn test_format_sql_empty() {
        assert_eq!(format_sql_response(&[], &[]), "No results.");
    }

    #[test]
    fn test_format_sql_with_data() {
        use crate::query_proto::{Column, Row};

        let columns = vec![
            Column {
                name: "name".into(),
                data_type: "Utf8".into(),
            },
            Column {
                name: "count".into(),
                data_type: "Int64".into(),
            },
        ];
        let rows = vec![
            Row {
                values: vec!["foo".into(), "42".into()],
            },
            Row {
                values: vec!["bar".into(), "7".into()],
            },
        ];
        let result = format_sql_response(&columns, &rows);
        assert!(result.contains("name"));
        assert!(result.contains("count"));
        assert!(result.contains("foo"));
        assert!(result.contains("42"));
        assert!(result.contains("bar"));
        assert!(result.contains("7"));
        assert!(result.contains("(2 row(s))"));
    }

    #[test]
    fn test_format_sql_headers_only() {
        use crate::query_proto::Column;

        let columns = vec![Column {
            name: "a".into(),
            data_type: "Utf8".into(),
        }];
        let result = format_sql_response(&columns, &[]);
        assert!(result.contains("Columns: a"));
        assert!(result.contains("(0 rows)"));
    }

    #[test]
    fn test_get_status_tool_attr() {
        let attr = MotelMcpServer::get_status_tool_attr();
        assert_eq!(attr.name.as_ref(), "get_status");
    }
}
