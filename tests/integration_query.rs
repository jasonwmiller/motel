// Integration tests for the motel query gRPC API.
//
// Tests cover: QueryTraces, QueryLogs, QueryMetrics with filters,
// SQL queries, Clear operations, Status, and Follow streams.

mod common;

use common::ServerGuard;
use common::otel::collector::logs::v1::logs_service_client::LogsServiceClient;
use common::otel::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use common::otel::collector::trace::v1::trace_service_client::TraceServiceClient;
use common::query_proto::query_service_client::QueryServiceClient;
use common::query_proto::{
    ClearRequest, FollowRequest, QueryLogsRequest, QueryMetricsRequest, QueryTracesRequest,
    SqlQueryRequest, StatusRequest,
};

// ---------------------------------------------------------------------------
// Helper: ingest test data into a running server
// ---------------------------------------------------------------------------

async fn ingest_test_traces(server: &ServerGuard) {
    let mut client = TraceServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect trace client");

    // Two different services, three traces total.
    client
        .export(common::make_export_trace_request(&[1u8; 16], "span-alpha"))
        .await
        .expect("export trace 1");

    client
        .export(common::make_export_trace_request(&[2u8; 16], "span-beta"))
        .await
        .expect("export trace 2");

    // Third trace with a different service name — we build it manually.
    let mut rs = common::make_resource_spans(&[3u8; 16], "span-gamma");
    if let Some(ref mut resource) = rs.resource {
        for attr in &mut resource.attributes {
            if attr.key == "service.name" {
                attr.value = Some(common::otel::common::v1::AnyValue {
                    value: Some(common::otel::common::v1::any_value::Value::StringValue(
                        "other-service".into(),
                    )),
                });
            }
        }
    }
    client
        .export(
            common::otel::collector::trace::v1::ExportTraceServiceRequest {
                resource_spans: vec![rs],
            },
        )
        .await
        .expect("export trace 3");
}

async fn ingest_test_logs(server: &ServerGuard) {
    let mut client = LogsServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect logs client");

    client
        .export(common::make_export_logs_request("application started"))
        .await
        .expect("export log 1");

    client
        .export(common::make_export_logs_request("request processed"))
        .await
        .expect("export log 2");
}

async fn ingest_test_metrics(server: &ServerGuard) {
    let mut client = MetricsServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect metrics client");

    client
        .export(common::make_export_metrics_request("cpu.usage", 55.0))
        .await
        .expect("export metric 1");

    client
        .export(common::make_export_metrics_request("memory.usage", 2048.0))
        .await
        .expect("export metric 2");
}

// ===========================================================================
// QueryTraces tests
// ===========================================================================

/// Query all traces (no filters).
#[tokio::test]
async fn test_query_traces_no_filter() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .query_traces(QueryTracesRequest::default())
        .await
        .expect("query traces")
        .into_inner();

    assert_eq!(resp.resource_spans.len(), 3, "expected 3 resource spans");
}

/// Query traces filtered by service_name.
#[tokio::test]
async fn test_query_traces_by_service_name() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .query_traces(QueryTracesRequest {
            service_name: "test-service".into(),
            ..Default::default()
        })
        .await
        .expect("query traces by service")
        .into_inner();

    assert_eq!(
        resp.resource_spans.len(),
        2,
        "expected 2 spans from test-service"
    );
}

/// Query traces filtered by span_name.
#[tokio::test]
async fn test_query_traces_by_span_name() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .query_traces(QueryTracesRequest {
            span_name: "span-alpha".into(),
            ..Default::default()
        })
        .await
        .expect("query traces by span name")
        .into_inner();

    assert_eq!(resp.resource_spans.len(), 1);
}

/// Query traces filtered by trace_id (hex encoded).
#[tokio::test]
async fn test_query_traces_by_trace_id() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    // trace_id [2u8; 16] as hex
    let trace_id_hex = hex::encode([2u8; 16]);
    let resp = client
        .query_traces(QueryTracesRequest {
            trace_id: trace_id_hex,
            ..Default::default()
        })
        .await
        .expect("query traces by trace_id")
        .into_inner();

    assert_eq!(resp.resource_spans.len(), 1);
    // Verify the span name matches.
    let span = &resp.resource_spans[0].scope_spans[0].spans[0];
    assert_eq!(span.name, "span-beta");
}

/// Query traces with a limit.
#[tokio::test]
async fn test_query_traces_with_limit() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .query_traces(QueryTracesRequest {
            limit: 1,
            ..Default::default()
        })
        .await
        .expect("query traces with limit")
        .into_inner();

    assert_eq!(resp.resource_spans.len(), 1);
}

// ===========================================================================
// QueryLogs tests
// ===========================================================================

/// Query all logs (no filters).
#[tokio::test]
async fn test_query_logs_no_filter() {
    let server = ServerGuard::start().await;
    ingest_test_logs(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .query_logs(QueryLogsRequest::default())
        .await
        .expect("query logs")
        .into_inner();

    assert_eq!(resp.resource_logs.len(), 2);
}

/// Query logs filtered by body content.
#[tokio::test]
async fn test_query_logs_by_body() {
    let server = ServerGuard::start().await;
    ingest_test_logs(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .query_logs(QueryLogsRequest {
            body_contains: "started".into(),
            ..Default::default()
        })
        .await
        .expect("query logs by body")
        .into_inner();

    assert_eq!(resp.resource_logs.len(), 1);
}

/// Query logs filtered by service name.
#[tokio::test]
async fn test_query_logs_by_service() {
    let server = ServerGuard::start().await;
    ingest_test_logs(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .query_logs(QueryLogsRequest {
            service_name: "test-service".into(),
            ..Default::default()
        })
        .await
        .expect("query logs by service")
        .into_inner();

    assert_eq!(resp.resource_logs.len(), 2);

    // Non-existent service should return zero results.
    let resp = client
        .query_logs(QueryLogsRequest {
            service_name: "nonexistent-service".into(),
            ..Default::default()
        })
        .await
        .expect("query logs by nonexistent service")
        .into_inner();

    assert_eq!(resp.resource_logs.len(), 0);
}

// ===========================================================================
// QueryMetrics tests
// ===========================================================================

/// Query all metrics (no filters).
#[tokio::test]
async fn test_query_metrics_no_filter() {
    let server = ServerGuard::start().await;
    ingest_test_metrics(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .query_metrics(QueryMetricsRequest::default())
        .await
        .expect("query metrics")
        .into_inner();

    assert_eq!(resp.resource_metrics.len(), 2);
}

/// Query metrics filtered by metric name.
#[tokio::test]
async fn test_query_metrics_by_name() {
    let server = ServerGuard::start().await;
    ingest_test_metrics(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .query_metrics(QueryMetricsRequest {
            metric_name: "cpu.usage".into(),
            ..Default::default()
        })
        .await
        .expect("query metrics by name")
        .into_inner();

    assert_eq!(resp.resource_metrics.len(), 1);
}

// ===========================================================================
// SQL query tests
// ===========================================================================

/// Execute a simple SELECT * SQL query on traces.
#[tokio::test]
async fn test_sql_select_all_traces() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .sql_query(SqlQueryRequest {
            query: "SELECT * FROM traces".into(),
        })
        .await
        .expect("sql query")
        .into_inner();

    // Should return rows for the 3 spans ingested.
    assert_eq!(resp.rows.len(), 3, "expected 3 rows from SQL query");
    assert!(!resp.columns.is_empty(), "expected column metadata");
}

/// SQL query with WHERE clause.
#[tokio::test]
async fn test_sql_where_clause() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .sql_query(SqlQueryRequest {
            query: "SELECT span_name FROM traces WHERE service_name = 'test-service'".into(),
        })
        .await
        .expect("sql query with WHERE")
        .into_inner();

    assert_eq!(resp.rows.len(), 2, "expected 2 rows for test-service");
}

/// SQL query with GROUP BY and COUNT.
#[tokio::test]
async fn test_sql_group_by() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .sql_query(SqlQueryRequest {
            query: "SELECT service_name, COUNT(*) as cnt FROM traces GROUP BY service_name".into(),
        })
        .await
        .expect("sql query with GROUP BY")
        .into_inner();

    // Two groups: "test-service" and "other-service".
    assert_eq!(resp.rows.len(), 2, "expected 2 groups");
}

/// SQL query with LIMIT.
#[tokio::test]
async fn test_sql_limit() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .sql_query(SqlQueryRequest {
            query: "SELECT * FROM traces LIMIT 1".into(),
        })
        .await
        .expect("sql query with LIMIT")
        .into_inner();

    assert_eq!(resp.rows.len(), 1);
}

/// SQL query on logs table.
#[tokio::test]
async fn test_sql_logs_table() {
    let server = ServerGuard::start().await;
    ingest_test_logs(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .sql_query(SqlQueryRequest {
            query: "SELECT * FROM logs".into(),
        })
        .await
        .expect("sql query on logs")
        .into_inner();

    assert_eq!(resp.rows.len(), 2, "expected 2 log rows");
}

/// SQL query on metrics table.
#[tokio::test]
async fn test_sql_metrics_table() {
    let server = ServerGuard::start().await;
    ingest_test_metrics(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .sql_query(SqlQueryRequest {
            query: "SELECT * FROM metrics".into(),
        })
        .await
        .expect("sql query on metrics")
        .into_inner();

    assert_eq!(resp.rows.len(), 2, "expected 2 metric rows");
}

// ===========================================================================
// Latency query tests
// ===========================================================================

/// Verify the SQL query underlying `motel latency` returns duration data.
#[tokio::test]
async fn test_latency_sql_query() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .sql_query(SqlQueryRequest {
            query: "SELECT duration_ns FROM traces WHERE span_name = 'span-alpha' ORDER BY duration_ns ASC".into(),
        })
        .await
        .expect("latency sql query")
        .into_inner();

    assert!(
        !resp.rows.is_empty(),
        "Should find duration data for span-alpha"
    );
    // Each row should have exactly one value (duration_ns)
    for row in &resp.rows {
        assert_eq!(row.values.len(), 1, "expected single duration_ns column");
        // The value should be parseable as an integer
        let val: i64 = row.values[0]
            .parse()
            .expect("duration_ns should be an integer");
        assert!(val >= 0, "duration should be non-negative");
    }
}

// ===========================================================================
// Clear tests
// ===========================================================================

/// Clear traces and verify the count goes to zero.
#[tokio::test]
async fn test_clear_traces() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    // Verify data exists first.
    let status = client
        .status(StatusRequest {})
        .await
        .expect("status")
        .into_inner();
    assert!(status.trace_count > 0);

    // Clear traces.
    let clear_resp = client
        .clear_traces(ClearRequest {})
        .await
        .expect("clear traces")
        .into_inner();
    assert!(clear_resp.cleared_count > 0);

    // Verify traces are gone.
    let status = client
        .status(StatusRequest {})
        .await
        .expect("status after clear")
        .into_inner();
    assert_eq!(status.trace_count, 0);
    assert_eq!(status.span_count, 0);
}

/// Clear logs and verify the count goes to zero.
#[tokio::test]
async fn test_clear_logs() {
    let server = ServerGuard::start().await;
    ingest_test_logs(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let clear_resp = client
        .clear_logs(ClearRequest {})
        .await
        .expect("clear logs")
        .into_inner();
    assert!(clear_resp.cleared_count > 0);

    let status = client
        .status(StatusRequest {})
        .await
        .expect("status after clear")
        .into_inner();
    assert_eq!(status.log_count, 0);
}

/// Clear metrics and verify the count goes to zero.
#[tokio::test]
async fn test_clear_metrics() {
    let server = ServerGuard::start().await;
    ingest_test_metrics(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let clear_resp = client
        .clear_metrics(ClearRequest {})
        .await
        .expect("clear metrics")
        .into_inner();
    assert!(clear_resp.cleared_count > 0);

    let status = client
        .status(StatusRequest {})
        .await
        .expect("status after clear")
        .into_inner();
    assert_eq!(status.metric_count, 0);
}

/// Clear all data at once.
#[tokio::test]
async fn test_clear_all() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;
    ingest_test_logs(&server).await;
    ingest_test_metrics(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    // Verify data exists.
    let status = client
        .status(StatusRequest {})
        .await
        .expect("status")
        .into_inner();
    assert!(status.trace_count > 0);
    assert!(status.log_count > 0);
    assert!(status.metric_count > 0);

    // Clear all.
    let clear_resp = client
        .clear_all(ClearRequest {})
        .await
        .expect("clear all")
        .into_inner();
    assert!(clear_resp.cleared_count > 0);

    // Verify everything is gone.
    let status = client
        .status(StatusRequest {})
        .await
        .expect("status after clear all")
        .into_inner();
    assert_eq!(status.trace_count, 0);
    assert_eq!(status.span_count, 0);
    assert_eq!(status.log_count, 0);
    assert_eq!(status.metric_count, 0);
}

// ===========================================================================
// Status tests
// ===========================================================================

/// Status reflects correct counts after mixed ingestion.
#[tokio::test]
async fn test_status_counts() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;
    ingest_test_logs(&server).await;
    ingest_test_metrics(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let status = client
        .status(StatusRequest {})
        .await
        .expect("status")
        .into_inner();

    assert_eq!(status.trace_count, 3);
    assert_eq!(status.span_count, 3);
    assert_eq!(status.log_count, 2);
    assert_eq!(status.metric_count, 2);
}

// ===========================================================================
// Follow (streaming) tests
// ===========================================================================

/// Follow traces: start a follow stream, send data, verify the stream receives it.
#[tokio::test]
async fn test_follow_traces() {
    let server = ServerGuard::start().await;

    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    // Start the follow stream.
    let mut stream = query_client
        .follow_traces(FollowRequest {})
        .await
        .expect("follow traces")
        .into_inner();

    // Now send a trace — the follow stream should receive it.
    let mut trace_client = TraceServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect trace client");

    trace_client
        .export(common::make_export_trace_request(
            &[10u8; 16],
            "followed-span",
        ))
        .await
        .expect("export trace");

    // Read from the stream with a timeout.
    let msg = tokio::time::timeout(std::time::Duration::from_secs(5), stream.message())
        .await
        .expect("timeout waiting for follow response")
        .expect("stream error")
        .expect("stream ended unexpectedly");

    assert_eq!(msg.resource_spans.len(), 1);
    assert_eq!(
        msg.resource_spans[0].scope_spans[0].spans[0].name,
        "followed-span"
    );
}

/// Follow logs: start a follow stream, send a log, verify the stream receives it.
#[tokio::test]
async fn test_follow_logs() {
    let server = ServerGuard::start().await;

    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let mut stream = query_client
        .follow_logs(FollowRequest {})
        .await
        .expect("follow logs")
        .into_inner();

    let mut logs_client = LogsServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect logs client");

    logs_client
        .export(common::make_export_logs_request("followed log message"))
        .await
        .expect("export log");

    let msg = tokio::time::timeout(std::time::Duration::from_secs(5), stream.message())
        .await
        .expect("timeout waiting for follow response")
        .expect("stream error")
        .expect("stream ended unexpectedly");

    assert_eq!(msg.resource_logs.len(), 1);
}

/// Follow metrics: start a follow stream, send a metric, verify the stream receives it.
#[tokio::test]
async fn test_follow_metrics() {
    let server = ServerGuard::start().await;

    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let mut stream = query_client
        .follow_metrics(FollowRequest {})
        .await
        .expect("follow metrics")
        .into_inner();

    let mut metrics_client = MetricsServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect metrics client");

    metrics_client
        .export(common::make_export_metrics_request("followed.metric", 99.0))
        .await
        .expect("export metric");

    let msg = tokio::time::timeout(std::time::Duration::from_secs(5), stream.message())
        .await
        .expect("timeout waiting for follow response")
        .expect("stream error")
        .expect("stream ended unexpectedly");

    assert_eq!(msg.resource_metrics.len(), 1);
}

// ===========================================================================
// Sampling tests
// ===========================================================================

/// Server with --sample-rate 0.0 drops all traces but status reports them.
#[tokio::test]
async fn test_server_sample_rate_zero_drops_all() {
    let server = ServerGuard::start_with_args(&["--sample-rate", "0.0"]).await;

    // Ingest traces
    ingest_test_traces(&server).await;

    // Also ingest logs to verify they are NOT sampled
    ingest_test_logs(&server).await;

    // Check status
    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .status(StatusRequest {})
        .await
        .expect("status")
        .into_inner();

    assert_eq!(resp.trace_count, 0, "all traces should be dropped");
    assert_eq!(resp.span_count, 0, "all spans should be dropped");
    assert!(resp.traces_dropped > 0, "should report dropped spans");
    assert!(
        (resp.sample_rate - 0.0).abs() < f64::EPSILON,
        "sample_rate should be 0.0"
    );
    // Logs should NOT be sampled
    assert_eq!(resp.log_count, 2, "logs should not be sampled");
}

/// Server with default sample rate (1.0) keeps all traces.
#[tokio::test]
async fn test_server_default_sample_rate_keeps_all() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let mut client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = client
        .status(StatusRequest {})
        .await
        .expect("status")
        .into_inner();

    assert_eq!(resp.trace_count, 3, "all traces should be kept");
    assert_eq!(resp.traces_dropped, 0, "nothing should be dropped");
    assert!(
        (resp.sample_rate - 1.0).abs() < f64::EPSILON,
        "sample_rate should be 1.0"
    );
}
