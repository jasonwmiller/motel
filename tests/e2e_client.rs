// End-to-end integration tests for motel.
//
// These tests exercise the full pipeline: start a server, ingest data via
// OTLP gRPC and HTTP, query via the query gRPC API, and verify results.

mod common;

use common::ServerGuard;
use common::otel::collector::logs::v1::logs_service_client::LogsServiceClient;
use common::otel::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use common::otel::collector::trace::v1::trace_service_client::TraceServiceClient;
use common::query_proto::query_service_client::QueryServiceClient;
use common::query_proto::{
    ClearRequest, QueryLogsRequest, QueryMetricsRequest, QueryTracesRequest, SqlQueryRequest,
    StatusRequest,
};

const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";

/// Full lifecycle: ingest traces via gRPC and HTTP, query them, clear, verify empty.
#[tokio::test]
async fn test_full_trace_lifecycle() {
    let server = ServerGuard::start().await;

    // 1. Ingest a trace via gRPC.
    let mut trace_client = TraceServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect trace client");

    trace_client
        .export(common::make_export_trace_request(
            &[1u8; 16],
            "grpc-lifecycle-span",
        ))
        .await
        .expect("export trace via gRPC");

    // 2. Ingest a trace via HTTP.
    let http_client = reqwest::Client::new();
    let http_request = common::make_export_trace_request(&[2u8; 16], "http-lifecycle-span");
    let body = common::encode_proto(&http_request);

    let resp = http_client
        .post(format!("{}/v1/traces", server.http_base_url()))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .body(body)
        .send()
        .await
        .expect("send HTTP trace");
    assert!(resp.status().is_success());

    // 3. Query and verify both traces exist.
    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let status = query_client
        .status(StatusRequest {})
        .await
        .expect("status")
        .into_inner();
    assert_eq!(status.trace_count, 2);
    assert_eq!(status.span_count, 2);

    // 4. Query by span name to verify data fidelity.
    let resp = query_client
        .query_traces(QueryTracesRequest {
            span_name: "grpc-lifecycle-span".into(),
            ..Default::default()
        })
        .await
        .expect("query by span name")
        .into_inner();
    assert_eq!(resp.resource_spans.len(), 1);
    assert_eq!(
        resp.resource_spans[0].scope_spans[0].spans[0].name,
        "grpc-lifecycle-span"
    );

    // 5. Clear all and verify empty.
    query_client
        .clear_all(ClearRequest {})
        .await
        .expect("clear all");

    let status = query_client
        .status(StatusRequest {})
        .await
        .expect("status after clear")
        .into_inner();
    assert_eq!(status.trace_count, 0);
    assert_eq!(status.span_count, 0);
}

/// Full lifecycle for logs: ingest, query, clear.
#[tokio::test]
async fn test_full_log_lifecycle() {
    let server = ServerGuard::start().await;

    // Ingest logs via gRPC.
    let mut logs_client = LogsServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect logs client");

    logs_client
        .export(common::make_export_logs_request("e2e log alpha"))
        .await
        .expect("export log 1");

    logs_client
        .export(common::make_export_logs_request("e2e log beta"))
        .await
        .expect("export log 2");

    // Ingest a log via HTTP.
    let http_client = reqwest::Client::new();
    let http_request = common::make_export_logs_request("e2e log gamma via http");
    let body = common::encode_proto(&http_request);

    let resp = http_client
        .post(format!("{}/v1/logs", server.http_base_url()))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .body(body)
        .send()
        .await
        .expect("send HTTP log");
    assert!(resp.status().is_success());

    // Query all logs.
    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = query_client
        .query_logs(QueryLogsRequest::default())
        .await
        .expect("query logs")
        .into_inner();
    assert_eq!(resp.resource_logs.len(), 3);

    // Query by body substring.
    let resp = query_client
        .query_logs(QueryLogsRequest {
            body_contains: "gamma".into(),
            ..Default::default()
        })
        .await
        .expect("query logs by body")
        .into_inner();
    assert_eq!(resp.resource_logs.len(), 1);

    // Clear and verify.
    query_client
        .clear_logs(ClearRequest {})
        .await
        .expect("clear logs");

    let status = query_client
        .status(StatusRequest {})
        .await
        .expect("status")
        .into_inner();
    assert_eq!(status.log_count, 0);
}

/// Full lifecycle for metrics: ingest, query, clear.
#[tokio::test]
async fn test_full_metric_lifecycle() {
    let server = ServerGuard::start().await;

    // Ingest via gRPC.
    let mut metrics_client = MetricsServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect metrics client");

    metrics_client
        .export(common::make_export_metrics_request("e2e.gauge", 100.0))
        .await
        .expect("export metric");

    // Ingest via HTTP.
    let http_client = reqwest::Client::new();
    let http_request = common::make_export_metrics_request("e2e.counter", 200.0);
    let body = common::encode_proto(&http_request);

    let resp = http_client
        .post(format!("{}/v1/metrics", server.http_base_url()))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .body(body)
        .send()
        .await
        .expect("send HTTP metric");
    assert!(resp.status().is_success());

    // Query all metrics.
    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let resp = query_client
        .query_metrics(QueryMetricsRequest::default())
        .await
        .expect("query metrics")
        .into_inner();
    assert_eq!(resp.resource_metrics.len(), 2);

    // Query by name.
    let resp = query_client
        .query_metrics(QueryMetricsRequest {
            metric_name: "e2e.gauge".into(),
            ..Default::default()
        })
        .await
        .expect("query metrics by name")
        .into_inner();
    assert_eq!(resp.resource_metrics.len(), 1);

    // Clear and verify.
    query_client
        .clear_metrics(ClearRequest {})
        .await
        .expect("clear metrics");

    let status = query_client
        .status(StatusRequest {})
        .await
        .expect("status")
        .into_inner();
    assert_eq!(status.metric_count, 0);
}

/// End-to-end SQL query test: ingest mixed data, run various SQL queries.
#[tokio::test]
async fn test_e2e_sql_queries() {
    let server = ServerGuard::start().await;

    // Ingest traces.
    let mut trace_client = TraceServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect trace client");

    for i in 0..5u8 {
        trace_client
            .export(common::make_export_trace_request(
                &[i + 1; 16],
                &format!("sql-span-{i}"),
            ))
            .await
            .expect("export trace");
    }

    // Ingest logs.
    let mut logs_client = LogsServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect logs client");

    for i in 0..3 {
        logs_client
            .export(common::make_export_logs_request(&format!(
                "sql test log {i}"
            )))
            .await
            .expect("export log");
    }

    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    // SELECT COUNT(*) from traces.
    let resp = query_client
        .sql_query(SqlQueryRequest {
            query: "SELECT COUNT(*) as cnt FROM traces".into(),
        })
        .await
        .expect("sql count traces")
        .into_inner();

    assert_eq!(resp.rows.len(), 1, "COUNT should return exactly 1 row");
    // The count value should be "5".
    assert_eq!(resp.rows[0].values[0], "5");

    // SELECT with ORDER BY and LIMIT.
    let resp = query_client
        .sql_query(SqlQueryRequest {
            query: "SELECT span_name FROM traces ORDER BY span_name LIMIT 2".into(),
        })
        .await
        .expect("sql order by limit")
        .into_inner();

    assert_eq!(resp.rows.len(), 2);

    // COUNT on logs table.
    let resp = query_client
        .sql_query(SqlQueryRequest {
            query: "SELECT COUNT(*) as cnt FROM logs".into(),
        })
        .await
        .expect("sql count logs")
        .into_inner();

    assert_eq!(resp.rows.len(), 1);
    assert_eq!(resp.rows[0].values[0], "3");
}

/// Verify that queries return correct data after partial clears.
#[tokio::test]
async fn test_partial_clear_and_query() {
    let server = ServerGuard::start().await;

    // Ingest both traces and logs.
    let mut trace_client = TraceServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect trace client");
    trace_client
        .export(common::make_export_trace_request(
            &[1u8; 16],
            "partial-span",
        ))
        .await
        .expect("export trace");

    let mut logs_client = LogsServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect logs client");
    logs_client
        .export(common::make_export_logs_request("partial clear log"))
        .await
        .expect("export log");

    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    // Clear only traces.
    query_client
        .clear_traces(ClearRequest {})
        .await
        .expect("clear traces");

    // Traces should be gone, logs should remain.
    let status = query_client
        .status(StatusRequest {})
        .await
        .expect("status")
        .into_inner();
    assert_eq!(status.trace_count, 0);
    assert_eq!(status.span_count, 0);
    assert_eq!(status.log_count, 1, "logs should still exist");
}

/// Verify multiple spans with the same trace ID are counted as one trace.
#[tokio::test]
async fn test_same_trace_id_counted_once() {
    let server = ServerGuard::start().await;

    let mut trace_client = TraceServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect trace client");

    // Send two spans with the same trace_id.
    let trace_id = [42u8; 16];
    trace_client
        .export(common::make_export_trace_request(&trace_id, "span-one"))
        .await
        .expect("export span 1");
    trace_client
        .export(common::make_export_trace_request(&trace_id, "span-two"))
        .await
        .expect("export span 2");

    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("connect query client");

    let status = query_client
        .status(StatusRequest {})
        .await
        .expect("status")
        .into_inner();

    // Should be 1 trace (same trace_id) but 2 spans.
    assert_eq!(status.trace_count, 1, "same trace_id = 1 trace");
    assert_eq!(status.span_count, 2, "two spans total");
}
