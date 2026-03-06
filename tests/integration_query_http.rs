// Integration tests for the motel HTTP/JSON query API.
//
// Tests cover: GET /api/traces, GET /api/logs, GET /api/metrics,
// POST /api/sql, GET /api/status, and POST /api/clear/* endpoints.

mod common;

use common::ServerGuard;
use common::otel::collector::logs::v1::logs_service_client::LogsServiceClient;
use common::otel::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use common::otel::collector::trace::v1::trace_service_client::TraceServiceClient;

// ---------------------------------------------------------------------------
// Helper: ingest test data into a running server via gRPC
// ---------------------------------------------------------------------------

async fn ingest_test_traces(server: &ServerGuard) {
    let mut client = TraceServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect trace client");

    // Two spans from test-service, one from other-service
    client
        .export(common::make_export_trace_request(&[1u8; 16], "span-alpha"))
        .await
        .expect("export trace 1");

    client
        .export(common::make_export_trace_request(&[2u8; 16], "span-beta"))
        .await
        .expect("export trace 2");

    // Third trace with a different service name
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
// GET /api/traces
// ===========================================================================

#[tokio::test]
async fn test_http_query_traces() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let url = format!("{}/api/traces", server.http_base_url());
    let resp = reqwest::get(&url).await.unwrap();
    assert_eq!(resp.status(), 200);

    let spans: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(spans.len(), 3, "expected 3 spans");
}

#[tokio::test]
async fn test_http_query_traces_with_service_filter() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let url = format!("{}/api/traces?service=test-service", server.http_base_url());
    let resp = reqwest::get(&url).await.unwrap();
    let spans: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(spans.len(), 2, "expected 2 spans from test-service");
}

#[tokio::test]
async fn test_http_query_traces_with_span_name_filter() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let url = format!("{}/api/traces?span_name=span-alpha", server.http_base_url());
    let resp = reqwest::get(&url).await.unwrap();
    let spans: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0]["name"], "span-alpha");
}

#[tokio::test]
async fn test_http_query_traces_with_trace_id_filter() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let trace_id_hex = hex::encode([2u8; 16]);
    let url = format!(
        "{}/api/traces?trace_id={}",
        server.http_base_url(),
        trace_id_hex
    );
    let resp = reqwest::get(&url).await.unwrap();
    let spans: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0]["name"], "span-beta");
}

#[tokio::test]
async fn test_http_query_traces_with_limit() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let url = format!("{}/api/traces?limit=1", server.http_base_url());
    let resp = reqwest::get(&url).await.unwrap();
    let spans: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(spans.len(), 1);
}

// ===========================================================================
// GET /api/logs
// ===========================================================================

#[tokio::test]
async fn test_http_query_logs() {
    let server = ServerGuard::start().await;
    ingest_test_logs(&server).await;

    let url = format!("{}/api/logs", server.http_base_url());
    let resp = reqwest::get(&url).await.unwrap();
    assert_eq!(resp.status(), 200);

    let logs: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(logs.len(), 2, "expected 2 log records");
}

#[tokio::test]
async fn test_http_query_logs_with_body_filter() {
    let server = ServerGuard::start().await;
    ingest_test_logs(&server).await;

    let url = format!("{}/api/logs?body=started", server.http_base_url());
    let resp = reqwest::get(&url).await.unwrap();
    let logs: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(logs.len(), 1);
}

#[tokio::test]
async fn test_http_query_logs_with_service_filter() {
    let server = ServerGuard::start().await;
    ingest_test_logs(&server).await;

    let url = format!("{}/api/logs?service=test-service", server.http_base_url());
    let resp = reqwest::get(&url).await.unwrap();
    let logs: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(logs.len(), 2);

    // Non-existent service returns empty
    let url = format!("{}/api/logs?service=nonexistent", server.http_base_url());
    let resp = reqwest::get(&url).await.unwrap();
    let logs: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(logs.len(), 0);
}

// ===========================================================================
// GET /api/metrics
// ===========================================================================

#[tokio::test]
async fn test_http_query_metrics() {
    let server = ServerGuard::start().await;
    ingest_test_metrics(&server).await;

    let url = format!("{}/api/metrics", server.http_base_url());
    let resp = reqwest::get(&url).await.unwrap();
    assert_eq!(resp.status(), 200);

    let metrics: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(metrics.len(), 2, "expected 2 metric records");
}

#[tokio::test]
async fn test_http_query_metrics_with_name_filter() {
    let server = ServerGuard::start().await;
    ingest_test_metrics(&server).await;

    let url = format!("{}/api/metrics?name=cpu.usage", server.http_base_url());
    let resp = reqwest::get(&url).await.unwrap();
    let metrics: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0]["name"], "cpu.usage");
}

// ===========================================================================
// POST /api/sql
// ===========================================================================

#[tokio::test]
async fn test_http_sql_query() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let url = format!("{}/api/sql", server.http_base_url());
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&serde_json::json!({"query": "SELECT COUNT(*) as cnt FROM traces"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let result: serde_json::Value = resp.json().await.unwrap();
    assert!(!result["columns"].as_array().unwrap().is_empty());
    assert!(!result["rows"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_http_sql_select_all() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let url = format!("{}/api/sql", server.http_base_url());
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&serde_json::json!({"query": "SELECT * FROM traces"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result["rows"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn test_http_sql_invalid_query() {
    let server = ServerGuard::start().await;

    let url = format!("{}/api/sql", server.http_base_url());
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&serde_json::json!({"query": "INVALID SQL HERE"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

// ===========================================================================
// GET /api/status
// ===========================================================================

#[tokio::test]
async fn test_http_status() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;
    ingest_test_logs(&server).await;
    ingest_test_metrics(&server).await;

    let url = format!("{}/api/status", server.http_base_url());
    let resp = reqwest::get(&url).await.unwrap();
    assert_eq!(resp.status(), 200);

    let status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status["trace_count"], 3);
    assert_eq!(status["span_count"], 3);
    assert_eq!(status["log_count"], 2);
    assert_eq!(status["metric_count"], 2);
}

#[tokio::test]
async fn test_http_status_empty() {
    let server = ServerGuard::start().await;

    let url = format!("{}/api/status", server.http_base_url());
    let resp = reqwest::get(&url).await.unwrap();
    let status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status["trace_count"], 0);
    assert_eq!(status["span_count"], 0);
    assert_eq!(status["log_count"], 0);
    assert_eq!(status["metric_count"], 0);
}

// ===========================================================================
// POST /api/clear/*
// ===========================================================================

#[tokio::test]
async fn test_http_clear_traces() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;

    let client = reqwest::Client::new();
    let url = format!("{}/api/clear/traces", server.http_base_url());
    let resp = client.post(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let result: serde_json::Value = resp.json().await.unwrap();
    assert!(result["cleared_count"].as_i64().unwrap() > 0);

    // Verify traces are gone
    let status_url = format!("{}/api/status", server.http_base_url());
    let status: serde_json::Value = reqwest::get(&status_url)
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["trace_count"], 0);
    assert_eq!(status["span_count"], 0);
}

#[tokio::test]
async fn test_http_clear_logs() {
    let server = ServerGuard::start().await;
    ingest_test_logs(&server).await;

    let client = reqwest::Client::new();
    let url = format!("{}/api/clear/logs", server.http_base_url());
    let resp = client.post(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let result: serde_json::Value = resp.json().await.unwrap();
    assert!(result["cleared_count"].as_i64().unwrap() > 0);

    let status_url = format!("{}/api/status", server.http_base_url());
    let status: serde_json::Value = reqwest::get(&status_url)
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["log_count"], 0);
}

#[tokio::test]
async fn test_http_clear_metrics() {
    let server = ServerGuard::start().await;
    ingest_test_metrics(&server).await;

    let client = reqwest::Client::new();
    let url = format!("{}/api/clear/metrics", server.http_base_url());
    let resp = client.post(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let result: serde_json::Value = resp.json().await.unwrap();
    assert!(result["cleared_count"].as_i64().unwrap() > 0);

    let status_url = format!("{}/api/status", server.http_base_url());
    let status: serde_json::Value = reqwest::get(&status_url)
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["metric_count"], 0);
}

#[tokio::test]
async fn test_http_clear_all() {
    let server = ServerGuard::start().await;
    ingest_test_traces(&server).await;
    ingest_test_logs(&server).await;
    ingest_test_metrics(&server).await;

    let client = reqwest::Client::new();
    let url = format!("{}/api/clear/all", server.http_base_url());
    let resp = client.post(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let result: serde_json::Value = resp.json().await.unwrap();
    assert!(result["cleared_count"].as_i64().unwrap() > 0);

    let status_url = format!("{}/api/status", server.http_base_url());
    let status: serde_json::Value = reqwest::get(&status_url)
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["trace_count"], 0);
    assert_eq!(status["span_count"], 0);
    assert_eq!(status["log_count"], 0);
    assert_eq!(status["metric_count"], 0);
}
