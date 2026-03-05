// Integration tests for OTLP HTTP ingestion.
//
// Each test starts its own motel server, sends protobuf-encoded data via
// HTTP POST to /v1/traces, /v1/logs, /v1/metrics, and verifies storage
// via the query gRPC Status RPC.

mod common;

use common::ServerGuard;
use common::query_proto::StatusRequest;
use common::query_proto::query_service_client::QueryServiceClient;

const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";

/// Send traces via HTTP POST /v1/traces and verify via Status.
#[tokio::test]
async fn test_send_traces_via_http() {
    let server = ServerGuard::start().await;

    let request = common::make_export_trace_request(&[1u8; 16], "http-test-span");
    let body = common::encode_proto(&request);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v1/traces", server.http_base_url()))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .body(body)
        .send()
        .await
        .expect("failed to send HTTP request");

    assert!(
        resp.status().is_success(),
        "expected 200 OK, got {}",
        resp.status()
    );

    // Verify via Status.
    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("failed to connect query client");

    let status = query_client
        .status(StatusRequest {})
        .await
        .expect("failed to get status")
        .into_inner();

    assert_eq!(status.trace_count, 1, "expected 1 trace");
    assert_eq!(status.span_count, 1, "expected 1 span");
}

/// Send logs via HTTP POST /v1/logs and verify via Status.
#[tokio::test]
async fn test_send_logs_via_http() {
    let server = ServerGuard::start().await;

    let request = common::make_export_logs_request("hello from http test");
    let body = common::encode_proto(&request);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v1/logs", server.http_base_url()))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .body(body)
        .send()
        .await
        .expect("failed to send HTTP request");

    assert!(
        resp.status().is_success(),
        "expected 200 OK, got {}",
        resp.status()
    );

    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("failed to connect query client");

    let status = query_client
        .status(StatusRequest {})
        .await
        .expect("failed to get status")
        .into_inner();

    assert_eq!(status.log_count, 1, "expected 1 log record");
}

/// Send metrics via HTTP POST /v1/metrics and verify via Status.
#[tokio::test]
async fn test_send_metrics_via_http() {
    let server = ServerGuard::start().await;

    let request = common::make_export_metrics_request("memory.usage", 1024.0);
    let body = common::encode_proto(&request);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v1/metrics", server.http_base_url()))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .body(body)
        .send()
        .await
        .expect("failed to send HTTP request");

    assert!(
        resp.status().is_success(),
        "expected 200 OK, got {}",
        resp.status()
    );

    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("failed to connect query client");

    let status = query_client
        .status(StatusRequest {})
        .await
        .expect("failed to get status")
        .into_inner();

    assert_eq!(status.metric_count, 1, "expected 1 metric");
}

/// Send multiple traces via HTTP and verify all are stored.
#[tokio::test]
async fn test_send_multiple_traces_via_http() {
    let server = ServerGuard::start().await;

    let client = reqwest::Client::new();

    for i in 0..5u8 {
        let request = common::make_export_trace_request(&[i + 1; 16], &format!("http-span-{i}"));
        let body = common::encode_proto(&request);

        let resp = client
            .post(format!("{}/v1/traces", server.http_base_url()))
            .header("Content-Type", CONTENT_TYPE_PROTOBUF)
            .body(body)
            .send()
            .await
            .expect("failed to send HTTP request");

        assert!(resp.status().is_success());
    }

    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("failed to connect query client");

    let status = query_client
        .status(StatusRequest {})
        .await
        .expect("failed to get status")
        .into_inner();

    assert_eq!(status.trace_count, 5);
    assert_eq!(status.span_count, 5);
}

/// Verify that sending an empty body returns an error or is handled gracefully.
#[tokio::test]
async fn test_empty_body_returns_error_or_empty() {
    let server = ServerGuard::start().await;

    let client = reqwest::Client::new();

    // An empty protobuf body decodes as an empty ExportTraceServiceRequest
    // (all fields default), which has zero resource_spans. The server should
    // accept it (prost decodes empty bytes as default message).
    let resp = client
        .post(format!("{}/v1/traces", server.http_base_url()))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .body(Vec::<u8>::new())
        .send()
        .await
        .expect("failed to send HTTP request");

    // Empty protobuf decodes as default message, so server should return 200.
    assert!(
        resp.status().is_success(),
        "expected success for empty protobuf body, got {}",
        resp.status()
    );

    // Verify nothing was stored.
    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("failed to connect query client");

    let status = query_client
        .status(StatusRequest {})
        .await
        .expect("failed to get status")
        .into_inner();

    assert_eq!(status.trace_count, 0);
    assert_eq!(status.span_count, 0);
}
