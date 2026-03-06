mod common;

use std::time::Duration;

use common::otel::collector::{
    logs::v1::logs_service_client::LogsServiceClient,
    metrics::v1::metrics_service_client::MetricsServiceClient,
    trace::v1::trace_service_client::TraceServiceClient,
};
use common::query_proto::StatusRequest;
use common::query_proto::query_service_client::QueryServiceClient;
use common::{
    ServerGuard, make_export_logs_request, make_export_metrics_request, make_export_trace_request,
};

/// Test that traces are forwarded from a proxy server to an upstream server.
#[tokio::test]
async fn test_forwarding_traces() {
    // Start upstream server (receives forwarded data)
    let upstream = ServerGuard::start().await;

    // Start proxy server with --forward-to pointing at upstream's gRPC port
    let proxy = ServerGuard::start_with_args(&[&format!(
        "--forward-to=http://127.0.0.1:{}",
        upstream.grpc_port
    )])
    .await;

    // Send traces to proxy
    let mut client = TraceServiceClient::connect(proxy.grpc_addr())
        .await
        .unwrap();
    client
        .export(make_export_trace_request(&[1u8; 16], "forwarded-span"))
        .await
        .unwrap();

    // Wait for forwarding to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify data in proxy (server A)
    let mut query_a = QueryServiceClient::connect(proxy.query_addr())
        .await
        .unwrap();
    let status_a = query_a.status(StatusRequest {}).await.unwrap().into_inner();
    assert_eq!(status_a.span_count, 1, "proxy should have 1 span");

    // Verify data in upstream (server B)
    let mut query_b = QueryServiceClient::connect(upstream.query_addr())
        .await
        .unwrap();
    let status_b = query_b.status(StatusRequest {}).await.unwrap().into_inner();
    assert_eq!(status_b.span_count, 1, "upstream should have 1 span");
}

/// Test that logs are forwarded from a proxy server to an upstream server.
#[tokio::test]
async fn test_forwarding_logs() {
    let upstream = ServerGuard::start().await;
    let proxy = ServerGuard::start_with_args(&[&format!(
        "--forward-to=http://127.0.0.1:{}",
        upstream.grpc_port
    )])
    .await;

    let mut client = LogsServiceClient::connect(proxy.grpc_addr()).await.unwrap();
    client
        .export(make_export_logs_request("forwarded log message"))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut query_b = QueryServiceClient::connect(upstream.query_addr())
        .await
        .unwrap();
    let status_b = query_b.status(StatusRequest {}).await.unwrap().into_inner();
    assert_eq!(status_b.log_count, 1, "upstream should have 1 log");
}

/// Test that metrics are forwarded from a proxy server to an upstream server.
#[tokio::test]
async fn test_forwarding_metrics() {
    let upstream = ServerGuard::start().await;
    let proxy = ServerGuard::start_with_args(&[&format!(
        "--forward-to=http://127.0.0.1:{}",
        upstream.grpc_port
    )])
    .await;

    let mut client = MetricsServiceClient::connect(proxy.grpc_addr())
        .await
        .unwrap();
    client
        .export(make_export_metrics_request("forwarded.metric", 42.0))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut query_b = QueryServiceClient::connect(upstream.query_addr())
        .await
        .unwrap();
    let status_b = query_b.status(StatusRequest {}).await.unwrap().into_inner();
    assert_eq!(status_b.metric_count, 1, "upstream should have 1 metric");
}

/// Test that forwarding failure to a dead endpoint does not block ingestion.
#[tokio::test]
async fn test_forwarding_failure_does_not_block() {
    // Start proxy with forward-to pointing at non-existent endpoint
    let proxy = ServerGuard::start_with_args(&["--forward-to=http://127.0.0.1:19999"]).await;

    // Send traces — should succeed (forwarding failure logged but not blocking)
    let mut client = TraceServiceClient::connect(proxy.grpc_addr())
        .await
        .unwrap();
    client
        .export(make_export_trace_request(&[2u8; 16], "test-span"))
        .await
        .unwrap();

    // Verify data stored locally
    let mut query = QueryServiceClient::connect(proxy.query_addr())
        .await
        .unwrap();
    let status = query.status(StatusRequest {}).await.unwrap().into_inner();
    assert_eq!(status.span_count, 1, "proxy should have stored 1 span");
}
