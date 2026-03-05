// Integration tests for OTLP gRPC ingestion.
//
// Each test starts its own motel server, sends data via OTLP gRPC, and
// verifies storage via the query gRPC Status RPC.

mod common;

use common::ServerGuard;
use common::otel::collector::{
    logs::v1::logs_service_client::LogsServiceClient,
    metrics::v1::metrics_service_client::MetricsServiceClient,
    trace::v1::trace_service_client::TraceServiceClient,
};
use common::query_proto::StatusRequest;
use common::query_proto::query_service_client::QueryServiceClient;

/// Send traces via OTLP gRPC and verify they appear in the store via Status.
#[tokio::test]
async fn test_send_traces_via_grpc() {
    let server = ServerGuard::start().await;

    // Connect a TraceService client to the gRPC OTLP port.
    let mut trace_client = TraceServiceClient::connect(server.grpc_addr())
        .await
        .expect("failed to connect trace client");

    // Send a trace.
    let request = common::make_export_trace_request(&[1u8; 16], "grpc-test-span");
    trace_client
        .export(request)
        .await
        .expect("failed to export traces");

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

/// Send multiple traces with different trace IDs and verify counts.
#[tokio::test]
async fn test_send_multiple_traces_via_grpc() {
    let server = ServerGuard::start().await;

    let mut trace_client = TraceServiceClient::connect(server.grpc_addr())
        .await
        .expect("failed to connect trace client");

    // Send three traces with different trace IDs.
    for i in 0..3u8 {
        let request = common::make_export_trace_request(&[i + 1; 16], &format!("span-{i}"));
        trace_client
            .export(request)
            .await
            .expect("failed to export traces");
    }

    let mut query_client = QueryServiceClient::connect(server.query_addr())
        .await
        .expect("failed to connect query client");

    let status = query_client
        .status(StatusRequest {})
        .await
        .expect("failed to get status")
        .into_inner();

    assert_eq!(status.trace_count, 3);
    assert_eq!(status.span_count, 3);
}

/// Send logs via OTLP gRPC and verify via Status.
#[tokio::test]
async fn test_send_logs_via_grpc() {
    let server = ServerGuard::start().await;

    let mut logs_client = LogsServiceClient::connect(server.grpc_addr())
        .await
        .expect("failed to connect logs client");

    let request = common::make_export_logs_request("hello from grpc test");
    logs_client
        .export(request)
        .await
        .expect("failed to export logs");

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

/// Send metrics via OTLP gRPC and verify via Status.
#[tokio::test]
async fn test_send_metrics_via_grpc() {
    let server = ServerGuard::start().await;

    let mut metrics_client = MetricsServiceClient::connect(server.grpc_addr())
        .await
        .expect("failed to connect metrics client");

    let request = common::make_export_metrics_request("cpu.usage", 42.5);
    metrics_client
        .export(request)
        .await
        .expect("failed to export metrics");

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

/// Verify that an empty server reports all-zero status.
#[tokio::test]
async fn test_empty_server_status() {
    let server = ServerGuard::start().await;

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
    assert_eq!(status.log_count, 0);
    assert_eq!(status.metric_count, 0);
}
