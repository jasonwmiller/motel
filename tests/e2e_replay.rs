// End-to-end integration tests for the `motel replay` command.
//
// These tests spin up two motel server instances and verify that data can be
// replayed from one to the other via the OTLP gRPC protocol.

mod common;

use common::ServerGuard;
use common::otel::collector::logs::v1::logs_service_client::LogsServiceClient;
use common::otel::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use common::otel::collector::trace::v1::trace_service_client::TraceServiceClient;
use common::query_proto::query_service_client::QueryServiceClient;
use common::query_proto::{
    QueryLogsRequest, QueryMetricsRequest, QueryTracesRequest, StatusRequest,
};
use std::process::Command;

/// Helper to run the replay binary command.
fn replay_command(source_query_addr: &str, target_grpc_addr: &str) -> std::process::Output {
    let bin = env!("CARGO_BIN_EXE_motel");
    Command::new(bin)
        .args([
            "replay",
            "--addr",
            source_query_addr,
            "--target",
            target_grpc_addr,
        ])
        .output()
        .expect("failed to run replay command")
}

fn replay_command_with_signal(
    source_query_addr: &str,
    target_grpc_addr: &str,
    signal: &str,
) -> std::process::Output {
    let bin = env!("CARGO_BIN_EXE_motel");
    Command::new(bin)
        .args([
            "replay",
            "--addr",
            source_query_addr,
            "--target",
            target_grpc_addr,
            "--signal",
            signal,
        ])
        .output()
        .expect("failed to run replay command")
}

fn replay_command_dry_run(source_query_addr: &str, target_grpc_addr: &str) -> std::process::Output {
    let bin = env!("CARGO_BIN_EXE_motel");
    Command::new(bin)
        .args([
            "replay",
            "--addr",
            source_query_addr,
            "--target",
            target_grpc_addr,
            "--dry-run",
        ])
        .output()
        .expect("failed to run replay command")
}

/// Replay traces from one server to another and verify they arrive.
#[tokio::test]
async fn test_replay_traces() {
    let source = ServerGuard::start().await;
    let target = ServerGuard::start().await;

    // Ingest traces into source.
    let mut trace_client = TraceServiceClient::connect(source.grpc_addr())
        .await
        .expect("connect trace client");

    trace_client
        .export(common::make_export_trace_request(
            &[1u8; 16],
            "replay-span-1",
        ))
        .await
        .expect("export trace 1");

    trace_client
        .export(common::make_export_trace_request(
            &[2u8; 16],
            "replay-span-2",
        ))
        .await
        .expect("export trace 2");

    // Verify source has traces.
    let mut source_query = QueryServiceClient::connect(source.query_addr())
        .await
        .expect("connect source query");
    let status = source_query
        .status(StatusRequest {})
        .await
        .expect("source status")
        .into_inner();
    assert_eq!(status.trace_count, 2);

    // Run replay.
    let output = replay_command(&source.query_addr(), &target.grpc_addr());
    assert!(
        output.status.success(),
        "replay failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("2 trace batches"),
        "expected trace count in output: {stdout}"
    );

    // Verify target has the traces.
    let mut target_query = QueryServiceClient::connect(target.query_addr())
        .await
        .expect("connect target query");
    let status = target_query
        .status(StatusRequest {})
        .await
        .expect("target status")
        .into_inner();
    assert_eq!(status.trace_count, 2);
    assert_eq!(status.span_count, 2);

    // Verify span names.
    let resp = target_query
        .query_traces(QueryTracesRequest {
            span_name: "replay-span-1".into(),
            ..Default::default()
        })
        .await
        .expect("query target traces")
        .into_inner();
    assert_eq!(resp.resource_spans.len(), 1);
}

/// Verify --signal traces only replays traces, not logs or metrics.
#[tokio::test]
async fn test_replay_signal_filter() {
    let source = ServerGuard::start().await;
    let target = ServerGuard::start().await;

    // Ingest traces, logs, and metrics into source.
    let mut trace_client = TraceServiceClient::connect(source.grpc_addr())
        .await
        .expect("connect trace client");
    trace_client
        .export(common::make_export_trace_request(&[1u8; 16], "sig-span"))
        .await
        .expect("export trace");

    let mut logs_client = LogsServiceClient::connect(source.grpc_addr())
        .await
        .expect("connect logs client");
    logs_client
        .export(common::make_export_logs_request("sig-log"))
        .await
        .expect("export log");

    let mut metrics_client = MetricsServiceClient::connect(source.grpc_addr())
        .await
        .expect("connect metrics client");
    metrics_client
        .export(common::make_export_metrics_request("sig.metric", 42.0))
        .await
        .expect("export metric");

    // Replay only traces.
    let output = replay_command_with_signal(&source.query_addr(), &target.grpc_addr(), "traces");
    assert!(
        output.status.success(),
        "replay failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Target should have traces but no logs or metrics.
    let mut target_query = QueryServiceClient::connect(target.query_addr())
        .await
        .expect("connect target query");
    let status = target_query
        .status(StatusRequest {})
        .await
        .expect("target status")
        .into_inner();
    assert_eq!(status.trace_count, 1, "should have 1 trace");
    assert_eq!(status.log_count, 0, "should have 0 logs");
    assert_eq!(status.metric_count, 0, "should have 0 metrics");
}

/// Verify --dry-run prints counts without sending data.
#[tokio::test]
async fn test_replay_dry_run() {
    let source = ServerGuard::start().await;
    let target = ServerGuard::start().await;

    // Ingest data into source.
    let mut trace_client = TraceServiceClient::connect(source.grpc_addr())
        .await
        .expect("connect trace client");
    trace_client
        .export(common::make_export_trace_request(
            &[1u8; 16],
            "dry-run-span",
        ))
        .await
        .expect("export trace");

    let mut logs_client = LogsServiceClient::connect(source.grpc_addr())
        .await
        .expect("connect logs client");
    logs_client
        .export(common::make_export_logs_request("dry-run-log"))
        .await
        .expect("export log");

    // Run dry-run replay.
    let output = replay_command_dry_run(&source.query_addr(), &target.grpc_addr());
    assert!(
        output.status.success(),
        "dry-run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("[dry-run]"),
        "expected [dry-run] prefix: {stdout}"
    );
    assert!(
        stdout.contains("1 trace batches"),
        "expected trace count: {stdout}"
    );

    // Target should have NO data.
    let mut target_query = QueryServiceClient::connect(target.query_addr())
        .await
        .expect("connect target query");
    let status = target_query
        .status(StatusRequest {})
        .await
        .expect("target status")
        .into_inner();
    assert_eq!(status.trace_count, 0, "dry-run should not send traces");
    assert_eq!(status.log_count, 0, "dry-run should not send logs");
    assert_eq!(status.metric_count, 0, "dry-run should not send metrics");
}

/// Replay all signal types (traces, logs, metrics).
#[tokio::test]
async fn test_replay_all_signals() {
    let source = ServerGuard::start().await;
    let target = ServerGuard::start().await;

    // Ingest all signal types into source.
    let mut trace_client = TraceServiceClient::connect(source.grpc_addr())
        .await
        .expect("connect trace client");
    trace_client
        .export(common::make_export_trace_request(&[1u8; 16], "all-span"))
        .await
        .expect("export trace");

    let mut logs_client = LogsServiceClient::connect(source.grpc_addr())
        .await
        .expect("connect logs client");
    logs_client
        .export(common::make_export_logs_request("all-log"))
        .await
        .expect("export log");

    let mut metrics_client = MetricsServiceClient::connect(source.grpc_addr())
        .await
        .expect("connect metrics client");
    metrics_client
        .export(common::make_export_metrics_request("all.metric", 99.0))
        .await
        .expect("export metric");

    // Replay all.
    let output = replay_command(&source.query_addr(), &target.grpc_addr());
    assert!(
        output.status.success(),
        "replay failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify all signals arrived at target.
    let mut target_query = QueryServiceClient::connect(target.query_addr())
        .await
        .expect("connect target query");
    let status = target_query
        .status(StatusRequest {})
        .await
        .expect("target status")
        .into_inner();
    assert_eq!(status.trace_count, 1, "should have 1 trace");
    assert_eq!(status.log_count, 1, "should have 1 log");
    assert_eq!(status.metric_count, 1, "should have 1 metric");

    // Verify data fidelity.
    let traces = target_query
        .query_traces(QueryTracesRequest::default())
        .await
        .expect("query traces")
        .into_inner();
    assert_eq!(
        traces.resource_spans[0].scope_spans[0].spans[0].name,
        "all-span"
    );

    let logs = target_query
        .query_logs(QueryLogsRequest::default())
        .await
        .expect("query logs")
        .into_inner();
    assert_eq!(logs.resource_logs.len(), 1);

    let metrics = target_query
        .query_metrics(QueryMetricsRequest::default())
        .await
        .expect("query metrics")
        .into_inner();
    assert_eq!(metrics.resource_metrics.len(), 1);
}
