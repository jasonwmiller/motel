mod common;

use std::io::Write;

use common::query_proto::StatusRequest;
use common::query_proto::query_service_client::QueryServiceClient;
use prost::Message;
use tempfile::NamedTempFile;

/// Import JSONL traces from a file and verify they appear in the store.
#[tokio::test]
async fn test_import_jsonl_traces() {
    let server = common::ServerGuard::start().await;

    // Create a temp JSONL file with trace data
    let mut file = NamedTempFile::with_suffix(".jsonl").unwrap();
    for i in 0..5 {
        let line = serde_json::json!({
            "time": "2024-01-15T10:30:00.000Z",
            "service": "test-svc",
            "span_name": format!("span-{}", i),
            "duration_ms": 10.0 + i as f64,
            "trace_id": format!("{:032x}", i + 1),
            "span_id": format!("{:016x}", i + 1),
            "status": "Ok"
        });
        writeln!(file, "{}", serde_json::to_string(&line).unwrap()).unwrap();
    }
    file.flush().unwrap();

    // Run import via CLI subprocess
    let bin = env!("CARGO_BIN_EXE_motel");
    let output = std::process::Command::new(bin)
        .args([
            "import",
            file.path().to_str().unwrap(),
            "--signal",
            "traces",
            "--addr",
            &server.grpc_addr(),
        ])
        .output()
        .expect("failed to run import");

    assert!(
        output.status.success(),
        "import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify data via status
    let mut query = QueryServiceClient::connect(server.query_addr())
        .await
        .unwrap();
    let status = query.status(StatusRequest {}).await.unwrap().into_inner();
    assert_eq!(status.span_count, 5);
}

/// Import JSONL logs from a file and verify they appear in the store.
#[tokio::test]
async fn test_import_jsonl_logs() {
    let server = common::ServerGuard::start().await;

    let mut file = NamedTempFile::with_suffix(".jsonl").unwrap();
    for i in 0..3 {
        let line = serde_json::json!({
            "time": "2024-01-15T10:30:00.000Z",
            "service": "test-svc",
            "severity": "SeverityNumberInfo",
            "body": format!("log message {}", i),
        });
        writeln!(file, "{}", serde_json::to_string(&line).unwrap()).unwrap();
    }
    file.flush().unwrap();

    let bin = env!("CARGO_BIN_EXE_motel");
    let output = std::process::Command::new(bin)
        .args([
            "import",
            file.path().to_str().unwrap(),
            "--signal",
            "logs",
            "--addr",
            &server.grpc_addr(),
        ])
        .output()
        .expect("failed to run import");

    assert!(
        output.status.success(),
        "import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut query = QueryServiceClient::connect(server.query_addr())
        .await
        .unwrap();
    let status = query.status(StatusRequest {}).await.unwrap().into_inner();
    assert_eq!(status.log_count, 3);
}

/// Import OTLP protobuf traces from a file.
#[tokio::test]
async fn test_import_proto_traces() {
    let server = common::ServerGuard::start().await;

    // Create protobuf file
    let rs = common::make_resource_spans(&[1u8; 16], "proto-span");
    let request = common::otel::collector::trace::v1::ExportTraceServiceRequest {
        resource_spans: vec![rs],
    };
    let bytes = request.encode_to_vec();

    let mut file = NamedTempFile::with_suffix(".pb").unwrap();
    file.write_all(&bytes).unwrap();
    file.flush().unwrap();

    let bin = env!("CARGO_BIN_EXE_motel");
    let output = std::process::Command::new(bin)
        .args([
            "import",
            file.path().to_str().unwrap(),
            "--format",
            "otlp-proto",
            "--signal",
            "traces",
            "--addr",
            &server.grpc_addr(),
        ])
        .output()
        .expect("failed to run import");

    assert!(
        output.status.success(),
        "import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut query = QueryServiceClient::connect(server.query_addr())
        .await
        .unwrap();
    let status = query.status(StatusRequest {}).await.unwrap().into_inner();
    assert_eq!(status.span_count, 1);
}

/// Import OTLP protobuf logs from a file.
#[tokio::test]
async fn test_import_proto_logs() {
    let server = common::ServerGuard::start().await;

    let rl = common::make_resource_logs("proto log message");
    let request = common::otel::collector::logs::v1::ExportLogsServiceRequest {
        resource_logs: vec![rl],
    };
    let bytes = request.encode_to_vec();

    let mut file = NamedTempFile::with_suffix(".pb").unwrap();
    file.write_all(&bytes).unwrap();
    file.flush().unwrap();

    let bin = env!("CARGO_BIN_EXE_motel");
    let output = std::process::Command::new(bin)
        .args([
            "import",
            file.path().to_str().unwrap(),
            "--format",
            "otlp-proto",
            "--signal",
            "logs",
            "--addr",
            &server.grpc_addr(),
        ])
        .output()
        .expect("failed to run import");

    assert!(
        output.status.success(),
        "import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut query = QueryServiceClient::connect(server.query_addr())
        .await
        .unwrap();
    let status = query.status(StatusRequest {}).await.unwrap().into_inner();
    assert_eq!(status.log_count, 1);
}

/// Import OTLP protobuf metrics from a file.
#[tokio::test]
async fn test_import_proto_metrics() {
    let server = common::ServerGuard::start().await;

    let rm = common::make_resource_metrics("test.metric", 42.0);
    let request = common::otel::collector::metrics::v1::ExportMetricsServiceRequest {
        resource_metrics: vec![rm],
    };
    let bytes = request.encode_to_vec();

    let mut file = NamedTempFile::with_suffix(".pb").unwrap();
    file.write_all(&bytes).unwrap();
    file.flush().unwrap();

    let bin = env!("CARGO_BIN_EXE_motel");
    let output = std::process::Command::new(bin)
        .args([
            "import",
            file.path().to_str().unwrap(),
            "--format",
            "otlp-proto",
            "--signal",
            "metrics",
            "--addr",
            &server.grpc_addr(),
        ])
        .output()
        .expect("failed to run import");

    assert!(
        output.status.success(),
        "import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut query = QueryServiceClient::connect(server.query_addr())
        .await
        .unwrap();
    let status = query.status(StatusRequest {}).await.unwrap().into_inner();
    assert_eq!(status.metric_count, 1);
}
