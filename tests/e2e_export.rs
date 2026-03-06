// End-to-end integration tests for the `motel export` command.

mod common;

use common::ServerGuard;
use common::otel::collector::logs::v1::logs_service_client::LogsServiceClient;
use common::otel::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use common::otel::collector::trace::v1::trace_service_client::TraceServiceClient;
use common::otel::trace::v1::ResourceSpans;
use prost::Message;

/// Export traces as JSONL, verify output contains expected spans.
#[tokio::test]
async fn test_export_traces_jsonl() {
    let server = ServerGuard::start().await;

    // Ingest test traces.
    let mut trace_client = TraceServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect trace client");

    trace_client
        .export(common::make_export_trace_request(
            &[1u8; 16],
            "export-span-one",
        ))
        .await
        .expect("export trace 1");

    trace_client
        .export(common::make_export_trace_request(
            &[2u8; 16],
            "export-span-two",
        ))
        .await
        .expect("export trace 2");

    // Run export command.
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_motel"))
        .args([
            "export",
            "traces",
            "-o",
            "jsonl",
            "--addr",
            &server.query_addr(),
        ])
        .output()
        .expect("run export command");

    assert!(output.status.success(), "export should succeed");
    let stdout = String::from_utf8(output.stdout).expect("valid utf8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 2, "should have 2 JSONL lines");

    // Each line should be valid JSON with expected fields.
    for line in &lines {
        let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
        assert_eq!(v["signal"], "trace");
        assert!(v["span_name"].is_string());
        assert!(v["trace_id"].is_string());
        assert_eq!(v["service"], "test-service");
    }
}

/// Export logs as CSV, verify headers and rows.
#[tokio::test]
async fn test_export_logs_csv() {
    let server = ServerGuard::start().await;

    let mut logs_client = LogsServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect logs client");

    logs_client
        .export(common::make_export_logs_request("export log alpha"))
        .await
        .expect("export log 1");

    logs_client
        .export(common::make_export_logs_request("export log beta"))
        .await
        .expect("export log 2");

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_motel"))
        .args([
            "export",
            "logs",
            "-o",
            "csv",
            "--addr",
            &server.query_addr(),
        ])
        .output()
        .expect("run export command");

    assert!(output.status.success(), "export should succeed");
    let stdout = String::from_utf8(output.stdout).expect("valid utf8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 3, "should have header + 2 rows");
    assert!(lines[0].contains("signal"), "header should contain signal");
    assert!(
        lines[0].contains("severity"),
        "header should contain severity"
    );
    assert!(lines[1].contains("log"), "row should contain signal=log");
}

/// Export metrics as text, verify output format.
#[tokio::test]
async fn test_export_metrics_text() {
    let server = ServerGuard::start().await;

    let mut metrics_client = MetricsServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect metrics client");

    metrics_client
        .export(common::make_export_metrics_request("export.gauge", 42.0))
        .await
        .expect("export metric");

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_motel"))
        .args([
            "export",
            "metrics",
            "-o",
            "text",
            "--addr",
            &server.query_addr(),
        ])
        .output()
        .expect("run export command");

    assert!(output.status.success(), "export should succeed");
    let stdout = String::from_utf8(output.stdout).expect("valid utf8");
    assert!(
        stdout.contains("export.gauge"),
        "output should contain metric name"
    );
    assert!(
        stdout.contains("test-service"),
        "output should contain service name"
    );
}

/// Export all signal types as JSONL, verify each has a signal field.
#[tokio::test]
async fn test_export_all_jsonl() {
    let server = ServerGuard::start().await;

    // Ingest one of each.
    let mut trace_client = TraceServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect trace client");
    trace_client
        .export(common::make_export_trace_request(&[1u8; 16], "all-span"))
        .await
        .expect("export trace");

    let mut logs_client = LogsServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect logs client");
    logs_client
        .export(common::make_export_logs_request("all-log"))
        .await
        .expect("export log");

    let mut metrics_client = MetricsServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect metrics client");
    metrics_client
        .export(common::make_export_metrics_request("all.metric", 1.0))
        .await
        .expect("export metric");

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_motel"))
        .args([
            "export",
            "all",
            "-o",
            "jsonl",
            "--addr",
            &server.query_addr(),
        ])
        .output()
        .expect("run export command");

    assert!(output.status.success(), "export should succeed");
    let stdout = String::from_utf8(output.stdout).expect("valid utf8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines.len(),
        3,
        "should have 3 JSONL lines (1 trace + 1 log + 1 metric)"
    );

    let signals: Vec<String> = lines
        .iter()
        .map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
            v["signal"].as_str().unwrap().to_string()
        })
        .collect();
    assert!(signals.contains(&"trace".to_string()));
    assert!(signals.contains(&"log".to_string()));
    assert!(signals.contains(&"metric".to_string()));
}

/// Export traces as proto (length-delimited), verify decoding.
#[tokio::test]
async fn test_export_traces_proto() {
    let server = ServerGuard::start().await;

    let mut trace_client = TraceServiceClient::connect(server.grpc_addr())
        .await
        .expect("connect trace client");

    trace_client
        .export(common::make_export_trace_request(&[1u8; 16], "proto-span"))
        .await
        .expect("export trace");

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_motel"))
        .args([
            "export",
            "traces",
            "-o",
            "proto",
            "--addr",
            &server.query_addr(),
        ])
        .output()
        .expect("run export command");

    assert!(output.status.success(), "export should succeed");
    let bytes = output.stdout;
    assert!(!bytes.is_empty(), "proto output should not be empty");

    // Decode length-delimited protobuf: 4-byte big-endian length prefix + message.
    let mut cursor = &bytes[..];
    let mut decoded_count = 0;
    while cursor.len() >= 4 {
        let len = u32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]) as usize;
        cursor = &cursor[4..];
        assert!(
            cursor.len() >= len,
            "not enough bytes for message of length {}",
            len
        );
        let rs = ResourceSpans::decode(&cursor[..len]).expect("valid protobuf ResourceSpans");
        assert!(!rs.scope_spans.is_empty());
        assert_eq!(rs.scope_spans[0].spans[0].name, "proto-span");
        cursor = &cursor[len..];
        decoded_count += 1;
    }
    assert_eq!(decoded_count, 1, "should have decoded 1 ResourceSpans");
    assert!(cursor.is_empty(), "no trailing bytes");
}

/// Export with no data produces empty output and exit code 0.
#[tokio::test]
async fn test_export_empty() {
    let server = ServerGuard::start().await;

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_motel"))
        .args([
            "export",
            "traces",
            "-o",
            "jsonl",
            "--addr",
            &server.query_addr(),
        ])
        .output()
        .expect("run export command");

    assert!(
        output.status.success(),
        "export with no data should succeed"
    );
    let stdout = String::from_utf8(output.stdout).expect("valid utf8");
    assert!(stdout.is_empty(), "empty store should produce no output");
}
