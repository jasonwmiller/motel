// Integration tests for the Prometheus scrape endpoint.
//
// Each test starts a motel server with --prometheus enabled, ingests data,
// and scrapes GET /metrics to verify Prometheus text exposition format.

mod common;

use std::net::TcpListener;
use std::process::{Child, Command};
use std::time::Duration;

/// Start a motel server with Prometheus enabled on a given port.
fn start_server_with_prometheus(
    grpc_port: u16,
    http_port: u16,
    query_port: u16,
    prom_port: u16,
) -> Child {
    let bin = env!("CARGO_BIN_EXE_motel");
    Command::new(bin)
        .args([
            "server",
            "--no-tui",
            "--grpc-addr",
            &format!("0.0.0.0:{grpc_port}"),
            "--http-addr",
            &format!("0.0.0.0:{http_port}"),
            "--query-addr",
            &format!("0.0.0.0:{query_port}"),
            "--prometheus",
            "--prom-addr",
            &format!("0.0.0.0:{prom_port}"),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to start motel server with prometheus")
}

fn get_available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("failed to bind")
        .local_addr()
        .unwrap()
        .port()
}

struct PromServerGuard {
    child: Child,
    grpc_port: u16,
    http_port: u16,
    query_port: u16,
    prom_port: u16,
}

impl PromServerGuard {
    async fn start() -> Self {
        let grpc_port = get_available_port();
        let http_port = get_available_port();
        let query_port = get_available_port();
        let prom_port = get_available_port();
        let child = start_server_with_prometheus(grpc_port, http_port, query_port, prom_port);

        // Wait for the Prometheus port to be ready
        let prom_addr = format!("127.0.0.1:{prom_port}");
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(&prom_addr).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(50)).await;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Self {
            child,
            grpc_port,
            http_port,
            query_port,
            prom_port,
        }
    }

    fn prom_url(&self) -> String {
        format!("http://127.0.0.1:{}/metrics", self.prom_port)
    }

    fn http_base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.http_port)
    }
}

impl Drop for PromServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";

/// Verify the Prometheus endpoint returns correct content-type and motel internal metrics.
#[tokio::test]
async fn test_prometheus_content_type_and_internal_metrics() {
    let server = PromServerGuard::start().await;

    let client = reqwest::Client::new();
    let resp = client
        .get(server.prom_url())
        .send()
        .await
        .expect("failed to scrape /metrics");

    assert!(resp.status().is_success());
    let ct = resp
        .headers()
        .get("content-type")
        .expect("missing content-type")
        .to_str()
        .unwrap();
    assert!(
        ct.contains("text/plain"),
        "expected text/plain content-type, got: {ct}"
    );

    let body = resp.text().await.unwrap();
    assert!(body.contains("motel_traces_total 0"));
    assert!(body.contains("motel_spans_total 0"));
    assert!(body.contains("motel_logs_total 0"));
    assert!(body.contains("motel_metrics_total 0"));
}

/// Ingest metrics and verify they appear in Prometheus format.
#[tokio::test]
async fn test_prometheus_shows_ingested_metrics() {
    let server = PromServerGuard::start().await;

    // Ingest a gauge metric via OTLP HTTP
    let request = common::make_export_metrics_request("cpu.usage", 42.5);
    let body = common::encode_proto(&request);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v1/metrics", server.http_base_url()))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .body(body)
        .send()
        .await
        .expect("failed to send metrics");
    assert!(resp.status().is_success());

    // Scrape /metrics
    let resp = client
        .get(server.prom_url())
        .send()
        .await
        .expect("failed to scrape /metrics");
    let body = resp.text().await.unwrap();

    assert!(
        body.contains("# TYPE cpu_usage gauge"),
        "expected gauge type line in:\n{body}"
    );
    assert!(
        body.contains("cpu_usage{service_name=\"test-service\"} 42.5"),
        "expected gauge data point in:\n{body}"
    );
    // Internal metrics should reflect the ingested data
    assert!(
        body.contains("motel_metrics_total 1"),
        "expected motel_metrics_total to be 1 in:\n{body}"
    );
}

/// Ingest traces and verify motel_traces_total increases.
#[tokio::test]
async fn test_prometheus_internal_metrics_after_trace_ingest() {
    let server = PromServerGuard::start().await;

    // Ingest a trace via OTLP HTTP
    let request = common::make_export_trace_request(&[1u8; 16], "test-span");
    let body = common::encode_proto(&request);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v1/traces", server.http_base_url()))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .body(body)
        .send()
        .await
        .expect("failed to send traces");
    assert!(resp.status().is_success());

    // Scrape /metrics
    let resp = client
        .get(server.prom_url())
        .send()
        .await
        .expect("failed to scrape /metrics");
    let body = resp.text().await.unwrap();

    assert!(
        body.contains("motel_traces_total 1"),
        "expected motel_traces_total to be 1 in:\n{body}"
    );
    assert!(
        body.contains("motel_spans_total 1"),
        "expected motel_spans_total to be 1 in:\n{body}"
    );
}
