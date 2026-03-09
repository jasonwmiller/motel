// Integration tests for the web UI dashboard.
//
// Each test starts its own motel server with --web enabled, then makes HTTP
// requests to the web UI endpoints to verify correct behavior.

mod common;

use common::ServerGuard;

const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";

/// Extended ServerGuard that also tracks the web port.
struct WebServerGuard {
    guard: ServerGuard,
    web_port: u16,
}

impl WebServerGuard {
    async fn start() -> Self {
        let grpc_port = common::get_available_port();
        let http_port = common::get_available_port();
        let query_port = common::get_available_port();
        let web_port = common::get_available_port();

        let bin = env!("CARGO_BIN_EXE_motel");
        let child = std::process::Command::new(bin)
            .args([
                "server",
                "--no-tui",
                "--web",
                "--grpc-addr",
                &format!("0.0.0.0:{grpc_port}"),
                "--http-addr",
                &format!("0.0.0.0:{http_port}"),
                "--query-addr",
                &format!("0.0.0.0:{query_port}"),
                "--web-addr",
                &format!("0.0.0.0:{web_port}"),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to start motel server");

        // Wait for both query and web ports
        common::wait_for_server(query_port).await;
        common::wait_for_server(web_port).await;

        Self {
            guard: ServerGuard {
                child,
                grpc_port,
                http_port,
                query_port,
            },
            web_port,
        }
    }

    fn web_base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.web_port)
    }
}

#[tokio::test]
async fn test_web_index_returns_html() {
    let server = WebServerGuard::start().await;
    let resp = reqwest::get(&format!("{}/", server.web_base_url()))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<title>motel"));
    assert!(body.contains("OpenTelemetry Dashboard"));
}

#[tokio::test]
async fn test_web_static_assets() {
    let server = WebServerGuard::start().await;

    let resp = reqwest::get(&format!("{}/app.js", server.web_base_url()))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(ct.contains("javascript"));

    let resp = reqwest::get(&format!("{}/style.css", server.web_base_url()))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(ct.contains("text/css"));
}

#[tokio::test]
async fn test_web_api_status_json() {
    let server = WebServerGuard::start().await;
    let resp = reqwest::get(&format!("{}/api/status", server.web_base_url()))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let data: serde_json::Value = resp.json().await.unwrap();
    assert!(data.get("trace_count").is_some());
    assert!(data.get("span_count").is_some());
    assert!(data.get("log_count").is_some());
    assert!(data.get("metric_count").is_some());
}

#[tokio::test]
async fn test_web_api_traces_after_ingest() {
    let server = WebServerGuard::start().await;

    // Ingest traces via OTLP HTTP
    let request = common::make_export_trace_request(&[1u8; 16], "web-test-span");
    let body = common::encode_proto(&request);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://127.0.0.1:{}/v1/traces",
            server.guard.http_port
        ))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Small delay for processing
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Query via web API
    let resp = reqwest::get(&format!("{}/api/traces", server.web_base_url()))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let data: serde_json::Value = resp.json().await.unwrap();
    let traces = data.as_array().unwrap();
    assert!(!traces.is_empty(), "expected at least one trace group");
    assert_eq!(traces[0]["root_span_name"], "web-test-span");
}

#[tokio::test]
async fn test_web_api_sql() {
    let server = WebServerGuard::start().await;

    let resp = reqwest::get(&format!(
        "{}/api/sql?q={}",
        server.web_base_url(),
        urlencoding::encode("SELECT 1 as x")
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 200);
    let data: serde_json::Value = resp.json().await.unwrap();
    assert!(data.get("columns").unwrap().as_array().unwrap().len() > 0);
    assert!(data.get("rows").unwrap().as_array().unwrap().len() > 0);
}
