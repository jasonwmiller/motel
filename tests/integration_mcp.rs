mod common;

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

use common::otel::collector::trace::v1::trace_service_client::TraceServiceClient;
use common::{ServerGuard, make_export_trace_request};

/// Send a JSON-RPC request and read the JSON-RPC response from the MCP process.
fn send_jsonrpc(
    stdin: &mut impl Write,
    stdout: &mut impl BufRead,
    request: &serde_json::Value,
) -> serde_json::Value {
    let msg = serde_json::to_string(request).unwrap();
    writeln!(stdin, "{}", msg).unwrap();
    stdin.flush().unwrap();

    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    serde_json::from_str(&line).unwrap()
}

#[tokio::test]
async fn test_mcp_tools_list() {
    let server = ServerGuard::start().await;

    let bin = env!("CARGO_BIN_EXE_motel");
    let mut child = Command::new(bin)
        .args(["mcp", "--addr", &server.query_addr()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start motel mcp");

    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());

    // Initialize
    let init_request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {
                "name": "test-client",
                "version": "1.0"
            }
        }
    });
    let init_response = send_jsonrpc(&mut stdin, &mut stdout, &init_request);
    assert!(
        init_response.get("result").is_some(),
        "Initialize should succeed: {:?}",
        init_response
    );

    // Send initialized notification
    let initialized_notif = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    });
    let notif_msg = serde_json::to_string(&initialized_notif).unwrap();
    writeln!(stdin, "{}", notif_msg).unwrap();
    stdin.flush().unwrap();

    // Small delay to let initialization complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    // List tools
    let list_request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list"
    });
    let list_response = send_jsonrpc(&mut stdin, &mut stdout, &list_request);
    let result = list_response
        .get("result")
        .expect("tools/list should return result");
    let tools = result
        .get("tools")
        .expect("result should have tools")
        .as_array()
        .unwrap();

    assert_eq!(tools.len(), 5, "Should have 5 tools");

    let tool_names: Vec<&str> = tools
        .iter()
        .map(|t| t.get("name").unwrap().as_str().unwrap())
        .collect();
    assert!(tool_names.contains(&"query_traces"));
    assert!(tool_names.contains(&"query_logs"));
    assert!(tool_names.contains(&"query_metrics"));
    assert!(tool_names.contains(&"run_sql"));
    assert!(tool_names.contains(&"get_status"));

    // Cleanup
    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test]
async fn test_mcp_get_status() {
    let server = ServerGuard::start().await;

    let bin = env!("CARGO_BIN_EXE_motel");
    let mut child = Command::new(bin)
        .args(["mcp", "--addr", &server.query_addr()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start motel mcp");

    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());

    // Initialize
    let init_request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {
                "name": "test-client",
                "version": "1.0"
            }
        }
    });
    let _init_response = send_jsonrpc(&mut stdin, &mut stdout, &init_request);

    // Send initialized notification
    let notif_msg = serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    }))
    .unwrap();
    writeln!(stdin, "{}", notif_msg).unwrap();
    stdin.flush().unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Call get_status tool
    let call_request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "get_status",
            "arguments": {}
        }
    });
    let call_response = send_jsonrpc(&mut stdin, &mut stdout, &call_request);
    let result = call_response
        .get("result")
        .expect("tools/call should return result");
    let content = result.get("content").unwrap().as_array().unwrap();
    assert!(!content.is_empty(), "Should have content");
    let text = content[0].get("text").unwrap().as_str().unwrap();
    assert!(
        text.contains("Traces:"),
        "Status should contain trace count: {}",
        text
    );
    assert!(
        text.contains("Logs:"),
        "Status should contain log count: {}",
        text
    );
    assert!(
        text.contains("Metrics:"),
        "Status should contain metric count: {}",
        text
    );

    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test]
async fn test_mcp_run_sql() {
    let server = ServerGuard::start().await;

    // Ingest some test traces
    let mut trace_client = TraceServiceClient::connect(server.grpc_addr())
        .await
        .unwrap();
    let export_req = make_export_trace_request(&[1u8; 16], "test-span");
    trace_client.export(export_req).await.unwrap();

    let bin = env!("CARGO_BIN_EXE_motel");
    let mut child = Command::new(bin)
        .args(["mcp", "--addr", &server.query_addr()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start motel mcp");

    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());

    // Initialize
    let init_request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {
                "name": "test-client",
                "version": "1.0"
            }
        }
    });
    let _init_response = send_jsonrpc(&mut stdin, &mut stdout, &init_request);
    let notif_msg = serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    }))
    .unwrap();
    writeln!(stdin, "{}", notif_msg).unwrap();
    stdin.flush().unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Run SQL query
    let call_request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "run_sql",
            "arguments": {
                "query": "SELECT COUNT(*) as cnt FROM traces"
            }
        }
    });
    let call_response = send_jsonrpc(&mut stdin, &mut stdout, &call_request);
    let result = call_response
        .get("result")
        .expect("SQL query should return result");
    let content = result.get("content").unwrap().as_array().unwrap();
    let text = content[0].get("text").unwrap().as_str().unwrap();
    assert!(
        text.contains("cnt"),
        "SQL result should contain column name: {}",
        text
    );
    assert!(
        text.contains("1"),
        "SQL result should contain count 1: {}",
        text
    );

    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test]
async fn test_mcp_query_traces_with_data() {
    let server = ServerGuard::start().await;

    // Ingest test traces
    let mut trace_client = TraceServiceClient::connect(server.grpc_addr())
        .await
        .unwrap();
    let export_req = make_export_trace_request(&[2u8; 16], "my-operation");
    trace_client.export(export_req).await.unwrap();

    let bin = env!("CARGO_BIN_EXE_motel");
    let mut child = Command::new(bin)
        .args(["mcp", "--addr", &server.query_addr()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start motel mcp");

    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());

    // Initialize
    let _init_response = send_jsonrpc(
        &mut stdin,
        &mut stdout,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": { "name": "test-client", "version": "1.0" }
            }
        }),
    );
    writeln!(
        stdin,
        "{}",
        serde_json::to_string(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }))
        .unwrap()
    )
    .unwrap();
    stdin.flush().unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Query traces
    let call_response = send_jsonrpc(
        &mut stdin,
        &mut stdout,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "query_traces",
                "arguments": {
                    "service": "test-service"
                }
            }
        }),
    );
    let result = call_response
        .get("result")
        .expect("query_traces should return result");
    let content = result.get("content").unwrap().as_array().unwrap();
    let text = content[0].get("text").unwrap().as_str().unwrap();
    assert!(
        text.contains("my-operation"),
        "Should contain span name: {}",
        text
    );
    assert!(
        text.contains("test-service"),
        "Should contain service name: {}",
        text
    );

    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test]
async fn test_mcp_error_handling() {
    let server = ServerGuard::start().await;

    let bin = env!("CARGO_BIN_EXE_motel");
    let mut child = Command::new(bin)
        .args(["mcp", "--addr", &server.query_addr()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start motel mcp");

    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());

    // Initialize
    let _init_response = send_jsonrpc(
        &mut stdin,
        &mut stdout,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": { "name": "test-client", "version": "1.0" }
            }
        }),
    );
    writeln!(
        stdin,
        "{}",
        serde_json::to_string(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }))
        .unwrap()
    )
    .unwrap();
    stdin.flush().unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Call with invalid SQL - should return error content
    let call_response = send_jsonrpc(
        &mut stdin,
        &mut stdout,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "run_sql",
                "arguments": {
                    "query": "SELECT * FROM nonexistent_table"
                }
            }
        }),
    );
    let result = call_response
        .get("result")
        .expect("Should have result even on error");
    // The error is returned as content with isError flag
    let is_error = result
        .get("isError")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let content = result.get("content").unwrap().as_array().unwrap();
    let text = content[0].get("text").unwrap().as_str().unwrap();
    // Either isError is true, or the content contains an error message
    assert!(
        is_error || text.contains("error") || text.contains("Error"),
        "Invalid SQL should produce error. isError={}, text={}",
        is_error,
        text
    );

    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
}
