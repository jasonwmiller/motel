use anyhow::Result;

use crate::cli::ResolvedViewArgs;
use crate::otel::{
    common::v1::{AnyValue, KeyValue, any_value},
    logs::v1::ResourceLogs,
    metrics::v1::ResourceMetrics,
    trace::v1::ResourceSpans,
};
use crate::query_proto::query_service_client::QueryServiceClient;
use crate::query_proto::{
    FollowRequest, QueryLogsRequest, QueryMetricsRequest, QueryTracesRequest,
};
use crate::store::{SharedStore, Store};

pub async fn run(args: ResolvedViewArgs) -> Result<()> {
    let multi_server = args.addr.len() > 1;

    // Create a single local store to merge all servers' data
    let (store, _event_rx) = Store::new_shared(10000, 100000, 100000);

    // Get a broadcast receiver for the TUI before inserting data
    let event_tx = store.read().await.event_tx.clone();
    let tui_event_rx = event_tx.subscribe();

    // Connect to each server and load existing + follow streams
    let mut any_connected = false;
    for addr in &args.addr {
        let server_label = extract_label(addr);
        match load_existing_data(addr, store.clone(), &server_label).await {
            Ok(_) => {
                any_connected = true;
                match spawn_follow_streams(addr, store.clone(), &server_label).await {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Warning: could not start follow streams for {}: {e}", addr);
                    }
                }
            }
            Err(e) => {
                eprintln!("Warning: could not connect to {}: {e}", addr);
            }
        }
    }

    if !any_connected {
        anyhow::bail!(
            "Could not connect to any server. Tried: {}",
            args.addr.join(", ")
        );
    }

    // Run the TUI — blocks until user quits
    crate::tui::run_with_options(store, tui_event_rx, multi_server).await
}

/// Extract a short label from the address URL for display.
fn extract_label(addr: &str) -> String {
    addr.trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}

/// Tag resource spans with the source server address.
fn tag_resource_spans(spans: &mut [ResourceSpans], source: &str) {
    for rs in spans.iter_mut() {
        let resource = rs.resource.get_or_insert_with(Default::default);
        resource.attributes.push(KeyValue {
            key: "motel.source".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(source.to_string())),
            }),
            ..Default::default()
        });
    }
}

/// Tag resource logs with the source server address.
fn tag_resource_logs(logs: &mut [ResourceLogs], source: &str) {
    for rl in logs.iter_mut() {
        let resource = rl.resource.get_or_insert_with(Default::default);
        resource.attributes.push(KeyValue {
            key: "motel.source".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(source.to_string())),
            }),
            ..Default::default()
        });
    }
}

/// Tag resource metrics with the source server address.
fn tag_resource_metrics(metrics: &mut [ResourceMetrics], source: &str) {
    for rm in metrics.iter_mut() {
        let resource = rm.resource.get_or_insert_with(Default::default);
        resource.attributes.push(KeyValue {
            key: "motel.source".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(source.to_string())),
            }),
            ..Default::default()
        });
    }
}

/// Load existing traces/logs/metrics from one server into the shared store.
async fn load_existing_data(addr: &str, store: SharedStore, server_label: &str) -> Result<()> {
    let mut client = QueryServiceClient::connect(addr.to_string()).await?;

    // Query existing traces
    let traces_resp = client
        .query_traces(QueryTracesRequest::default())
        .await?
        .into_inner();
    let mut resource_spans = traces_resp.resource_spans;
    if !resource_spans.is_empty() {
        tag_resource_spans(&mut resource_spans, server_label);
        store.write().await.insert_traces(resource_spans);
    }

    // Query existing logs
    let mut logs_client = QueryServiceClient::connect(addr.to_string()).await?;
    let logs_resp = logs_client
        .query_logs(QueryLogsRequest::default())
        .await?
        .into_inner();
    let mut resource_logs = logs_resp.resource_logs;
    if !resource_logs.is_empty() {
        tag_resource_logs(&mut resource_logs, server_label);
        store.write().await.insert_logs(resource_logs);
    }

    // Query existing metrics
    let mut metrics_client = QueryServiceClient::connect(addr.to_string()).await?;
    let metrics_resp = metrics_client
        .query_metrics(QueryMetricsRequest::default())
        .await?
        .into_inner();
    let mut resource_metrics = metrics_resp.resource_metrics;
    if !resource_metrics.is_empty() {
        tag_resource_metrics(&mut resource_metrics, server_label);
        store.write().await.insert_metrics(resource_metrics);
    }

    Ok(())
}

/// Spawn background tasks that follow live data from one server.
async fn spawn_follow_streams(addr: &str, store: SharedStore, server_label: &str) -> Result<()> {
    // Follow traces
    let mut client = QueryServiceClient::connect(addr.to_string()).await?;
    let traces_stream = client
        .follow_traces(FollowRequest::default())
        .await?
        .into_inner();
    let store_t = store.clone();
    let label_t = server_label.to_string();
    tokio::spawn(async move {
        let mut stream = traces_stream;
        loop {
            match stream.message().await {
                Ok(Some(resp)) => {
                    let mut spans = resp.resource_spans;
                    tag_resource_spans(&mut spans, &label_t);
                    store_t.write().await.insert_traces(spans);
                }
                Ok(None) => {
                    eprintln!("[{}] Trace follow stream ended", label_t);
                    break;
                }
                Err(e) => {
                    eprintln!("[{}] Trace follow error: {e}", label_t);
                    break;
                }
            }
        }
    });

    // Follow logs
    let mut logs_client = QueryServiceClient::connect(addr.to_string()).await?;
    let logs_stream = logs_client
        .follow_logs(FollowRequest::default())
        .await?
        .into_inner();
    let store_l = store.clone();
    let label_l = server_label.to_string();
    tokio::spawn(async move {
        let mut stream = logs_stream;
        loop {
            match stream.message().await {
                Ok(Some(resp)) => {
                    let mut logs = resp.resource_logs;
                    tag_resource_logs(&mut logs, &label_l);
                    store_l.write().await.insert_logs(logs);
                }
                Ok(None) => {
                    eprintln!("[{}] Log follow stream ended", label_l);
                    break;
                }
                Err(e) => {
                    eprintln!("[{}] Log follow error: {e}", label_l);
                    break;
                }
            }
        }
    });

    // Follow metrics
    let mut metrics_client = QueryServiceClient::connect(addr.to_string()).await?;
    let metrics_stream = metrics_client
        .follow_metrics(FollowRequest::default())
        .await?
        .into_inner();
    let store_m = store.clone();
    let label_m = server_label.to_string();
    tokio::spawn(async move {
        let mut stream = metrics_stream;
        loop {
            match stream.message().await {
                Ok(Some(resp)) => {
                    let mut metrics = resp.resource_metrics;
                    tag_resource_metrics(&mut metrics, &label_m);
                    store_m.write().await.insert_metrics(metrics);
                }
                Ok(None) => {
                    eprintln!("[{}] Metrics follow stream ended", label_m);
                    break;
                }
                Err(e) => {
                    eprintln!("[{}] Metrics follow error: {e}", label_m);
                    break;
                }
            }
        }
    });

    Ok(())
}
