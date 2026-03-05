use anyhow::Result;

use crate::cli::ViewArgs;
use crate::query_proto::FollowRequest;
use crate::query_proto::query_service_client::QueryServiceClient;
use crate::store::Store;

pub async fn run(args: ViewArgs) -> Result<()> {
    let mut client = QueryServiceClient::connect(args.addr.clone()).await?;

    // Create a local store to accumulate data for TUI
    let (store, _event_rx) = Store::new_shared(10000, 100000, 100000);

    // Subscribe to all three follow streams
    let mut traces_stream = client
        .follow_traces(FollowRequest {
            ..Default::default()
        })
        .await?
        .into_inner();

    let mut logs_client = QueryServiceClient::connect(args.addr.clone()).await?;
    let mut logs_stream = logs_client
        .follow_logs(FollowRequest {
            ..Default::default()
        })
        .await?
        .into_inner();

    let mut metrics_client = QueryServiceClient::connect(args.addr.clone()).await?;
    let mut metrics_stream = metrics_client
        .follow_metrics(FollowRequest {
            ..Default::default()
        })
        .await?
        .into_inner();

    println!(
        "Connected to {}. Following traces, logs, and metrics...",
        args.addr
    );

    // Process incoming data from all streams concurrently
    let store_traces = store.clone();
    let store_logs = store.clone();
    let store_metrics = store.clone();

    let traces_task = tokio::spawn(async move {
        while let Ok(Some(resp)) = traces_stream.message().await {
            let count: usize = resp
                .resource_spans
                .iter()
                .map(|rs| {
                    rs.scope_spans
                        .iter()
                        .map(|ss| ss.spans.len())
                        .sum::<usize>()
                })
                .sum();
            store_traces
                .write()
                .await
                .insert_traces(resp.resource_spans);
            eprintln!("Received {} spans", count);
        }
    });

    let logs_task = tokio::spawn(async move {
        while let Ok(Some(resp)) = logs_stream.message().await {
            let count: usize = resp
                .resource_logs
                .iter()
                .map(|rl| {
                    rl.scope_logs
                        .iter()
                        .map(|sl| sl.log_records.len())
                        .sum::<usize>()
                })
                .sum();
            store_logs.write().await.insert_logs(resp.resource_logs);
            eprintln!("Received {} log records", count);
        }
    });

    let metrics_task = tokio::spawn(async move {
        while let Ok(Some(resp)) = metrics_stream.message().await {
            let count: usize = resp
                .resource_metrics
                .iter()
                .map(|rm| {
                    rm.scope_metrics
                        .iter()
                        .map(|sm| sm.metrics.len())
                        .sum::<usize>()
                })
                .sum();
            store_metrics
                .write()
                .await
                .insert_metrics(resp.resource_metrics);
            eprintln!("Received {} metrics", count);
        }
    });

    // Wait for any stream to end (they run until server disconnects or error)
    tokio::select! {
        _ = traces_task => {}
        _ = logs_task => {}
        _ = metrics_task => {}
    }

    Ok(())
}
