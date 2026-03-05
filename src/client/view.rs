use anyhow::Result;

use crate::cli::ViewArgs;
use crate::query_proto::FollowRequest;
use crate::query_proto::query_service_client::QueryServiceClient;
use crate::store::Store;

pub async fn run(args: ViewArgs) -> Result<()> {
    let mut client = QueryServiceClient::connect(args.addr.clone()).await?;

    // Create a local store to accumulate data for TUI
    let (store, _event_rx) = Store::new_shared(10000, 100000, 100000);

    // Get a broadcast receiver for the TUI before spawning tasks
    // (insert_* on the store will broadcast events)
    let event_tx = store.read().await.event_tx.clone();
    let tui_event_rx = event_tx.subscribe();

    // Subscribe to all three follow streams
    let traces_stream = client
        .follow_traces(FollowRequest {
            ..Default::default()
        })
        .await?
        .into_inner();

    let mut logs_client = QueryServiceClient::connect(args.addr.clone()).await?;
    let logs_stream = logs_client
        .follow_logs(FollowRequest {
            ..Default::default()
        })
        .await?
        .into_inner();

    let mut metrics_client = QueryServiceClient::connect(args.addr.clone()).await?;
    let metrics_stream = metrics_client
        .follow_metrics(FollowRequest {
            ..Default::default()
        })
        .await?
        .into_inner();

    // Spawn background tasks to pipe stream data into the local store
    let store_traces = store.clone();
    tokio::spawn(async move {
        let mut stream = traces_stream;
        loop {
            match stream.message().await {
                Ok(Some(resp)) => {
                    store_traces
                        .write()
                        .await
                        .insert_traces(resp.resource_spans);
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("Follow stream error: {e}");
                    break;
                }
            }
        }
    });

    let store_logs = store.clone();
    tokio::spawn(async move {
        let mut stream = logs_stream;
        loop {
            match stream.message().await {
                Ok(Some(resp)) => {
                    store_logs.write().await.insert_logs(resp.resource_logs);
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("Follow stream error: {e}");
                    break;
                }
            }
        }
    });

    let store_metrics = store.clone();
    tokio::spawn(async move {
        let mut stream = metrics_stream;
        loop {
            match stream.message().await {
                Ok(Some(resp)) => {
                    store_metrics
                        .write()
                        .await
                        .insert_metrics(resp.resource_metrics);
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("Follow stream error: {e}");
                    break;
                }
            }
        }
    });

    // Run the TUI — blocks until user quits
    crate::tui::run(store, tui_event_rx).await
}
