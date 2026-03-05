use anyhow::Result;

use crate::cli::StatusArgs;
use crate::query_proto::StatusRequest;
use crate::query_proto::query_service_client::QueryServiceClient;

pub async fn run(args: StatusArgs) -> Result<()> {
    let mut client = QueryServiceClient::connect(args.addr.clone()).await?;

    let response = client
        .status(StatusRequest {
            ..Default::default()
        })
        .await?;
    let resp = response.into_inner();

    println!("Traces:  {} ({} spans)", resp.trace_count, resp.span_count);
    println!("Logs:    {}", resp.log_count);
    println!("Metrics: {}", resp.metric_count);

    Ok(())
}
