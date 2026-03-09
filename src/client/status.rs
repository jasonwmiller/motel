use anyhow::{Context, Result};

use crate::cli::ResolvedStatusArgs;
use crate::query_proto::StatusRequest;
use crate::query_proto::query_service_client::QueryServiceClient;

pub async fn run(args: ResolvedStatusArgs) -> Result<()> {
    let mut client = QueryServiceClient::connect(args.addr.clone())
        .await
        .with_context(|| {
            format!(
                "could not connect to motel server at {}. Is it running?",
                args.addr
            )
        })?;

    let response = client.status(StatusRequest {}).await?;
    let resp = response.into_inner();

    println!("Traces:  {} ({} spans)", resp.trace_count, resp.span_count);
    println!("Logs:    {}", resp.log_count);
    println!("Metrics: {}", resp.metric_count);
    if resp.sample_rate < 1.0 {
        println!(
            "Sample:  {:.1}% (dropped {} spans)",
            resp.sample_rate * 100.0,
            resp.traces_dropped
        );
    }

    Ok(())
}
