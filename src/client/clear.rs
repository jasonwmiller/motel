use anyhow::Result;

use crate::cli::{ClearTarget, ResolvedClearArgs};
use crate::query_proto::ClearRequest;
use crate::query_proto::query_service_client::QueryServiceClient;

pub async fn run(args: ResolvedClearArgs) -> Result<()> {
    let mut client = QueryServiceClient::connect(args.addr.clone()).await?;

    let request = ClearRequest {
        ..Default::default()
    };

    match args.target {
        ClearTarget::Traces => {
            let resp = client.clear_traces(request).await?.into_inner();
            println!("Cleared {} traces", resp.cleared_count);
        }
        ClearTarget::Logs => {
            let resp = client.clear_logs(request).await?.into_inner();
            println!("Cleared {} logs", resp.cleared_count);
        }
        ClearTarget::Metrics => {
            let resp = client.clear_metrics(request).await?.into_inner();
            println!("Cleared {} metrics", resp.cleared_count);
        }
        ClearTarget::All => {
            let resp = client.clear_all(request).await?.into_inner();
            println!("Cleared {} items", resp.cleared_count);
        }
    }

    Ok(())
}
