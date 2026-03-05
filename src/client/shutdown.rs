use anyhow::Result;

use crate::cli::ShutdownArgs;
use crate::query_proto::ShutdownRequest;
use crate::query_proto::query_service_client::QueryServiceClient;

pub async fn run(args: ShutdownArgs) -> Result<()> {
    let mut client = QueryServiceClient::connect(args.addr.clone()).await?;

    let response = client
        .shutdown(ShutdownRequest {
            ..Default::default()
        })
        .await?;
    let resp = response.into_inner();

    println!("{}", resp.message);

    Ok(())
}
