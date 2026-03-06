use anyhow::Result;

use crate::cli::{OutputFormat, ResolvedSqlArgs};
use crate::client::{extract_request_trace_id, print_table};
use crate::query_proto::SqlQueryRequest;
use crate::query_proto::query_service_client::QueryServiceClient;

pub async fn run(args: ResolvedSqlArgs) -> Result<()> {
    let mut client = QueryServiceClient::connect(args.addr.clone()).await?;

    let request = SqlQueryRequest {
        query: args.query.clone(),
        ..Default::default()
    };

    let response = client.sql_query(request).await?;

    if args.show_trace_id {
        if let Some(trace_id) = extract_request_trace_id(&response) {
            eprintln!("trace_id: {}", trace_id);
        }
    }

    let resp = response.into_inner();

    let col_names: Vec<&str> = resp.columns.iter().map(|c| c.name.as_str()).collect();

    match args.output {
        OutputFormat::Table => {
            if resp.rows.is_empty() {
                if !col_names.is_empty() {
                    // Print headers even with no rows
                    let headers: Vec<&str> = col_names.clone();
                    let empty_cols: Vec<Vec<String>> =
                        (0..col_names.len()).map(|_| Vec::new()).collect();
                    print_table(&headers, &empty_cols);
                } else {
                    println!("No results.");
                }
                return Ok(());
            }
            let headers: Vec<&str> = col_names.clone();
            let col_data: Vec<Vec<String>> = (0..col_names.len())
                .map(|i| {
                    resp.rows
                        .iter()
                        .map(|row| row.values.get(i).cloned().unwrap_or_default())
                        .collect()
                })
                .collect();
            print_table(&headers, &col_data);
        }
        OutputFormat::Text => {
            for row in &resp.rows {
                let parts: Vec<String> = col_names
                    .iter()
                    .zip(row.values.iter())
                    .map(|(name, val)| format!("{}={}", name, val))
                    .collect();
                println!("{}", parts.join(" "));
            }
        }
        OutputFormat::Jsonl => {
            for row in &resp.rows {
                let mut map = serde_json::Map::new();
                for (name, val) in col_names.iter().zip(row.values.iter()) {
                    map.insert(name.to_string(), serde_json::Value::String(val.clone()));
                }
                println!("{}", serde_json::Value::Object(map));
            }
        }
        OutputFormat::Csv => {
            let mut wtr = csv::Writer::from_writer(std::io::stdout());
            wtr.write_record(&col_names)?;
            for row in &resp.rows {
                wtr.write_record(&row.values)?;
            }
            wtr.flush()?;
        }
    }

    Ok(())
}

