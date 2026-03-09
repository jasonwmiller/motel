use anyhow::{Context, Result};

use crate::cli::{OutputFormat, ResolvedLogsArgs};
use crate::client::trace::format_timestamp_ns;
use crate::client::{extract_request_trace_id, parse_attributes, print_table};
use crate::query_proto::query_service_client::QueryServiceClient;
use crate::query_proto::{FollowRequest, QueryLogsRequest};

pub async fn run(args: ResolvedLogsArgs) -> Result<()> {
    if args.follow {
        return run_follow(args).await;
    }

    let mut client = QueryServiceClient::connect(args.addr.clone())
        .await
        .with_context(|| {
            format!(
                "could not connect to motel server at {}. Is it running?",
                args.addr
            )
        })?;

    let attributes = parse_attributes(&args.attribute)?;

    let request = QueryLogsRequest {
        service_name: args.service.clone().unwrap_or_default(),
        severity: args.severity.clone().unwrap_or_default(),
        body_contains: args.body.clone().unwrap_or_default(),
        since: args.since.clone().unwrap_or_default(),
        until: args.until.clone().unwrap_or_default(),
        limit: args.limit.unwrap_or(0),
        attributes: attributes.into_iter().collect(),
        ..Default::default()
    };

    let response = client.query_logs(request).await?;

    if args.show_trace_id {
        if let Some(trace_id) = extract_request_trace_id(&response) {
            eprintln!("trace_id: {}", trace_id);
        }
    }

    let resp = response.into_inner();

    // Flatten all log records
    let mut rows: Vec<LogRow> = Vec::new();
    for rl in &resp.resource_logs {
        let service_name = extract_service_name(rl);
        for sl in &rl.scope_logs {
            for lr in &sl.log_records {
                rows.push(LogRow {
                    time: format_timestamp_ns(lr.time_unix_nano),
                    service: service_name.clone(),
                    severity: format!("{:?}", lr.severity_number()),
                    body: lr.body.as_ref().map(format_any_value).unwrap_or_default(),
                });
            }
        }
    }

    match args.output {
        OutputFormat::Text => {
            for row in &rows {
                println!(
                    "{} {} [{}] {}",
                    row.time, row.service, row.severity, row.body
                );
            }
        }
        OutputFormat::Table => {
            if rows.is_empty() {
                println!("No logs found.");
                return Ok(());
            }
            let headers = &["TIME", "SERVICE", "SEVERITY", "BODY"];
            let col_data: Vec<Vec<String>> = vec![
                rows.iter().map(|r| r.time.clone()).collect(),
                rows.iter().map(|r| r.service.clone()).collect(),
                rows.iter().map(|r| r.severity.clone()).collect(),
                rows.iter().map(|r| r.body.clone()).collect(),
            ];
            print_table(headers, &col_data);
        }
        OutputFormat::Jsonl => {
            for row in &rows {
                let obj = serde_json::json!({
                    "time": row.time,
                    "service": row.service,
                    "severity": row.severity,
                    "body": row.body,
                });
                println!("{}", serde_json::to_string(&obj)?);
            }
        }
        OutputFormat::Csv => {
            let mut wtr = csv::Writer::from_writer(std::io::stdout());
            wtr.write_record(["time", "service", "severity", "body"])?;
            for row in &rows {
                wtr.write_record([&row.time, &row.service, &row.severity, &row.body])?;
            }
            wtr.flush()?;
        }
    }

    Ok(())
}

struct LogRow {
    time: String,
    service: String,
    severity: String,
    body: String,
}

fn extract_service_name(rl: &crate::otel::logs::v1::ResourceLogs) -> String {
    rl.resource
        .as_ref()
        .and_then(|r| {
            r.attributes
                .iter()
                .find(|kv| kv.key == "service.name")
                .and_then(|kv| kv.value.as_ref())
                .and_then(|v| match &v.value {
                    Some(crate::otel::common::v1::any_value::Value::StringValue(s)) => {
                        Some(s.clone())
                    }
                    _ => None,
                })
        })
        .unwrap_or_default()
}

fn format_any_value(v: &crate::otel::common::v1::AnyValue) -> String {
    match &v.value {
        Some(crate::otel::common::v1::any_value::Value::StringValue(s)) => s.clone(),
        Some(crate::otel::common::v1::any_value::Value::IntValue(i)) => i.to_string(),
        Some(crate::otel::common::v1::any_value::Value::DoubleValue(d)) => d.to_string(),
        Some(crate::otel::common::v1::any_value::Value::BoolValue(b)) => b.to_string(),
        Some(other) => format!("{:?}", other),
        None => String::new(),
    }
}

async fn run_follow(args: ResolvedLogsArgs) -> Result<()> {
    let mut client = QueryServiceClient::connect(args.addr.clone())
        .await
        .with_context(|| {
            format!(
                "could not connect to motel server at {}. Is it running?",
                args.addr
            )
        })?;

    let mut stream = client
        .follow_logs(FollowRequest::default())
        .await?
        .into_inner();

    let mut csv_writer = if matches!(args.output, OutputFormat::Csv) {
        let mut wtr = csv::Writer::from_writer(std::io::stdout());
        wtr.write_record(["time", "service", "severity", "body"])?;
        wtr.flush()?;
        Some(wtr)
    } else {
        None
    };

    loop {
        match stream.message().await {
            Ok(Some(resp)) => {
                for rl in &resp.resource_logs {
                    let service_name = extract_service_name(rl);
                    for sl in &rl.scope_logs {
                        for lr in &sl.log_records {
                            if let Some(ref svc) = args.service
                                && &service_name != svc
                            {
                                continue;
                            }
                            if let Some(ref sev) = args.severity {
                                let sev_text = format!("{:?}", lr.severity_number());
                                if !sev_text.to_uppercase().contains(&sev.to_uppercase()) {
                                    continue;
                                }
                            }
                            if let Some(ref body_filter) = args.body {
                                let body_str =
                                    lr.body.as_ref().map(format_any_value).unwrap_or_default();
                                if !body_str.contains(body_filter.as_str()) {
                                    continue;
                                }
                            }

                            let row = LogRow {
                                time: format_timestamp_ns(lr.time_unix_nano),
                                service: service_name.clone(),
                                severity: format!("{:?}", lr.severity_number()),
                                body: lr.body.as_ref().map(format_any_value).unwrap_or_default(),
                            };

                            match args.output {
                                OutputFormat::Text | OutputFormat::Table => {
                                    println!(
                                        "{} {} [{}] {}",
                                        row.time, row.service, row.severity, row.body
                                    );
                                }
                                OutputFormat::Jsonl => {
                                    let obj = serde_json::json!({
                                        "time": row.time,
                                        "service": row.service,
                                        "severity": row.severity,
                                        "body": row.body,
                                    });
                                    println!("{}", serde_json::to_string(&obj)?);
                                }
                                OutputFormat::Csv => {
                                    if let Some(ref mut wtr) = csv_writer {
                                        wtr.write_record([
                                            &row.time,
                                            &row.service,
                                            &row.severity,
                                            &row.body,
                                        ])?;
                                        wtr.flush()?;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Ok(None) => {
                eprintln!("Server closed the follow stream");
                break;
            }
            Err(e) => {
                eprintln!("Follow stream error: {}", e);
                break;
            }
        }
    }
    Ok(())
}
