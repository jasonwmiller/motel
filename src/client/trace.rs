use anyhow::Result;

use crate::cli::{OutputFormat, ResolvedTracesArgs};
use crate::client::{extract_request_trace_id, hex_encode, parse_attributes, print_table};
use crate::query_proto::query_service_client::QueryServiceClient;
use crate::query_proto::{FollowRequest, QueryTracesRequest};

pub async fn run(args: ResolvedTracesArgs) -> Result<()> {
    if args.follow {
        return run_follow(args).await;
    }

    let mut client = QueryServiceClient::connect(args.addr.clone()).await?;

    let attributes = parse_attributes(&args.attribute)?;

    let request = QueryTracesRequest {
        service_name: args.service.clone().unwrap_or_default(),
        span_name: args.span_name.clone().unwrap_or_default(),
        trace_id: args.trace_id.clone().unwrap_or_default(),
        since: args.since.clone().unwrap_or_default(),
        until: args.until.clone().unwrap_or_default(),
        limit: args.limit.unwrap_or(0),
        attributes: attributes.into_iter().collect(),
        ..Default::default()
    };

    let response = client.query_traces(request).await?;

    if args.show_trace_id {
        if let Some(trace_id) = extract_request_trace_id(&response) {
            eprintln!("trace_id: {}", trace_id);
        }
    }

    let resp = response.into_inner();

    // Flatten all spans from all resource_spans
    let mut rows: Vec<SpanRow> = Vec::new();
    for rs in &resp.resource_spans {
        let service_name = extract_service_name(rs);
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                let start_ns = span.start_time_unix_nano;
                let end_ns = span.end_time_unix_nano;
                let duration_ns = end_ns.saturating_sub(start_ns);
                rows.push(SpanRow {
                    time: format_timestamp_ns(start_ns),
                    service: service_name.clone(),
                    span_name: span.name.clone(),
                    duration_ms: duration_ns as f64 / 1_000_000.0,
                    trace_id: hex_encode(&span.trace_id),
                    span_id: hex_encode(&span.span_id),
                    status: format_status(span.status.as_ref()),
                });
            }
        }
    }

    match args.output {
        OutputFormat::Text => {
            for row in &rows {
                println!(
                    "{} {} {} {:.3}ms trace_id={}",
                    row.time, row.service, row.span_name, row.duration_ms, row.trace_id,
                );
            }
        }
        OutputFormat::Table => {
            if rows.is_empty() {
                println!("No traces found.");
                return Ok(());
            }
            let headers = &["TIME", "SERVICE", "SPAN", "DURATION", "TRACE_ID", "STATUS"];
            let col_data: Vec<Vec<String>> = vec![
                rows.iter().map(|r| r.time.clone()).collect(),
                rows.iter().map(|r| r.service.clone()).collect(),
                rows.iter().map(|r| r.span_name.clone()).collect(),
                rows.iter()
                    .map(|r| format!("{:.3}ms", r.duration_ms))
                    .collect(),
                rows.iter().map(|r| r.trace_id.clone()).collect(),
                rows.iter().map(|r| r.status.clone()).collect(),
            ];
            print_table(headers, &col_data);
        }
        OutputFormat::Jsonl => {
            for row in &rows {
                let obj = serde_json::json!({
                    "time": row.time,
                    "service": row.service,
                    "span_name": row.span_name,
                    "duration_ms": row.duration_ms,
                    "trace_id": row.trace_id,
                    "span_id": row.span_id,
                    "status": row.status,
                });
                println!("{}", serde_json::to_string(&obj)?);
            }
        }
        OutputFormat::Csv => {
            let mut wtr = csv::Writer::from_writer(std::io::stdout());
            wtr.write_record([
                "time",
                "service",
                "span_name",
                "duration_ms",
                "trace_id",
                "span_id",
                "status",
            ])?;
            for row in &rows {
                wtr.write_record([
                    &row.time,
                    &row.service,
                    &row.span_name,
                    &format!("{:.3}", row.duration_ms),
                    &row.trace_id,
                    &row.span_id,
                    &row.status,
                ])?;
            }
            wtr.flush()?;
        }
    }

    Ok(())
}

struct SpanRow {
    time: String,
    service: String,
    span_name: String,
    duration_ms: f64,
    trace_id: String,
    span_id: String,
    status: String,
}

fn extract_service_name(rs: &crate::otel::trace::v1::ResourceSpans) -> String {
    rs.resource
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

pub fn format_timestamp_ns(ns: u64) -> String {
    use chrono::{DateTime, Utc};
    let secs = (ns / 1_000_000_000) as i64;
    let nsec = (ns % 1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nsec)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| format!("{}ns", ns))
}

fn format_status(status: Option<&crate::otel::trace::v1::Status>) -> String {
    match status {
        Some(s) => format!("{:?}", s.code()),
        None => "UNSET".into(),
    }
}

async fn run_follow(args: ResolvedTracesArgs) -> Result<()> {
    let mut client = QueryServiceClient::connect(args.addr.clone()).await?;

    let mut stream = client
        .follow_traces(FollowRequest::default())
        .await?
        .into_inner();

    let mut csv_writer = if matches!(args.output, OutputFormat::Csv) {
        let mut wtr = csv::Writer::from_writer(std::io::stdout());
        wtr.write_record([
            "time",
            "service",
            "span_name",
            "duration_ms",
            "trace_id",
            "span_id",
            "status",
        ])?;
        wtr.flush()?;
        Some(wtr)
    } else {
        None
    };

    loop {
        match stream.message().await {
            Ok(Some(resp)) => {
                for rs in &resp.resource_spans {
                    let service_name = extract_service_name(rs);
                    for ss in &rs.scope_spans {
                        for span in &ss.spans {
                            if let Some(ref svc) = args.service
                                && &service_name != svc
                            {
                                continue;
                            }
                            if let Some(ref name) = args.span_name
                                && &span.name != name
                            {
                                continue;
                            }

                            let start_ns = span.start_time_unix_nano;
                            let end_ns = span.end_time_unix_nano;
                            let duration_ns = end_ns.saturating_sub(start_ns);
                            let row = SpanRow {
                                time: format_timestamp_ns(start_ns),
                                service: service_name.clone(),
                                span_name: span.name.clone(),
                                duration_ms: duration_ns as f64 / 1_000_000.0,
                                trace_id: hex_encode(&span.trace_id),
                                span_id: hex_encode(&span.span_id),
                                status: format_status(span.status.as_ref()),
                            };

                            match args.output {
                                OutputFormat::Text | OutputFormat::Table => {
                                    println!(
                                        "{} {} {} {:.3}ms trace_id={}",
                                        row.time,
                                        row.service,
                                        row.span_name,
                                        row.duration_ms,
                                        row.trace_id,
                                    );
                                }
                                OutputFormat::Jsonl => {
                                    let obj = serde_json::json!({
                                        "time": row.time,
                                        "service": row.service,
                                        "span_name": row.span_name,
                                        "duration_ms": row.duration_ms,
                                        "trace_id": row.trace_id,
                                        "span_id": row.span_id,
                                        "status": row.status,
                                    });
                                    println!("{}", serde_json::to_string(&obj)?);
                                }
                                OutputFormat::Csv => {
                                    if let Some(ref mut wtr) = csv_writer {
                                        wtr.write_record([
                                            &row.time,
                                            &row.service,
                                            &row.span_name,
                                            &format!("{:.3}", row.duration_ms),
                                            &row.trace_id,
                                            &row.span_id,
                                            &row.status,
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
