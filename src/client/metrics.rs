use anyhow::Result;

use crate::cli::{OutputFormat, ResolvedMetricsArgs};
use crate::client::{extract_request_trace_id, print_table};
use crate::query_proto::query_service_client::QueryServiceClient;
use crate::query_proto::{FollowRequest, QueryMetricsRequest};

pub async fn run(args: ResolvedMetricsArgs) -> Result<()> {
    if args.follow {
        return run_follow(args).await;
    }

    let mut client = QueryServiceClient::connect(args.addr.clone()).await?;

    let request = QueryMetricsRequest {
        service_name: args.service.clone().unwrap_or_default(),
        metric_name: args.name.clone().unwrap_or_default(),
        since: args.since.clone().unwrap_or_default(),
        until: args.until.clone().unwrap_or_default(),
        limit: args.limit.unwrap_or(0),
        ..Default::default()
    };

    let response = client.query_metrics(request).await?;

    if args.show_trace_id {
        if let Some(trace_id) = extract_request_trace_id(&response) {
            eprintln!("trace_id: {}", trace_id);
        }
    }

    let resp = response.into_inner();

    // Flatten all metrics
    let mut rows: Vec<MetricRow> = Vec::new();
    for rm in &resp.resource_metrics {
        let service_name = extract_service_name(rm);
        for sm in &rm.scope_metrics {
            for metric in &sm.metrics {
                let data_type = describe_metric_data(&metric.data);
                rows.push(MetricRow {
                    service: service_name.clone(),
                    name: metric.name.clone(),
                    description: metric.description.clone(),
                    unit: metric.unit.clone(),
                    data_type,
                });
            }
        }
    }

    match args.output {
        OutputFormat::Text => {
            for row in &rows {
                println!(
                    "{} {} ({}) [{}] {}",
                    row.service, row.name, row.unit, row.data_type, row.description,
                );
            }
        }
        OutputFormat::Table => {
            if rows.is_empty() {
                println!("No metrics found.");
                return Ok(());
            }
            let headers = &["SERVICE", "NAME", "UNIT", "TYPE", "DESCRIPTION"];
            let col_data: Vec<Vec<String>> = vec![
                rows.iter().map(|r| r.service.clone()).collect(),
                rows.iter().map(|r| r.name.clone()).collect(),
                rows.iter().map(|r| r.unit.clone()).collect(),
                rows.iter().map(|r| r.data_type.clone()).collect(),
                rows.iter().map(|r| r.description.clone()).collect(),
            ];
            print_table(headers, &col_data);
        }
        OutputFormat::Jsonl => {
            for row in &rows {
                let obj = serde_json::json!({
                    "service": row.service,
                    "name": row.name,
                    "description": row.description,
                    "unit": row.unit,
                    "data_type": row.data_type,
                });
                println!("{}", serde_json::to_string(&obj)?);
            }
        }
        OutputFormat::Csv => {
            let mut wtr = csv::Writer::from_writer(std::io::stdout());
            wtr.write_record(["service", "name", "unit", "data_type", "description"])?;
            for row in &rows {
                wtr.write_record([
                    &row.service,
                    &row.name,
                    &row.unit,
                    &row.data_type,
                    &row.description,
                ])?;
            }
            wtr.flush()?;
        }
    }

    Ok(())
}

struct MetricRow {
    service: String,
    name: String,
    description: String,
    unit: String,
    data_type: String,
}

fn extract_service_name(rm: &crate::otel::metrics::v1::ResourceMetrics) -> String {
    rm.resource
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

fn describe_metric_data(data: &Option<crate::otel::metrics::v1::metric::Data>) -> String {
    match data {
        Some(crate::otel::metrics::v1::metric::Data::Gauge(_)) => "Gauge".into(),
        Some(crate::otel::metrics::v1::metric::Data::Sum(_)) => "Sum".into(),
        Some(crate::otel::metrics::v1::metric::Data::Histogram(_)) => "Histogram".into(),
        Some(crate::otel::metrics::v1::metric::Data::ExponentialHistogram(_)) => {
            "ExponentialHistogram".into()
        }
        Some(crate::otel::metrics::v1::metric::Data::Summary(_)) => "Summary".into(),
        None => "Unknown".into(),
    }
}

async fn run_follow(args: ResolvedMetricsArgs) -> Result<()> {
    let mut client = QueryServiceClient::connect(args.addr.clone()).await?;

    let mut stream = client
        .follow_metrics(FollowRequest::default())
        .await?
        .into_inner();

    let mut csv_writer = if matches!(args.output, OutputFormat::Csv) {
        let mut wtr = csv::Writer::from_writer(std::io::stdout());
        wtr.write_record(["service", "name", "unit", "data_type", "description"])?;
        wtr.flush()?;
        Some(wtr)
    } else {
        None
    };

    loop {
        match stream.message().await {
            Ok(Some(resp)) => {
                for rm in &resp.resource_metrics {
                    let service_name = extract_service_name(rm);
                    for sm in &rm.scope_metrics {
                        for metric in &sm.metrics {
                            if let Some(ref svc) = args.service
                                && &service_name != svc
                            {
                                continue;
                            }
                            if let Some(ref name) = args.name
                                && &metric.name != name
                            {
                                continue;
                            }

                            let data_type = describe_metric_data(&metric.data);
                            let row = MetricRow {
                                service: service_name.clone(),
                                name: metric.name.clone(),
                                description: metric.description.clone(),
                                unit: metric.unit.clone(),
                                data_type,
                            };

                            match args.output {
                                OutputFormat::Text | OutputFormat::Table => {
                                    println!(
                                        "{} {} ({}) [{}] {}",
                                        row.service,
                                        row.name,
                                        row.unit,
                                        row.data_type,
                                        row.description,
                                    );
                                }
                                OutputFormat::Jsonl => {
                                    let obj = serde_json::json!({
                                        "service": row.service,
                                        "name": row.name,
                                        "description": row.description,
                                        "unit": row.unit,
                                        "data_type": row.data_type,
                                    });
                                    println!("{}", serde_json::to_string(&obj)?);
                                }
                                OutputFormat::Csv => {
                                    if let Some(ref mut wtr) = csv_writer {
                                        wtr.write_record([
                                            &row.service,
                                            &row.name,
                                            &row.unit,
                                            &row.data_type,
                                            &row.description,
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
