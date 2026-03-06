use anyhow::Result;
use prost::Message;

use crate::cli::{ExportArgs, ExportFormat, ExportTarget};
use crate::client::hex_encode;
use crate::client::trace::format_timestamp_ns;
use crate::query_proto::query_service_client::QueryServiceClient;
use crate::query_proto::{QueryLogsRequest, QueryMetricsRequest, QueryTracesRequest};

pub async fn run(args: ExportArgs) -> Result<()> {
    let mut client = QueryServiceClient::connect(args.addr.clone()).await?;

    match args.target {
        ExportTarget::Traces => export_traces(&mut client, &args.output).await,
        ExportTarget::Logs => export_logs(&mut client, &args.output).await,
        ExportTarget::Metrics => export_metrics(&mut client, &args.output).await,
        ExportTarget::All => {
            export_traces(&mut client, &args.output).await?;
            export_logs(&mut client, &args.output).await?;
            export_metrics(&mut client, &args.output).await
        }
    }
}

async fn export_traces(
    client: &mut QueryServiceClient<tonic::transport::Channel>,
    format: &ExportFormat,
) -> Result<()> {
    let response = client
        .query_traces(QueryTracesRequest::default())
        .await?
        .into_inner();

    match format {
        ExportFormat::Proto => {
            use std::io::Write;
            let stdout = std::io::stdout();
            let mut lock = stdout.lock();
            for rs in &response.resource_spans {
                let bytes = rs.encode_to_vec();
                lock.write_all(&(bytes.len() as u32).to_be_bytes())?;
                lock.write_all(&bytes)?;
            }
            lock.flush()?;
        }
        ExportFormat::Jsonl => {
            for rs in &response.resource_spans {
                let service_name = extract_trace_service_name(rs);
                for ss in &rs.scope_spans {
                    for span in &ss.spans {
                        let obj = serde_json::json!({
                            "signal": "trace",
                            "time": format_timestamp_ns(span.start_time_unix_nano),
                            "service": service_name,
                            "span_name": span.name,
                            "duration_ms": (span.end_time_unix_nano.saturating_sub(span.start_time_unix_nano)) as f64 / 1_000_000.0,
                            "trace_id": hex_encode(&span.trace_id),
                            "span_id": hex_encode(&span.span_id),
                            "parent_span_id": hex_encode(&span.parent_span_id),
                            "kind": span.kind,
                            "status_code": span.status.as_ref().map_or(0, |s| s.code),
                            "status_message": span.status.as_ref().map_or("", |s| &s.message),
                        });
                        println!("{}", serde_json::to_string(&obj)?);
                    }
                }
            }
        }
        ExportFormat::Csv => {
            let mut wtr = csv::Writer::from_writer(std::io::stdout());
            wtr.write_record([
                "signal",
                "time",
                "service",
                "span_name",
                "duration_ms",
                "trace_id",
                "span_id",
                "parent_span_id",
                "kind",
                "status_code",
            ])?;
            for rs in &response.resource_spans {
                let service_name = extract_trace_service_name(rs);
                for ss in &rs.scope_spans {
                    for span in &ss.spans {
                        wtr.write_record([
                            "trace",
                            &format_timestamp_ns(span.start_time_unix_nano),
                            &service_name,
                            &span.name,
                            &format!(
                                "{:.3}",
                                (span
                                    .end_time_unix_nano
                                    .saturating_sub(span.start_time_unix_nano))
                                    as f64
                                    / 1_000_000.0
                            ),
                            &hex_encode(&span.trace_id),
                            &hex_encode(&span.span_id),
                            &hex_encode(&span.parent_span_id),
                            &span.kind.to_string(),
                            &span.status.as_ref().map_or(0, |s| s.code).to_string(),
                        ])?;
                    }
                }
            }
            wtr.flush()?;
        }
        ExportFormat::Text => {
            for rs in &response.resource_spans {
                let service_name = extract_trace_service_name(rs);
                for ss in &rs.scope_spans {
                    for span in &ss.spans {
                        let duration_ms = (span
                            .end_time_unix_nano
                            .saturating_sub(span.start_time_unix_nano))
                            as f64
                            / 1_000_000.0;
                        println!(
                            "{} {} {} {:.3}ms trace_id={}",
                            format_timestamp_ns(span.start_time_unix_nano),
                            service_name,
                            span.name,
                            duration_ms,
                            hex_encode(&span.trace_id),
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

async fn export_logs(
    client: &mut QueryServiceClient<tonic::transport::Channel>,
    format: &ExportFormat,
) -> Result<()> {
    let response = client
        .query_logs(QueryLogsRequest::default())
        .await?
        .into_inner();

    match format {
        ExportFormat::Proto => {
            use std::io::Write;
            let stdout = std::io::stdout();
            let mut lock = stdout.lock();
            for rl in &response.resource_logs {
                let bytes = rl.encode_to_vec();
                lock.write_all(&(bytes.len() as u32).to_be_bytes())?;
                lock.write_all(&bytes)?;
            }
            lock.flush()?;
        }
        ExportFormat::Jsonl => {
            for rl in &response.resource_logs {
                let service_name = extract_log_service_name(rl);
                for sl in &rl.scope_logs {
                    for lr in &sl.log_records {
                        let obj = serde_json::json!({
                            "signal": "log",
                            "time": format_timestamp_ns(lr.time_unix_nano),
                            "service": service_name,
                            "severity": format!("{:?}", lr.severity_number()),
                            "body": lr.body.as_ref().map(format_any_value).unwrap_or_default(),
                        });
                        println!("{}", serde_json::to_string(&obj)?);
                    }
                }
            }
        }
        ExportFormat::Csv => {
            let mut wtr = csv::Writer::from_writer(std::io::stdout());
            wtr.write_record(["signal", "time", "service", "severity", "body"])?;
            for rl in &response.resource_logs {
                let service_name = extract_log_service_name(rl);
                for sl in &rl.scope_logs {
                    for lr in &sl.log_records {
                        wtr.write_record([
                            "log",
                            &format_timestamp_ns(lr.time_unix_nano),
                            &service_name,
                            &format!("{:?}", lr.severity_number()),
                            &lr.body.as_ref().map(format_any_value).unwrap_or_default(),
                        ])?;
                    }
                }
            }
            wtr.flush()?;
        }
        ExportFormat::Text => {
            for rl in &response.resource_logs {
                let service_name = extract_log_service_name(rl);
                for sl in &rl.scope_logs {
                    for lr in &sl.log_records {
                        println!(
                            "{} {} [{:?}] {}",
                            format_timestamp_ns(lr.time_unix_nano),
                            service_name,
                            lr.severity_number(),
                            lr.body.as_ref().map(format_any_value).unwrap_or_default(),
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

async fn export_metrics(
    client: &mut QueryServiceClient<tonic::transport::Channel>,
    format: &ExportFormat,
) -> Result<()> {
    let response = client
        .query_metrics(QueryMetricsRequest::default())
        .await?
        .into_inner();

    match format {
        ExportFormat::Proto => {
            use std::io::Write;
            let stdout = std::io::stdout();
            let mut lock = stdout.lock();
            for rm in &response.resource_metrics {
                let bytes = rm.encode_to_vec();
                lock.write_all(&(bytes.len() as u32).to_be_bytes())?;
                lock.write_all(&bytes)?;
            }
            lock.flush()?;
        }
        ExportFormat::Jsonl => {
            for rm in &response.resource_metrics {
                let service_name = extract_metric_service_name(rm);
                for sm in &rm.scope_metrics {
                    for metric in &sm.metrics {
                        let data_point_count = count_data_points(&metric.data);
                        let obj = serde_json::json!({
                            "signal": "metric",
                            "service": service_name,
                            "metric_name": metric.name,
                            "type": describe_metric_data(&metric.data),
                            "unit": metric.unit,
                            "description": metric.description,
                            "data_point_count": data_point_count,
                        });
                        println!("{}", serde_json::to_string(&obj)?);
                    }
                }
            }
        }
        ExportFormat::Csv => {
            let mut wtr = csv::Writer::from_writer(std::io::stdout());
            wtr.write_record([
                "signal",
                "service",
                "metric_name",
                "type",
                "unit",
                "description",
                "data_point_count",
            ])?;
            for rm in &response.resource_metrics {
                let service_name = extract_metric_service_name(rm);
                for sm in &rm.scope_metrics {
                    for metric in &sm.metrics {
                        let data_point_count = count_data_points(&metric.data);
                        wtr.write_record([
                            "metric",
                            &service_name,
                            &metric.name,
                            &describe_metric_data(&metric.data),
                            &metric.unit,
                            &metric.description,
                            &data_point_count.to_string(),
                        ])?;
                    }
                }
            }
            wtr.flush()?;
        }
        ExportFormat::Text => {
            for rm in &response.resource_metrics {
                let service_name = extract_metric_service_name(rm);
                for sm in &rm.scope_metrics {
                    for metric in &sm.metrics {
                        println!(
                            "{} {} ({}) [{}] {}",
                            service_name,
                            metric.name,
                            metric.unit,
                            describe_metric_data(&metric.data),
                            metric.description,
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

fn extract_trace_service_name(rs: &crate::otel::trace::v1::ResourceSpans) -> String {
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

fn extract_log_service_name(rl: &crate::otel::logs::v1::ResourceLogs) -> String {
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

fn extract_metric_service_name(rm: &crate::otel::metrics::v1::ResourceMetrics) -> String {
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

fn count_data_points(data: &Option<crate::otel::metrics::v1::metric::Data>) -> usize {
    match data {
        Some(crate::otel::metrics::v1::metric::Data::Gauge(g)) => g.data_points.len(),
        Some(crate::otel::metrics::v1::metric::Data::Sum(s)) => s.data_points.len(),
        Some(crate::otel::metrics::v1::metric::Data::Histogram(h)) => h.data_points.len(),
        Some(crate::otel::metrics::v1::metric::Data::ExponentialHistogram(h)) => {
            h.data_points.len()
        }
        Some(crate::otel::metrics::v1::metric::Data::Summary(s)) => s.data_points.len(),
        None => 0,
    }
}
