use std::io::BufRead;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use prost::Message;

use crate::cli::{ImportArgs, ImportFormat, SignalType};
use crate::otel::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
};
use crate::otel::collector::metrics::v1::{
    ExportMetricsServiceRequest, metrics_service_client::MetricsServiceClient,
};
use crate::otel::collector::trace::v1::{
    ExportTraceServiceRequest, trace_service_client::TraceServiceClient,
};
use crate::otel::common::v1::{AnyValue, KeyValue, any_value::Value};
use crate::otel::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use crate::otel::metrics::v1::ResourceMetrics;
use crate::otel::resource::v1::Resource;
use crate::otel::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

use crate::client::hex_decode;

pub async fn run(args: ImportArgs) -> Result<()> {
    let mut total_imported = 0u64;

    for file_pattern in &args.files {
        let paths = expand_paths(file_pattern)?;

        for path in paths {
            let format = args
                .format
                .clone()
                .unwrap_or_else(|| detect_format(&path));
            let signal = args
                .signal
                .clone()
                .unwrap_or_else(|| detect_signal(&path));

            eprintln!(
                "Importing {} ({:?}/{:?})...",
                path.display(),
                format,
                signal
            );

            let count = import_file(&path, &format, &signal, &args.addr, args.batch_size)
                .await
                .with_context(|| format!("failed to import {}", path.display()))?;

            eprintln!("  Imported {} records", count);
            total_imported += count;
        }
    }

    eprintln!("Total imported: {} records", total_imported);
    Ok(())
}

fn detect_format(path: &Path) -> ImportFormat {
    match path.extension().and_then(|e| e.to_str()) {
        Some("jsonl") | Some("json") | Some("ndjson") => ImportFormat::Jsonl,
        Some("pb") | Some("proto") | Some("bin") => ImportFormat::OtlpProto,
        _ => ImportFormat::Jsonl, // default
    }
}

fn detect_signal(path: &Path) -> SignalType {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    if stem.contains("log") {
        SignalType::Logs
    } else if stem.contains("metric") {
        SignalType::Metrics
    } else {
        // Default to traces (also matches "trace" and "span" in filename)
        SignalType::Traces
    }
}

fn expand_paths(pattern: &str) -> Result<Vec<PathBuf>> {
    let path = Path::new(pattern);
    if path.exists() {
        return Ok(vec![path.to_path_buf()]);
    }
    // Try glob expansion
    let entries: Vec<PathBuf> = glob::glob(pattern)
        .with_context(|| format!("invalid glob pattern: {}", pattern))?
        .filter_map(|r| r.ok())
        .collect();
    if entries.is_empty() {
        bail!("No files match pattern: {}", pattern);
    }
    Ok(entries)
}

async fn import_file(
    path: &Path,
    format: &ImportFormat,
    signal: &SignalType,
    addr: &str,
    batch_size: usize,
) -> Result<u64> {
    match (format, signal) {
        (ImportFormat::Jsonl, SignalType::Traces) => {
            import_jsonl_traces(path, addr, batch_size).await
        }
        (ImportFormat::Jsonl, SignalType::Logs) => {
            import_jsonl_logs(path, addr, batch_size).await
        }
        (ImportFormat::Jsonl, SignalType::Metrics) => {
            import_jsonl_metrics(path, addr, batch_size).await
        }
        (ImportFormat::OtlpProto, SignalType::Traces) => {
            import_proto_traces(path, addr, batch_size).await
        }
        (ImportFormat::OtlpProto, SignalType::Logs) => {
            import_proto_logs(path, addr, batch_size).await
        }
        (ImportFormat::OtlpProto, SignalType::Metrics) => {
            import_proto_metrics(path, addr, batch_size).await
        }
    }
}

// ---------------------------------------------------------------------------
// JSONL trace import
// ---------------------------------------------------------------------------

async fn import_jsonl_traces(path: &Path, addr: &str, batch_size: usize) -> Result<u64> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let mut client = TraceServiceClient::connect(addr.to_string()).await?;
    let mut count = 0u64;
    let mut batch: Vec<ResourceSpans> = Vec::new();

    for (line_num, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let row: serde_json::Value = serde_json::from_str(&line)
            .with_context(|| format!("invalid JSON on line {}", line_num + 1))?;

        let rs = jsonl_row_to_resource_spans(&row)?;
        batch.push(rs);

        if batch.len() >= batch_size {
            let n = batch.len() as u64;
            send_trace_batch(&mut client, &mut batch).await?;
            count += n;
        }
    }

    if !batch.is_empty() {
        let remaining = batch.len() as u64;
        send_trace_batch(&mut client, &mut batch).await?;
        count += remaining;
    }

    Ok(count)
}

fn jsonl_row_to_resource_spans(row: &serde_json::Value) -> Result<ResourceSpans> {
    let service = row["service"].as_str().unwrap_or("unknown").to_string();
    let span_name = row["span_name"].as_str().unwrap_or("").to_string();
    let trace_id = row["trace_id"]
        .as_str()
        .map(hex_decode)
        .transpose()?
        .unwrap_or_else(|| vec![0u8; 16]);
    let span_id = row["span_id"]
        .as_str()
        .map(hex_decode)
        .transpose()?
        .unwrap_or_else(|| vec![0u8; 8]);
    let duration_ms = row["duration_ms"].as_f64().unwrap_or(0.0);
    let time_str = row["time"].as_str().unwrap_or("");

    let start_time_unix_nano = parse_timestamp_to_nanos(time_str)?;
    let duration_ns = (duration_ms * 1_000_000.0) as u64;
    let end_time_unix_nano = start_time_unix_nano + duration_ns;

    let status_code = match row["status"].as_str() {
        Some("Ok") => 1,
        Some("Error") => 2,
        _ => 0, // Unset
    };

    Ok(ResourceSpans {
        resource: Some(Resource {
            attributes: vec![KeyValue {
                key: "service.name".into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(service)),
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        scope_spans: vec![ScopeSpans {
            scope: None,
            spans: vec![Span {
                trace_id,
                span_id,
                name: span_name,
                start_time_unix_nano,
                end_time_unix_nano,
                status: Some(Status {
                    code: status_code,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    })
}

async fn send_trace_batch(
    client: &mut TraceServiceClient<tonic::transport::Channel>,
    batch: &mut Vec<ResourceSpans>,
) -> Result<()> {
    let request = ExportTraceServiceRequest {
        resource_spans: std::mem::take(batch),
    };
    client
        .export(request)
        .await
        .context("failed to send trace batch")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// JSONL log import
// ---------------------------------------------------------------------------

async fn import_jsonl_logs(path: &Path, addr: &str, batch_size: usize) -> Result<u64> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let mut client = LogsServiceClient::connect(addr.to_string()).await?;
    let mut count = 0u64;
    let mut batch: Vec<ResourceLogs> = Vec::new();

    for (line_num, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let row: serde_json::Value = serde_json::from_str(&line)
            .with_context(|| format!("invalid JSON on line {}", line_num + 1))?;

        let rl = jsonl_row_to_resource_logs(&row)?;
        batch.push(rl);

        if batch.len() >= batch_size {
            let n = batch.len() as u64;
            send_logs_batch(&mut client, &mut batch).await?;
            count += n;
        }
    }

    if !batch.is_empty() {
        let remaining = batch.len() as u64;
        send_logs_batch(&mut client, &mut batch).await?;
        count += remaining;
    }

    Ok(count)
}

fn jsonl_row_to_resource_logs(row: &serde_json::Value) -> Result<ResourceLogs> {
    let service = row["service"].as_str().unwrap_or("unknown").to_string();
    let body_text = row["body"].as_str().unwrap_or("").to_string();
    let severity_text = row["severity"].as_str().unwrap_or("").to_string();
    let time_str = row["time"].as_str().unwrap_or("");

    let time_unix_nano = parse_timestamp_to_nanos(time_str)?;
    let severity_number = severity_text_to_number(&severity_text);

    Ok(ResourceLogs {
        resource: Some(Resource {
            attributes: vec![KeyValue {
                key: "service.name".into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(service)),
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        scope_logs: vec![ScopeLogs {
            scope: None,
            log_records: vec![LogRecord {
                time_unix_nano,
                observed_time_unix_nano: time_unix_nano,
                severity_number,
                severity_text,
                body: Some(AnyValue {
                    value: Some(Value::StringValue(body_text)),
                }),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    })
}

/// Convert severity text to OTLP severity number.
/// Uses the standard OpenTelemetry severity number mapping.
fn severity_text_to_number(text: &str) -> i32 {
    // The log export format uses the Debug-style enum names from prost,
    // e.g. "SeverityNumberInfo" or just severity text like "INFO".
    // Handle both styles.
    let upper = text.to_uppercase();
    if upper.contains("TRACE") {
        1 // SEVERITY_NUMBER_TRACE
    } else if upper.contains("DEBUG") {
        5 // SEVERITY_NUMBER_DEBUG
    } else if upper.contains("INFO") {
        9 // SEVERITY_NUMBER_INFO
    } else if upper.contains("WARN") {
        13 // SEVERITY_NUMBER_WARN
    } else if upper.contains("ERROR") {
        17 // SEVERITY_NUMBER_ERROR
    } else if upper.contains("FATAL") {
        21 // SEVERITY_NUMBER_FATAL
    } else {
        0 // SEVERITY_NUMBER_UNSPECIFIED
    }
}

async fn send_logs_batch(
    client: &mut LogsServiceClient<tonic::transport::Channel>,
    batch: &mut Vec<ResourceLogs>,
) -> Result<()> {
    let request = ExportLogsServiceRequest {
        resource_logs: std::mem::take(batch),
    };
    client
        .export(request)
        .await
        .context("failed to send logs batch")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// JSONL metrics import
// ---------------------------------------------------------------------------

async fn import_jsonl_metrics(path: &Path, addr: &str, batch_size: usize) -> Result<u64> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let mut client = MetricsServiceClient::connect(addr.to_string()).await?;
    let mut count = 0u64;
    let mut batch: Vec<ResourceMetrics> = Vec::new();

    for (line_num, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let row: serde_json::Value = serde_json::from_str(&line)
            .with_context(|| format!("invalid JSON on line {}", line_num + 1))?;

        let rm = jsonl_row_to_resource_metrics(&row)?;
        batch.push(rm);

        if batch.len() >= batch_size {
            let n = batch.len() as u64;
            send_metrics_batch(&mut client, &mut batch).await?;
            count += n;
        }
    }

    if !batch.is_empty() {
        let remaining = batch.len() as u64;
        send_metrics_batch(&mut client, &mut batch).await?;
        count += remaining;
    }

    Ok(count)
}

fn jsonl_row_to_resource_metrics(row: &serde_json::Value) -> Result<ResourceMetrics> {
    use crate::otel::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ScopeMetrics, number_data_point,
    };

    let service = row["service"].as_str().unwrap_or("unknown").to_string();
    let name = row["name"].as_str().unwrap_or("").to_string();
    let description = row["description"].as_str().unwrap_or("").to_string();
    let unit = row["unit"].as_str().unwrap_or("").to_string();

    // For JSONL re-import, we create a Gauge with a zero data point as a placeholder.
    // The JSONL metrics export format doesn't include full data point details.
    Ok(ResourceMetrics {
        resource: Some(Resource {
            attributes: vec![KeyValue {
                key: "service.name".into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(service)),
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        scope_metrics: vec![ScopeMetrics {
            scope: None,
            metrics: vec![Metric {
                name,
                description,
                unit,
                data: Some(crate::otel::metrics::v1::metric::Data::Gauge(Gauge {
                    data_points: vec![NumberDataPoint {
                        time_unix_nano: 1_000_000_000,
                        value: Some(number_data_point::Value::AsDouble(0.0)),
                        ..Default::default()
                    }],
                })),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    })
}

async fn send_metrics_batch(
    client: &mut MetricsServiceClient<tonic::transport::Channel>,
    batch: &mut Vec<ResourceMetrics>,
) -> Result<()> {
    let request = ExportMetricsServiceRequest {
        resource_metrics: std::mem::take(batch),
    };
    client
        .export(request)
        .await
        .context("failed to send metrics batch")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// OTLP protobuf import
// ---------------------------------------------------------------------------

async fn import_proto_traces(path: &Path, addr: &str, batch_size: usize) -> Result<u64> {
    let bytes = std::fs::read(path)?;
    let request = ExportTraceServiceRequest::decode(bytes.as_slice())
        .context("failed to decode protobuf as ExportTraceServiceRequest")?;

    let mut client = TraceServiceClient::connect(addr.to_string()).await?;
    let total = request.resource_spans.len() as u64;

    for chunk in request.resource_spans.chunks(batch_size) {
        let batch_request = ExportTraceServiceRequest {
            resource_spans: chunk.to_vec(),
        };
        client
            .export(batch_request)
            .await
            .context("failed to send protobuf trace batch")?;
    }

    Ok(total)
}

async fn import_proto_logs(path: &Path, addr: &str, batch_size: usize) -> Result<u64> {
    let bytes = std::fs::read(path)?;
    let request = ExportLogsServiceRequest::decode(bytes.as_slice())
        .context("failed to decode protobuf as ExportLogsServiceRequest")?;

    let mut client = LogsServiceClient::connect(addr.to_string()).await?;
    let total = request.resource_logs.len() as u64;

    for chunk in request.resource_logs.chunks(batch_size) {
        let batch_request = ExportLogsServiceRequest {
            resource_logs: chunk.to_vec(),
        };
        client
            .export(batch_request)
            .await
            .context("failed to send protobuf logs batch")?;
    }

    Ok(total)
}

async fn import_proto_metrics(path: &Path, addr: &str, batch_size: usize) -> Result<u64> {
    let bytes = std::fs::read(path)?;
    let request = ExportMetricsServiceRequest::decode(bytes.as_slice())
        .context("failed to decode protobuf as ExportMetricsServiceRequest")?;

    let mut client = MetricsServiceClient::connect(addr.to_string()).await?;
    let total = request.resource_metrics.len() as u64;

    for chunk in request.resource_metrics.chunks(batch_size) {
        let batch_request = ExportMetricsServiceRequest {
            resource_metrics: chunk.to_vec(),
        };
        client
            .export(batch_request)
            .await
            .context("failed to send protobuf metrics batch")?;
    }

    Ok(total)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_timestamp_to_nanos(s: &str) -> Result<u64> {
    if s.is_empty() {
        return Ok(0);
    }
    use chrono::DateTime;
    let dt = DateTime::parse_from_rfc3339(s)
        .with_context(|| format!("invalid timestamp: {}", s))?;
    Ok(dt.timestamp_nanos_opt().unwrap_or(0) as u64)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_format_jsonl() {
        assert!(matches!(
            detect_format(Path::new("traces.jsonl")),
            ImportFormat::Jsonl
        ));
        assert!(matches!(
            detect_format(Path::new("data.ndjson")),
            ImportFormat::Jsonl
        ));
        assert!(matches!(
            detect_format(Path::new("data.json")),
            ImportFormat::Jsonl
        ));
    }

    #[test]
    fn test_detect_format_proto() {
        assert!(matches!(
            detect_format(Path::new("traces.pb")),
            ImportFormat::OtlpProto
        ));
        assert!(matches!(
            detect_format(Path::new("data.proto")),
            ImportFormat::OtlpProto
        ));
        assert!(matches!(
            detect_format(Path::new("data.bin")),
            ImportFormat::OtlpProto
        ));
    }

    #[test]
    fn test_detect_format_default() {
        assert!(matches!(
            detect_format(Path::new("data.txt")),
            ImportFormat::Jsonl
        ));
    }

    #[test]
    fn test_detect_signal_from_filename() {
        assert!(matches!(
            detect_signal(Path::new("traces.jsonl")),
            SignalType::Traces
        ));
        assert!(matches!(
            detect_signal(Path::new("my-logs.jsonl")),
            SignalType::Logs
        ));
        assert!(matches!(
            detect_signal(Path::new("metrics_export.pb")),
            SignalType::Metrics
        ));
        assert!(matches!(
            detect_signal(Path::new("spans.jsonl")),
            SignalType::Traces
        ));
    }

    #[test]
    fn test_jsonl_row_to_resource_spans() {
        let row = serde_json::json!({
            "time": "2024-01-15T10:30:00.000Z",
            "service": "test-svc",
            "span_name": "GET /api",
            "duration_ms": 50.2,
            "trace_id": "0102030405060708090a0b0c0d0e0f10",
            "span_id": "0102030405060708",
            "status": "Ok"
        });

        let rs = jsonl_row_to_resource_spans(&row).unwrap();
        assert_eq!(rs.scope_spans[0].spans[0].name, "GET /api");
        assert_eq!(rs.scope_spans[0].spans[0].trace_id.len(), 16);
        assert!(
            rs.scope_spans[0].spans[0].end_time_unix_nano
                > rs.scope_spans[0].spans[0].start_time_unix_nano
        );
        // Verify status code 1 = Ok
        assert_eq!(rs.scope_spans[0].spans[0].status.as_ref().unwrap().code, 1);
    }

    #[test]
    fn test_jsonl_row_to_resource_spans_minimal() {
        let row = serde_json::json!({
            "time": "2024-01-15T10:30:00.000Z",
            "span_name": "test"
        });
        let rs = jsonl_row_to_resource_spans(&row).unwrap();
        assert_eq!(rs.scope_spans[0].spans[0].name, "test");
    }

    #[test]
    fn test_jsonl_row_to_resource_logs() {
        let row = serde_json::json!({
            "time": "2024-01-15T10:30:00.000Z",
            "service": "test-svc",
            "severity": "SeverityNumberInfo",
            "body": "hello world"
        });
        let rl = jsonl_row_to_resource_logs(&row).unwrap();
        let lr = &rl.scope_logs[0].log_records[0];
        assert_eq!(lr.severity_number, 9);
        assert!(lr.body.is_some());
    }

    #[test]
    fn test_parse_timestamp_to_nanos() {
        let ns = parse_timestamp_to_nanos("2024-01-15T10:30:00.000Z").unwrap();
        assert!(ns > 0);
    }

    #[test]
    fn test_parse_timestamp_to_nanos_empty() {
        let ns = parse_timestamp_to_nanos("").unwrap();
        assert_eq!(ns, 0);
    }

    #[test]
    fn test_parse_timestamp_invalid() {
        assert!(parse_timestamp_to_nanos("not-a-date").is_err());
    }

    #[test]
    fn test_severity_text_to_number() {
        assert_eq!(severity_text_to_number("INFO"), 9);
        assert_eq!(severity_text_to_number("SeverityNumberInfo"), 9);
        assert_eq!(severity_text_to_number("WARN"), 13);
        assert_eq!(severity_text_to_number("ERROR"), 17);
        assert_eq!(severity_text_to_number("DEBUG"), 5);
        assert_eq!(severity_text_to_number("TRACE"), 1);
        assert_eq!(severity_text_to_number("FATAL"), 21);
        assert_eq!(severity_text_to_number(""), 0);
    }
}
