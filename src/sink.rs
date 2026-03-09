use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use prost::Message;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;

use crate::cli::SinkFormat;
use crate::client::hex_encode;
use crate::client::trace::format_timestamp_ns;
use crate::otel::logs::v1::ResourceLogs;
use crate::otel::metrics::v1::ResourceMetrics;
use crate::otel::trace::v1::ResourceSpans;
use crate::store::StoreEvent;

struct RotatingWriter {
    base_dir: PathBuf,
    prefix: String,
    extension: String,
    file: Option<File>,
    current_size: u64,
    file_opened_at: Instant,
    max_size: u64,
    rotate_interval: Duration,
}

impl RotatingWriter {
    fn new(
        base_dir: &Path,
        prefix: &str,
        format: &SinkFormat,
        max_size: u64,
        rotate_interval: Duration,
    ) -> Self {
        let extension = match format {
            SinkFormat::Jsonl => ".jsonl".to_string(),
            SinkFormat::Proto => ".bin".to_string(),
        };
        Self {
            base_dir: base_dir.to_path_buf(),
            prefix: prefix.to_string(),
            extension,
            file: None,
            current_size: 0,
            file_opened_at: Instant::now(),
            max_size,
            rotate_interval,
        }
    }

    fn needs_rotation(&self) -> bool {
        self.current_size >= self.max_size || self.file_opened_at.elapsed() >= self.rotate_interval
    }

    fn next_filename(&self) -> String {
        let ts = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
        format!("{}_{}{}", self.prefix, ts, self.extension)
    }

    async fn rotate(&mut self) -> Result<()> {
        // Flush and close the current file
        if let Some(ref mut f) = self.file {
            f.flush().await.context("flushing sink file")?;
        }
        self.file = None;

        let filename = self.next_filename();
        let path = self.base_dir.join(&filename);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .with_context(|| format!("opening sink file {}", path.display()))?;
        tracing::info!("Sink: opened {}", path.display());
        self.file = Some(file);
        self.current_size = 0;
        self.file_opened_at = Instant::now();
        Ok(())
    }

    async fn write(&mut self, data: &[u8]) -> Result<()> {
        if self.needs_rotation() || self.file.is_none() {
            self.rotate().await?;
        }
        if let Some(ref mut f) = self.file {
            f.write_all(data).await.context("writing to sink file")?;
            self.current_size += data.len() as u64;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if let Some(ref mut f) = self.file {
            f.flush().await.context("flushing sink file")?;
        }
        Ok(())
    }
}

/// Parse a human-friendly duration string like "30s", "5m", "1h", "2d", "500ms".
///
/// Delegates to the canonical [`crate::cli::parse_duration_arg`].
pub fn parse_duration(s: &str) -> Result<Duration> {
    crate::cli::parse_duration_arg(s).map_err(|e| anyhow::anyhow!("{e}"))
}

fn encode_resource_spans(rs: &ResourceSpans, format: &SinkFormat) -> Result<Vec<u8>> {
    match format {
        SinkFormat::Jsonl => {
            let service_name = extract_trace_service_name(rs);
            let mut lines = Vec::new();
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
                    let mut line = serde_json::to_string(&obj)?;
                    line.push('\n');
                    lines.extend_from_slice(line.as_bytes());
                }
            }
            Ok(lines)
        }
        SinkFormat::Proto => encode_length_prefixed(rs),
    }
}

fn encode_resource_logs(rl: &ResourceLogs, format: &SinkFormat) -> Result<Vec<u8>> {
    match format {
        SinkFormat::Jsonl => {
            let service_name = extract_log_service_name(rl);
            let mut lines = Vec::new();
            for sl in &rl.scope_logs {
                for lr in &sl.log_records {
                    let obj = serde_json::json!({
                        "signal": "log",
                        "time": format_timestamp_ns(lr.time_unix_nano),
                        "service": service_name,
                        "severity": format!("{:?}", lr.severity_number()),
                        "body": lr.body.as_ref().map(format_any_value).unwrap_or_default(),
                    });
                    let mut line = serde_json::to_string(&obj)?;
                    line.push('\n');
                    lines.extend_from_slice(line.as_bytes());
                }
            }
            Ok(lines)
        }
        SinkFormat::Proto => encode_length_prefixed(rl),
    }
}

fn encode_resource_metrics(rm: &ResourceMetrics, format: &SinkFormat) -> Result<Vec<u8>> {
    match format {
        SinkFormat::Jsonl => {
            let service_name = extract_metric_service_name(rm);
            let mut lines = Vec::new();
            for sm in &rm.scope_metrics {
                for metric in &sm.metrics {
                    let obj = serde_json::json!({
                        "signal": "metric",
                        "service": service_name,
                        "metric_name": metric.name,
                        "type": describe_metric_data(&metric.data),
                        "unit": metric.unit,
                        "description": metric.description,
                    });
                    let mut line = serde_json::to_string(&obj)?;
                    line.push('\n');
                    lines.extend_from_slice(line.as_bytes());
                }
            }
            Ok(lines)
        }
        SinkFormat::Proto => encode_length_prefixed(rm),
    }
}

fn encode_length_prefixed<T: Message>(item: &T) -> Result<Vec<u8>> {
    let proto_bytes = item.encode_to_vec();
    let len = (proto_bytes.len() as u32).to_be_bytes();
    let mut buf = Vec::with_capacity(4 + proto_bytes.len());
    buf.extend_from_slice(&len);
    buf.extend_from_slice(&proto_bytes);
    Ok(buf)
}

/// Run the file sink event loop. Listens on the broadcast channel and writes
/// incoming OTLP data to rotating files on disk.
pub async fn run(
    mut event_rx: broadcast::Receiver<StoreEvent>,
    base_dir: PathBuf,
    format: SinkFormat,
    max_size: u64,
    rotate_interval: Duration,
) -> Result<()> {
    fs::create_dir_all(&base_dir)
        .await
        .with_context(|| format!("creating sink directory {}", base_dir.display()))?;

    let mut traces_writer =
        RotatingWriter::new(&base_dir, "traces", &format, max_size, rotate_interval);
    let mut logs_writer =
        RotatingWriter::new(&base_dir, "logs", &format, max_size, rotate_interval);
    let mut metrics_writer =
        RotatingWriter::new(&base_dir, "metrics", &format, max_size, rotate_interval);

    tracing::info!("Sink: writing to {}", base_dir.display());

    loop {
        match event_rx.recv().await {
            Ok(StoreEvent::TracesInserted(resource_spans)) => {
                for rs in &resource_spans {
                    let data = encode_resource_spans(rs, &format)?;
                    traces_writer.write(&data).await?;
                }
                traces_writer.flush().await?;
            }
            Ok(StoreEvent::LogsInserted(resource_logs)) => {
                for rl in &resource_logs {
                    let data = encode_resource_logs(rl, &format)?;
                    logs_writer.write(&data).await?;
                }
                logs_writer.flush().await?;
            }
            Ok(StoreEvent::MetricsInserted(resource_metrics)) => {
                for rm in &resource_metrics {
                    let data = encode_resource_metrics(rm, &format)?;
                    metrics_writer.write(&data).await?;
                }
                metrics_writer.flush().await?;
            }
            Ok(_) => {} // Ignore clear events
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!("Sink lagged by {n} events — some data may not have been written");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => {
                tracing::info!("Sink: event channel closed, shutting down");
                break;
            }
        }
    }

    // Final flush
    traces_writer.flush().await?;
    logs_writer.flush().await?;
    metrics_writer.flush().await?;

    Ok(())
}

// Helper functions for extracting service names and formatting values,
// mirroring the patterns in src/client/export.rs.

fn extract_trace_service_name(rs: &ResourceSpans) -> String {
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

fn extract_log_service_name(rl: &ResourceLogs) -> String {
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

fn extract_metric_service_name(rm: &ResourceMetrics) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration("2d").unwrap(), Duration::from_secs(172800));
    }

    #[test]
    fn test_parse_duration_invalid_unit() {
        assert!(parse_duration("5x").is_err());
    }

    #[test]
    fn test_parse_duration_invalid_number() {
        assert!(parse_duration("abch").is_err());
    }

    #[test]
    fn test_parse_duration_too_short() {
        assert!(parse_duration("s").is_err());
    }

    #[test]
    fn test_encode_length_prefixed() {
        use crate::otel::trace::v1::ResourceSpans;
        let rs = ResourceSpans::default();
        let encoded = encode_length_prefixed(&rs).unwrap();
        // Empty message encodes to 0 bytes of protobuf
        let len = u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert_eq!(len as usize, encoded.len() - 4);
        // Verify we can decode the protobuf portion back
        let decoded = ResourceSpans::decode(&encoded[4..]).unwrap();
        assert_eq!(decoded, rs);
    }

    #[test]
    fn test_encode_resource_spans_jsonl() {
        use crate::store::tests::make_resource_spans;
        let rs = make_resource_spans(&[0xAB; 16], "test-span");
        let data = encode_resource_spans(&rs, &SinkFormat::Jsonl).unwrap();
        let line = String::from_utf8(data).unwrap();
        assert!(line.ends_with('\n'));
        let parsed: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(parsed["signal"], "trace");
        assert_eq!(parsed["span_name"], "test-span");
        assert_eq!(parsed["service"], "test-service");
    }

    #[test]
    fn test_encode_resource_spans_proto() {
        use crate::store::tests::make_resource_spans;
        let rs = make_resource_spans(&[0xAB; 16], "test-span");
        let data = encode_resource_spans(&rs, &SinkFormat::Proto).unwrap();
        // Verify length prefix
        let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        assert_eq!(len as usize, data.len() - 4);
        // Verify we can decode back
        let decoded = ResourceSpans::decode(&data[4..]).unwrap();
        assert_eq!(decoded.scope_spans[0].spans[0].name, "test-span");
    }

    #[tokio::test]
    async fn test_rotating_writer_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = RotatingWriter::new(
            dir.path(),
            "traces",
            &SinkFormat::Jsonl,
            1024,
            Duration::from_secs(3600),
        );
        writer.write(b"test line\n").await.unwrap();
        writer.flush().await.unwrap();

        // Verify a file was created
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 1);
        let filename = entries[0].file_name().to_string_lossy().to_string();
        assert!(filename.starts_with("traces_"));
        assert!(filename.ends_with(".jsonl"));
    }

    #[tokio::test]
    async fn test_rotating_writer_rotates_by_size() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = RotatingWriter::new(
            dir.path(),
            "traces",
            &SinkFormat::Jsonl,
            20, // Very small max to trigger rotation
            Duration::from_secs(3600),
        );

        // Write enough to trigger rotation
        writer.write(b"twelve chars!\n").await.unwrap();
        writer.write(b"more data here\n").await.unwrap();
        // This write should trigger rotation since we exceeded 20 bytes
        // We need to allow time difference in filenames
        tokio::time::sleep(Duration::from_millis(1100)).await;
        writer.write(b"after rotation\n").await.unwrap();
        writer.flush().await.unwrap();

        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            entries.len() >= 2,
            "expected at least 2 files after rotation, got {}",
            entries.len()
        );
    }

    #[tokio::test]
    async fn test_sink_run_writes_events() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, rx) = broadcast::channel(16);

        let base_dir = dir.path().to_path_buf();
        let handle = tokio::spawn(async move {
            run(
                rx,
                base_dir,
                SinkFormat::Jsonl,
                104857600,
                Duration::from_secs(3600),
            )
            .await
        });

        // Send a trace event
        use crate::store::tests::make_resource_spans;
        let rs = make_resource_spans(&[1; 16], "sink-test");
        tx.send(StoreEvent::TracesInserted(vec![rs])).unwrap();

        // Give the sink task time to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Drop sender to close the channel
        drop(tx);

        // Wait for task to finish
        handle.await.unwrap().unwrap();

        // Verify traces file was created with content
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("traces_"))
            .collect();
        assert_eq!(entries.len(), 1);

        let content = std::fs::read_to_string(entries[0].path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
        assert_eq!(parsed["signal"], "trace");
        assert_eq!(parsed["span_name"], "sink-test");
    }
}
