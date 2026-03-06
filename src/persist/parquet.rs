use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{Context, Result};
use prost::Message;

use crate::otel::{
    logs::v1::ResourceLogs, metrics::v1::ResourceMetrics, trace::v1::ResourceSpans,
};

use super::PersistBackend;

/// Parquet persistence backend.
///
/// Stores protobuf-encoded BLOBs in Parquet files (one file per signal type).
/// Uses a simple single-column schema with protobuf bytes for lossless
/// round-tripping, similar to the SQLite backend.
///
/// Files are stored as `{dir}/traces.parquet`, `{dir}/logs.parquet`,
/// `{dir}/metrics.parquet`.
pub struct ParquetPersist {
    dir: PathBuf,
    /// Accumulated trace data for appending to parquet on write.
    trace_buf: Mutex<Vec<Vec<u8>>>,
    log_buf: Mutex<Vec<Vec<u8>>>,
    metric_buf: Mutex<Vec<Vec<u8>>>,
}

impl ParquetPersist {
    /// Open (or create) a Parquet persistence directory.
    pub fn open(dir: &str) -> Result<Self> {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir).context("failed to create Parquet persistence directory")?;

        // Load existing data from parquet files into buffers so that
        // subsequent writes append to existing data.
        let trace_buf = Self::load_raw_from_file(&dir.join("traces.parquet"))?;
        let log_buf = Self::load_raw_from_file(&dir.join("logs.parquet"))?;
        let metric_buf = Self::load_raw_from_file(&dir.join("metrics.parquet"))?;

        Ok(Self {
            dir,
            trace_buf: Mutex::new(trace_buf),
            log_buf: Mutex::new(log_buf),
            metric_buf: Mutex::new(metric_buf),
        })
    }

    /// Read all protobuf blobs from a parquet file (single binary column "data").
    fn load_raw_from_file(path: &std::path::Path) -> Result<Vec<Vec<u8>>> {
        use arrow::array::{Array, BinaryArray};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = std::fs::File::open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("failed to read parquet {}", path.display()))?;
        let reader = builder.build()?;

        let mut result = Vec::new();
        for batch in reader {
            let batch = batch?;
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context("expected binary column")?;
            for i in 0..col.len() {
                result.push(col.value(i).to_vec());
            }
        }
        Ok(result)
    }

    /// Write all buffered blobs to a parquet file (overwrites).
    fn flush_to_file(path: &std::path::Path, blobs: &[Vec<u8>]) -> Result<()> {
        use arrow::array::BinaryArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;

        let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::Binary,
            false,
        )]));

        let values: Vec<&[u8]> = blobs.iter().map(|b| b.as_slice()).collect();
        let array = BinaryArray::from(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![std::sync::Arc::new(array)])?;

        let file = std::fs::File::create(path)
            .with_context(|| format!("failed to create {}", path.display()))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl PersistBackend for ParquetPersist {
    async fn write_traces(&self, data: &[ResourceSpans]) -> Result<()> {
        let path = self.dir.join("traces.parquet");
        let mut buf = self.trace_buf.lock().unwrap();
        for rs in data {
            buf.push(rs.encode_to_vec());
        }
        Self::flush_to_file(&path, &buf)?;
        Ok(())
    }

    async fn write_logs(&self, data: &[ResourceLogs]) -> Result<()> {
        let path = self.dir.join("logs.parquet");
        let mut buf = self.log_buf.lock().unwrap();
        for rl in data {
            buf.push(rl.encode_to_vec());
        }
        Self::flush_to_file(&path, &buf)?;
        Ok(())
    }

    async fn write_metrics(&self, data: &[ResourceMetrics]) -> Result<()> {
        let path = self.dir.join("metrics.parquet");
        let mut buf = self.metric_buf.lock().unwrap();
        for rm in data {
            buf.push(rm.encode_to_vec());
        }
        Self::flush_to_file(&path, &buf)?;
        Ok(())
    }

    async fn load_traces(&self) -> Result<Vec<ResourceSpans>> {
        let buf = self.trace_buf.lock().unwrap();
        let mut result = Vec::with_capacity(buf.len());
        for bytes in buf.iter() {
            result.push(
                ResourceSpans::decode(bytes.as_slice())
                    .context("failed to decode persisted ResourceSpans")?,
            );
        }
        Ok(result)
    }

    async fn load_logs(&self) -> Result<Vec<ResourceLogs>> {
        let buf = self.log_buf.lock().unwrap();
        let mut result = Vec::with_capacity(buf.len());
        for bytes in buf.iter() {
            result.push(
                ResourceLogs::decode(bytes.as_slice())
                    .context("failed to decode persisted ResourceLogs")?,
            );
        }
        Ok(result)
    }

    async fn load_metrics(&self) -> Result<Vec<ResourceMetrics>> {
        let buf = self.metric_buf.lock().unwrap();
        let mut result = Vec::with_capacity(buf.len());
        for bytes in buf.iter() {
            result.push(
                ResourceMetrics::decode(bytes.as_slice())
                    .context("failed to decode persisted ResourceMetrics")?,
            );
        }
        Ok(result)
    }

    async fn clear_traces(&self) -> Result<()> {
        let mut buf = self.trace_buf.lock().unwrap();
        buf.clear();
        let path = self.dir.join("traces.parquet");
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    async fn clear_logs(&self) -> Result<()> {
        let mut buf = self.log_buf.lock().unwrap();
        buf.clear();
        let path = self.dir.join("logs.parquet");
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    async fn clear_metrics(&self) -> Result<()> {
        let mut buf = self.metric_buf.lock().unwrap();
        buf.clear();
        let path = self.dir.join("metrics.parquet");
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::otel::common::v1::{AnyValue, KeyValue, any_value::Value};
    use crate::otel::resource::v1::Resource;
    use crate::otel::trace::v1::{ScopeSpans, Span};

    fn test_resource() -> Option<Resource> {
        Some(Resource {
            attributes: vec![KeyValue {
                key: "service.name".into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("test-service".into())),
                }),
                ..Default::default()
            }],
            ..Default::default()
        })
    }

    fn make_resource_spans(trace_id: &[u8], span_name: &str) -> ResourceSpans {
        ResourceSpans {
            resource: test_resource(),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: trace_id.to_vec(),
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    parent_span_id: vec![],
                    name: span_name.to_string(),
                    kind: 1,
                    start_time_unix_nano: 1_000_000_000,
                    end_time_unix_nano: 2_000_000_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_parquet_round_trip_traces() {
        let tmp = tempfile::TempDir::new().unwrap();
        let persist = ParquetPersist::open(tmp.path().to_str().unwrap()).unwrap();

        let rs = make_resource_spans(&[1; 16], "test-span");
        persist.write_traces(&[rs.clone()]).await.unwrap();

        let loaded = persist.load_traces().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].encode_to_vec(), rs.encode_to_vec());
    }

    #[tokio::test]
    async fn test_parquet_survives_reopen() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        // Write data
        {
            let persist = ParquetPersist::open(dir).unwrap();
            persist
                .write_traces(&[make_resource_spans(&[1; 16], "span1")])
                .await
                .unwrap();
        }

        // Reopen and verify
        {
            let persist = ParquetPersist::open(dir).unwrap();
            let traces = persist.load_traces().await.unwrap();
            assert_eq!(traces.len(), 1);
        }
    }

    #[tokio::test]
    async fn test_parquet_clear() {
        let tmp = tempfile::TempDir::new().unwrap();
        let persist = ParquetPersist::open(tmp.path().to_str().unwrap()).unwrap();

        persist
            .write_traces(&[make_resource_spans(&[1; 16], "span1")])
            .await
            .unwrap();
        persist.clear_traces().await.unwrap();

        assert!(persist.load_traces().await.unwrap().is_empty());
    }
}
