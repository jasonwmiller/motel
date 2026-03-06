use std::sync::Mutex;

use anyhow::{Context, Result};
use prost::Message;
use rusqlite::Connection;

use crate::otel::{
    logs::v1::ResourceLogs, metrics::v1::ResourceMetrics, trace::v1::ResourceSpans,
};

use super::PersistBackend;

/// SQLite persistence backend.
///
/// Each `ResourceSpans`/`ResourceLogs`/`ResourceMetrics` is stored as a
/// protobuf-encoded BLOB. This is simple, lossless, and avoids schema mapping
/// complexity.
pub struct SqlitePersist {
    conn: Mutex<Connection>,
}

impl SqlitePersist {
    /// Open (or create) a SQLite database at `path` and ensure the schema exists.
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path).context("failed to open SQLite database")?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS traces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data BLOB NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data BLOB NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data BLOB NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            );
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
        ",
        )
        .context("failed to initialize SQLite schema")?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

#[async_trait::async_trait]
impl PersistBackend for SqlitePersist {
    async fn write_traces(&self, data: &[ResourceSpans]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached("INSERT INTO traces (data) VALUES (?1)")?;
        for rs in data {
            let bytes = rs.encode_to_vec();
            stmt.execute(rusqlite::params![bytes])?;
        }
        Ok(())
    }

    async fn write_logs(&self, data: &[ResourceLogs]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached("INSERT INTO logs (data) VALUES (?1)")?;
        for rl in data {
            let bytes = rl.encode_to_vec();
            stmt.execute(rusqlite::params![bytes])?;
        }
        Ok(())
    }

    async fn write_metrics(&self, data: &[ResourceMetrics]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached("INSERT INTO metrics (data) VALUES (?1)")?;
        for rm in data {
            let bytes = rm.encode_to_vec();
            stmt.execute(rusqlite::params![bytes])?;
        }
        Ok(())
    }

    async fn load_traces(&self) -> Result<Vec<ResourceSpans>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT data FROM traces ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            let bytes: Vec<u8> = row.get(0)?;
            Ok(bytes)
        })?;
        let mut result = Vec::new();
        for row in rows {
            let bytes = row?;
            let rs = ResourceSpans::decode(bytes.as_slice())
                .context("failed to decode persisted ResourceSpans")?;
            result.push(rs);
        }
        Ok(result)
    }

    async fn load_logs(&self) -> Result<Vec<ResourceLogs>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT data FROM logs ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            let bytes: Vec<u8> = row.get(0)?;
            Ok(bytes)
        })?;
        let mut result = Vec::new();
        for row in rows {
            let bytes = row?;
            let rl = ResourceLogs::decode(bytes.as_slice())
                .context("failed to decode persisted ResourceLogs")?;
            result.push(rl);
        }
        Ok(result)
    }

    async fn load_metrics(&self) -> Result<Vec<ResourceMetrics>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT data FROM metrics ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            let bytes: Vec<u8> = row.get(0)?;
            Ok(bytes)
        })?;
        let mut result = Vec::new();
        for row in rows {
            let bytes = row?;
            let rm = ResourceMetrics::decode(bytes.as_slice())
                .context("failed to decode persisted ResourceMetrics")?;
            result.push(rm);
        }
        Ok(result)
    }

    async fn clear_traces(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM traces", [])?;
        Ok(())
    }

    async fn clear_logs(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM logs", [])?;
        Ok(())
    }

    async fn clear_metrics(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM metrics", [])?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::otel::common::v1::{AnyValue, KeyValue, any_value::Value};
    use crate::otel::logs::v1::{LogRecord, ScopeLogs};
    use crate::otel::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ScopeMetrics, number_data_point,
    };
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

    fn make_resource_logs(body: &str) -> ResourceLogs {
        ResourceLogs {
            resource: test_resource(),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1_000_000_000,
                    observed_time_unix_nano: 1_000_000_000,
                    severity_number: 9,
                    severity_text: "INFO".into(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue(body.to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn make_resource_metrics(metric_name: &str, value: f64) -> ResourceMetrics {
        ResourceMetrics {
            resource: test_resource(),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: "test metric".to_string(),
                    unit: "1".to_string(),
                    data: Some(crate::otel::metrics::v1::metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_000_000_000,
                            start_time_unix_nano: 0,
                            value: Some(number_data_point::Value::AsDouble(value)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn temp_db_path() -> (tempfile::NamedTempFile, String) {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        (tmp, path)
    }

    #[tokio::test]
    async fn test_sqlite_round_trip_traces() {
        let (_tmp, path) = temp_db_path();
        let persist = SqlitePersist::open(&path).unwrap();

        let rs = make_resource_spans(&[1; 16], "test-span");
        persist.write_traces(&[rs.clone()]).await.unwrap();

        let loaded = persist.load_traces().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].encode_to_vec(), rs.encode_to_vec());
    }

    #[tokio::test]
    async fn test_sqlite_round_trip_logs() {
        let (_tmp, path) = temp_db_path();
        let persist = SqlitePersist::open(&path).unwrap();

        let rl = make_resource_logs("hello world");
        persist.write_logs(&[rl.clone()]).await.unwrap();

        let loaded = persist.load_logs().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].encode_to_vec(), rl.encode_to_vec());
    }

    #[tokio::test]
    async fn test_sqlite_round_trip_metrics() {
        let (_tmp, path) = temp_db_path();
        let persist = SqlitePersist::open(&path).unwrap();

        let rm = make_resource_metrics("cpu.usage", 42.0);
        persist.write_metrics(&[rm.clone()]).await.unwrap();

        let loaded = persist.load_metrics().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].encode_to_vec(), rm.encode_to_vec());
    }

    #[tokio::test]
    async fn test_sqlite_clear() {
        let (_tmp, path) = temp_db_path();
        let persist = SqlitePersist::open(&path).unwrap();

        persist
            .write_traces(&[make_resource_spans(&[1; 16], "span1")])
            .await
            .unwrap();
        persist
            .write_logs(&[make_resource_logs("log1")])
            .await
            .unwrap();
        persist
            .write_metrics(&[make_resource_metrics("m1", 1.0)])
            .await
            .unwrap();

        persist.clear_traces().await.unwrap();
        persist.clear_logs().await.unwrap();
        persist.clear_metrics().await.unwrap();

        assert!(persist.load_traces().await.unwrap().is_empty());
        assert!(persist.load_logs().await.unwrap().is_empty());
        assert!(persist.load_metrics().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_sqlite_multiple_writes() {
        let (_tmp, path) = temp_db_path();
        let persist = SqlitePersist::open(&path).unwrap();

        persist
            .write_traces(&[make_resource_spans(&[1; 16], "span1")])
            .await
            .unwrap();
        persist
            .write_traces(&[make_resource_spans(&[2; 16], "span2")])
            .await
            .unwrap();
        persist
            .write_traces(&[make_resource_spans(&[3; 16], "span3")])
            .await
            .unwrap();

        let loaded = persist.load_traces().await.unwrap();
        assert_eq!(loaded.len(), 3);
    }

    #[tokio::test]
    async fn test_sqlite_survives_reopen() {
        let (_tmp, path) = temp_db_path();

        // Write data with one instance
        {
            let persist = SqlitePersist::open(&path).unwrap();
            persist
                .write_traces(&[make_resource_spans(&[1; 16], "span1")])
                .await
                .unwrap();
            persist
                .write_logs(&[make_resource_logs("log1")])
                .await
                .unwrap();
        }

        // Reopen and verify data is still there
        {
            let persist = SqlitePersist::open(&path).unwrap();
            let traces = persist.load_traces().await.unwrap();
            assert_eq!(traces.len(), 1);
            let logs = persist.load_logs().await.unwrap();
            assert_eq!(logs.len(), 1);
        }
    }
}
