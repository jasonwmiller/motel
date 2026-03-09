pub mod parquet;
pub mod sqlite;

use anyhow::Result;

use crate::otel::{logs::v1::ResourceLogs, metrics::v1::ResourceMetrics, trace::v1::ResourceSpans};

/// Trait for persistence backends.
#[async_trait::async_trait]
pub trait PersistBackend: Send + Sync + 'static {
    /// Write a batch of traces to persistent storage.
    async fn write_traces(&self, data: &[ResourceSpans]) -> Result<()>;

    /// Write a batch of logs to persistent storage.
    async fn write_logs(&self, data: &[ResourceLogs]) -> Result<()>;

    /// Write a batch of metrics to persistent storage.
    async fn write_metrics(&self, data: &[ResourceMetrics]) -> Result<()>;

    /// Load all persisted traces (called once at startup).
    async fn load_traces(&self) -> Result<Vec<ResourceSpans>>;

    /// Load all persisted logs (called once at startup).
    async fn load_logs(&self) -> Result<Vec<ResourceLogs>>;

    /// Load all persisted metrics (called once at startup).
    async fn load_metrics(&self) -> Result<Vec<ResourceMetrics>>;

    /// Clear all persisted traces.
    async fn clear_traces(&self) -> Result<()>;

    /// Clear all persisted logs.
    async fn clear_logs(&self) -> Result<()>;

    /// Clear all persisted metrics.
    async fn clear_metrics(&self) -> Result<()>;
}

pub type SharedPersistBackend = std::sync::Arc<dyn PersistBackend>;
