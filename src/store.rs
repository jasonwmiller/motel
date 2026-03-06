use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};

use crate::otel::{logs::v1::ResourceLogs, metrics::v1::ResourceMetrics, trace::v1::ResourceSpans};
use crate::persist::SharedPersistBackend;

pub type SharedStore = Arc<RwLock<Store>>;

#[derive(Debug, Clone)]
pub enum StoreEvent {
    TracesInserted(Vec<ResourceSpans>),
    LogsInserted(Vec<ResourceLogs>),
    MetricsInserted(Vec<ResourceMetrics>),
    TracesCleared,
    LogsCleared,
    MetricsCleared,
}

pub struct Store {
    // Traces stored per trace_id for FIFO eviction by trace
    pub traces: VecDeque<ResourceSpans>,
    trace_id_order: VecDeque<Vec<u8>>,
    trace_id_set: HashSet<Vec<u8>>,

    pub logs: VecDeque<ResourceLogs>,
    pub metrics: VecDeque<ResourceMetrics>,

    pub max_traces: usize,
    pub max_logs: usize,
    pub max_metrics: usize,

    pub event_tx: broadcast::Sender<StoreEvent>,

    /// Optional persistence backend (write-through on insert, clear on clear).
    pub persist: Option<SharedPersistBackend>,
}

impl Store {
    pub fn new(
        max_traces: usize,
        max_logs: usize,
        max_metrics: usize,
    ) -> (Self, broadcast::Receiver<StoreEvent>) {
        Self::new_with_persist(max_traces, max_logs, max_metrics, None)
    }

    pub fn new_with_persist(
        max_traces: usize,
        max_logs: usize,
        max_metrics: usize,
        persist: Option<SharedPersistBackend>,
    ) -> (Self, broadcast::Receiver<StoreEvent>) {
        let (event_tx, event_rx) = broadcast::channel(1024);
        let store = Self {
            traces: VecDeque::new(),
            trace_id_order: VecDeque::new(),
            trace_id_set: HashSet::new(),
            logs: VecDeque::new(),
            metrics: VecDeque::new(),
            max_traces,
            max_logs,
            max_metrics,
            event_tx,
            persist,
        };
        (store, event_rx)
    }

    pub fn new_shared(
        max_traces: usize,
        max_logs: usize,
        max_metrics: usize,
    ) -> (SharedStore, broadcast::Receiver<StoreEvent>) {
        Self::new_shared_with_persist(max_traces, max_logs, max_metrics, None)
    }

    pub fn new_shared_with_persist(
        max_traces: usize,
        max_logs: usize,
        max_metrics: usize,
        persist: Option<SharedPersistBackend>,
    ) -> (SharedStore, broadcast::Receiver<StoreEvent>) {
        let (store, rx) = Self::new_with_persist(max_traces, max_logs, max_metrics, persist);
        (Arc::new(RwLock::new(store)), rx)
    }

    #[tracing::instrument(skip_all, fields(count = resource_spans.len()))]
    pub fn insert_traces(&mut self, resource_spans: Vec<ResourceSpans>) {
        // Write-through to persistence (spawned as background task)
        if let Some(ref persist) = self.persist {
            let persist = persist.clone();
            let data = resource_spans.clone();
            tokio::spawn(async move {
                if let Err(e) = persist.write_traces(&data).await {
                    tracing::warn!("persistence write_traces failed: {e}");
                }
            });
        }

        self.insert_traces_no_persist(resource_spans);
    }

    /// Insert traces without writing to persistence (used during startup load).
    pub fn insert_traces_no_persist(&mut self, resource_spans: Vec<ResourceSpans>) {
        for rs in &resource_spans {
            // Extract unique trace IDs from this ResourceSpans
            for scope_spans in &rs.scope_spans {
                for span in &scope_spans.spans {
                    let tid = &span.trace_id;
                    if self.trace_id_set.insert(tid.clone()) {
                        self.trace_id_order.push_back(tid.clone());
                    }
                }
            }
        }

        let event = StoreEvent::TracesInserted(resource_spans.clone());
        self.traces.extend(resource_spans);

        // Evict oldest traces by trace_id
        while self.trace_id_set.len() > self.max_traces {
            if let Some(oldest_tid) = self.trace_id_order.pop_front() {
                self.trace_id_set.remove(&oldest_tid);
                for rs in self.traces.iter_mut() {
                    for ss in rs.scope_spans.iter_mut() {
                        ss.spans.retain(|s| s.trace_id != oldest_tid);
                    }
                    rs.scope_spans.retain(|ss| !ss.spans.is_empty());
                }
                self.traces.retain(|rs| !rs.scope_spans.is_empty());
            }
        }

        let _ = self.event_tx.send(event);
    }

    #[tracing::instrument(skip_all, fields(count = resource_logs.len()))]
    pub fn insert_logs(&mut self, resource_logs: Vec<ResourceLogs>) {
        // Write-through to persistence
        if let Some(ref persist) = self.persist {
            let persist = persist.clone();
            let data = resource_logs.clone();
            tokio::spawn(async move {
                if let Err(e) = persist.write_logs(&data).await {
                    tracing::warn!("persistence write_logs failed: {e}");
                }
            });
        }

        self.insert_logs_no_persist(resource_logs);
    }

    /// Insert logs without writing to persistence (used during startup load).
    pub fn insert_logs_no_persist(&mut self, resource_logs: Vec<ResourceLogs>) {
        let event = StoreEvent::LogsInserted(resource_logs.clone());
        self.logs.extend(resource_logs);
        while self.logs.len() > self.max_logs {
            self.logs.pop_front();
        }
        let _ = self.event_tx.send(event);
    }

    #[tracing::instrument(skip_all, fields(count = resource_metrics.len()))]
    pub fn insert_metrics(&mut self, resource_metrics: Vec<ResourceMetrics>) {
        // Write-through to persistence
        if let Some(ref persist) = self.persist {
            let persist = persist.clone();
            let data = resource_metrics.clone();
            tokio::spawn(async move {
                if let Err(e) = persist.write_metrics(&data).await {
                    tracing::warn!("persistence write_metrics failed: {e}");
                }
            });
        }

        self.insert_metrics_no_persist(resource_metrics);
    }

    /// Insert metrics without writing to persistence (used during startup load).
    pub fn insert_metrics_no_persist(&mut self, resource_metrics: Vec<ResourceMetrics>) {
        let event = StoreEvent::MetricsInserted(resource_metrics.clone());
        self.metrics.extend(resource_metrics);
        while self.metrics.len() > self.max_metrics {
            self.metrics.pop_front();
        }
        let _ = self.event_tx.send(event);
    }

    #[tracing::instrument(skip_all)]
    pub fn clear_traces(&mut self) -> usize {
        if let Some(ref persist) = self.persist {
            let persist = persist.clone();
            tokio::spawn(async move {
                if let Err(e) = persist.clear_traces().await {
                    tracing::warn!("persistence clear_traces failed: {e}");
                }
            });
        }
        let count = self.traces.len();
        self.traces.clear();
        self.trace_id_order.clear();
        self.trace_id_set.clear();
        let _ = self.event_tx.send(StoreEvent::TracesCleared);
        count
    }

    #[tracing::instrument(skip_all)]
    pub fn clear_logs(&mut self) -> usize {
        if let Some(ref persist) = self.persist {
            let persist = persist.clone();
            tokio::spawn(async move {
                if let Err(e) = persist.clear_logs().await {
                    tracing::warn!("persistence clear_logs failed: {e}");
                }
            });
        }
        let count = self.logs.len();
        self.logs.clear();
        let _ = self.event_tx.send(StoreEvent::LogsCleared);
        count
    }

    #[tracing::instrument(skip_all)]
    pub fn clear_metrics(&mut self) -> usize {
        if let Some(ref persist) = self.persist {
            let persist = persist.clone();
            tokio::spawn(async move {
                if let Err(e) = persist.clear_metrics().await {
                    tracing::warn!("persistence clear_metrics failed: {e}");
                }
            });
        }
        let count = self.metrics.len();
        self.metrics.clear();
        let _ = self.event_tx.send(StoreEvent::MetricsCleared);
        count
    }

    pub fn span_count(&self) -> usize {
        self.traces
            .iter()
            .map(|rs| {
                rs.scope_spans
                    .iter()
                    .map(|ss| ss.spans.len())
                    .sum::<usize>()
            })
            .sum()
    }

    pub fn log_count(&self) -> usize {
        self.logs
            .iter()
            .map(|rl| {
                rl.scope_logs
                    .iter()
                    .map(|sl| sl.log_records.len())
                    .sum::<usize>()
            })
            .sum()
    }

    pub fn metric_count(&self) -> usize {
        self.metrics
            .iter()
            .map(|rm| {
                rm.scope_metrics
                    .iter()
                    .map(|sm| sm.metrics.len())
                    .sum::<usize>()
            })
            .sum()
    }

    pub fn trace_count(&self) -> usize {
        self.trace_id_set.len()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::otel::common::v1::KeyValue;
    use crate::otel::resource::v1::Resource;
    use crate::otel::trace::v1::{ResourceSpans, ScopeSpans, Span};

    pub fn make_resource_spans(trace_id: &[u8], span_name: &str) -> ResourceSpans {
        ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(crate::otel::common::v1::AnyValue {
                        value: Some(crate::otel::common::v1::any_value::Value::StringValue(
                            "test-service".into(),
                        )),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: trace_id.to_vec(),
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    parent_span_id: vec![],
                    name: span_name.to_string(),
                    kind: 1,
                    start_time_unix_nano: 1000000000,
                    end_time_unix_nano: 2000000000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn test_insert_and_count() {
        let (mut store, _rx) = Store::new(100, 100, 100);
        let rs = make_resource_spans(&[1; 16], "test-span");
        store.insert_traces(vec![rs]);
        assert_eq!(store.trace_count(), 1);
        assert_eq!(store.span_count(), 1);
    }

    #[test]
    fn test_fifo_eviction() {
        let (mut store, _rx) = Store::new(2, 100, 100);
        store.insert_traces(vec![make_resource_spans(&[1; 16], "span1")]);
        store.insert_traces(vec![make_resource_spans(&[2; 16], "span2")]);
        store.insert_traces(vec![make_resource_spans(&[3; 16], "span3")]);
        assert_eq!(store.trace_count(), 2);
        // First trace should be evicted
        assert!(!store.trace_id_set.contains(&vec![1u8; 16]));
    }

    #[test]
    fn test_clear() {
        let (mut store, _rx) = Store::new(100, 100, 100);
        store.insert_traces(vec![make_resource_spans(&[1; 16], "span1")]);
        assert_eq!(store.clear_traces(), 1);
        assert_eq!(store.trace_count(), 0);
        assert_eq!(store.span_count(), 0);
    }
}
