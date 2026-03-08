use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
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

    /// Fraction of traces to keep (0.0-1.0). 1.0 = keep all.
    pub sample_rate: f64,
    /// Service names that bypass sampling (always kept).
    pub sample_always: HashSet<String>,
    /// Counter of spans dropped by sampling, for status reporting.
    pub traces_dropped: u64,
    /// Trace IDs that are pinned (excluded from FIFO eviction).
    pub pinned_trace_ids: HashSet<Vec<u8>>,
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
        Self::new_with_sampling(max_traces, max_logs, max_metrics, persist, 1.0, vec![])
    }

    pub fn new_with_sampling(
        max_traces: usize,
        max_logs: usize,
        max_metrics: usize,
        persist: Option<SharedPersistBackend>,
        sample_rate: f64,
        sample_always: Vec<String>,
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
            sample_rate,
            sample_always: sample_always.into_iter().collect(),
            traces_dropped: 0,
            pinned_trace_ids: HashSet::new(),
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

    pub fn new_shared_with_sampling(
        max_traces: usize,
        max_logs: usize,
        max_metrics: usize,
        persist: Option<SharedPersistBackend>,
        sample_rate: f64,
        sample_always: Vec<String>,
    ) -> (SharedStore, broadcast::Receiver<StoreEvent>) {
        let (store, rx) = Self::new_with_sampling(
            max_traces,
            max_logs,
            max_metrics,
            persist,
            sample_rate,
            sample_always,
        );
        (Arc::new(RwLock::new(store)), rx)
    }

    #[tracing::instrument(skip_all, fields(count = resource_spans.len()))]
    pub fn insert_traces(&mut self, resource_spans: Vec<ResourceSpans>) {
        // Apply sampling filter
        let resource_spans = self.apply_sampling(resource_spans);
        if resource_spans.is_empty() {
            return;
        }

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

        self.insert_traces_inner(resource_spans);
    }

    /// Insert traces without writing to persistence (used during startup load).
    /// Sampling is NOT applied here since persisted data was already sampled.
    pub fn insert_traces_no_persist(&mut self, resource_spans: Vec<ResourceSpans>) {
        self.insert_traces_inner(resource_spans);
    }

    /// Core insertion logic shared by insert_traces and insert_traces_no_persist.
    fn insert_traces_inner(&mut self, resource_spans: Vec<ResourceSpans>) {
        let mut new_tids: HashSet<Vec<u8>> = HashSet::new();
        for rs in &resource_spans {
            // Extract unique trace IDs from this ResourceSpans
            for scope_spans in &rs.scope_spans {
                for span in &scope_spans.spans {
                    let tid = &span.trace_id;
                    if self.trace_id_set.insert(tid.clone()) {
                        self.trace_id_order.push_back(tid.clone());
                        new_tids.insert(tid.clone());
                    }
                }
            }
        }

        let event = StoreEvent::TracesInserted(resource_spans.clone());
        self.traces.extend(resource_spans);

        // Evict oldest traces by trace_id, skipping pinned and newly-inserted ones
        while self.trace_id_set.len() > self.max_traces {
            let mut found = false;
            let order_len = self.trace_id_order.len();
            for _ in 0..order_len {
                if let Some(oldest_tid) = self.trace_id_order.pop_front() {
                    if self.pinned_trace_ids.contains(&oldest_tid)
                        || new_tids.contains(&oldest_tid)
                    {
                        // Pinned or just inserted — put it back at the end and keep looking
                        self.trace_id_order.push_back(oldest_tid);
                        continue;
                    }
                    // Not pinned — evict it
                    self.trace_id_set.remove(&oldest_tid);
                    for rs in self.traces.iter_mut() {
                        for ss in rs.scope_spans.iter_mut() {
                            ss.spans.retain(|s| s.trace_id != oldest_tid);
                        }
                        rs.scope_spans.retain(|ss| !ss.spans.is_empty());
                    }
                    self.traces.retain(|rs| !rs.scope_spans.is_empty());
                    found = true;
                    break;
                }
            }
            if !found {
                // All remaining traces are pinned or just inserted — can't evict further
                break;
            }
        }

        let _ = self.event_tx.send(event);
    }

    /// Apply trace-level sampling to incoming ResourceSpans.
    /// Returns the filtered list (may be empty if all were dropped).
    /// Fast path: if sample_rate >= 1.0, returns input unchanged.
    fn apply_sampling(&mut self, resource_spans: Vec<ResourceSpans>) -> Vec<ResourceSpans> {
        if self.sample_rate >= 1.0 {
            return resource_spans;
        }

        let mut sampled: Vec<ResourceSpans> = Vec::new();
        let mut dropped_count: u64 = 0;

        for mut rs in resource_spans {
            // Check if this ResourceSpans belongs to an always-sampled service
            let is_always = !self.sample_always.is_empty()
                && rs.resource.as_ref().is_some_and(|r| {
                    r.attributes.iter().any(|kv| {
                        kv.key == "service.name"
                            && kv.value.as_ref().is_some_and(|v| {
                                if let Some(
                                    crate::otel::common::v1::any_value::Value::StringValue(s),
                                ) = &v.value
                                {
                                    self.sample_always.contains(s)
                                } else {
                                    false
                                }
                            })
                    })
                });

            if is_always {
                sampled.push(rs);
                continue;
            }

            // Filter spans within each ResourceSpans by trace_id sampling
            for ss in &mut rs.scope_spans {
                let before = ss.spans.len();
                ss.spans.retain(|span| self.should_sample(&span.trace_id));
                dropped_count += (before - ss.spans.len()) as u64;
            }
            rs.scope_spans.retain(|ss| !ss.spans.is_empty());
            if !rs.scope_spans.is_empty() {
                sampled.push(rs);
            }
        }

        self.traces_dropped += dropped_count;
        sampled
    }

    /// Deterministic sampling decision based on trace_id hash.
    /// The same trace_id always produces the same decision, so all spans
    /// belonging to a trace are either all kept or all dropped.
    pub fn should_sample(&self, trace_id: &[u8]) -> bool {
        if self.sample_rate >= 1.0 {
            return true;
        }
        if self.sample_rate <= 0.0 {
            return false;
        }
        let mut hasher = std::hash::DefaultHasher::new();
        trace_id.hash(&mut hasher);
        let hash = hasher.finish();
        // Map hash to [0.0, 1.0) range
        let normalized = (hash as f64) / (u64::MAX as f64);
        normalized < self.sample_rate
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
        self.pinned_trace_ids.clear();
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

    /// Pin a trace ID, excluding it from FIFO eviction.
    pub fn pin_trace(&mut self, trace_id: Vec<u8>) -> bool {
        self.pinned_trace_ids.insert(trace_id)
    }

    /// Unpin a trace ID, allowing FIFO eviction again.
    pub fn unpin_trace(&mut self, trace_id: &[u8]) -> bool {
        self.pinned_trace_ids.remove(trace_id)
    }

    /// Check if a trace ID is pinned.
    pub fn is_pinned(&self, trace_id: &[u8]) -> bool {
        self.pinned_trace_ids.contains(trace_id)
    }

    /// Toggle pin state for a trace ID. Returns the new pin state.
    pub fn toggle_pin(&mut self, trace_id: Vec<u8>) -> bool {
        if self.pinned_trace_ids.contains(&trace_id) {
            self.pinned_trace_ids.remove(&trace_id);
            false
        } else {
            self.pinned_trace_ids.insert(trace_id);
            true
        }
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

    /// Remove traces with all spans older than `cutoff_ns` (nanoseconds since epoch).
    /// Returns the number of ResourceSpans entries removed.
    #[tracing::instrument(skip_all)]
    pub fn evict_traces_by_age(&mut self, cutoff_ns: u64) -> usize {
        let before = self.traces.len();
        // Remove spans older than cutoff from each ResourceSpans
        for rs in self.traces.iter_mut() {
            for ss in rs.scope_spans.iter_mut() {
                ss.spans.retain(|s| s.end_time_unix_nano >= cutoff_ns);
            }
            rs.scope_spans.retain(|ss| !ss.spans.is_empty());
        }
        self.traces.retain(|rs| !rs.scope_spans.is_empty());

        // Rebuild trace_id tracking
        self.trace_id_set.clear();
        self.trace_id_order.clear();
        for rs in &self.traces {
            for ss in &rs.scope_spans {
                for span in &ss.spans {
                    if self.trace_id_set.insert(span.trace_id.clone()) {
                        self.trace_id_order.push_back(span.trace_id.clone());
                    }
                }
            }
        }

        let removed = before - self.traces.len();
        if removed > 0 {
            let _ = self.event_tx.send(StoreEvent::TracesCleared);
        }
        removed
    }

    /// Remove logs older than `cutoff_ns`. Returns number of ResourceLogs entries removed.
    #[tracing::instrument(skip_all)]
    pub fn evict_logs_by_age(&mut self, cutoff_ns: u64) -> usize {
        let before = self.logs.len();
        for rl in self.logs.iter_mut() {
            for sl in rl.scope_logs.iter_mut() {
                sl.log_records.retain(|lr| {
                    let ts = if lr.time_unix_nano > 0 {
                        lr.time_unix_nano
                    } else {
                        lr.observed_time_unix_nano
                    };
                    ts >= cutoff_ns
                });
            }
            rl.scope_logs.retain(|sl| !sl.log_records.is_empty());
        }
        self.logs.retain(|rl| !rl.scope_logs.is_empty());
        let removed = before - self.logs.len();
        if removed > 0 {
            let _ = self.event_tx.send(StoreEvent::LogsCleared);
        }
        removed
    }

    /// Remove metrics older than `cutoff_ns`. Returns number of ResourceMetrics entries removed.
    #[tracing::instrument(skip_all)]
    pub fn evict_metrics_by_age(&mut self, cutoff_ns: u64) -> usize {
        use crate::otel::metrics::v1::metric;

        let before = self.metrics.len();
        for rm in self.metrics.iter_mut() {
            for sm in rm.scope_metrics.iter_mut() {
                for m in sm.metrics.iter_mut() {
                    match &mut m.data {
                        Some(metric::Data::Gauge(g)) => {
                            g.data_points.retain(|dp| dp.time_unix_nano >= cutoff_ns);
                        }
                        Some(metric::Data::Sum(s)) => {
                            s.data_points.retain(|dp| dp.time_unix_nano >= cutoff_ns);
                        }
                        Some(metric::Data::Histogram(h)) => {
                            h.data_points.retain(|dp| dp.time_unix_nano >= cutoff_ns);
                        }
                        Some(metric::Data::ExponentialHistogram(h)) => {
                            h.data_points.retain(|dp| dp.time_unix_nano >= cutoff_ns);
                        }
                        Some(metric::Data::Summary(s)) => {
                            s.data_points.retain(|dp| dp.time_unix_nano >= cutoff_ns);
                        }
                        None => {}
                    }
                }
                sm.metrics.retain(|m| has_metric_data_points(&m.data));
            }
            rm.scope_metrics.retain(|sm| !sm.metrics.is_empty());
        }
        self.metrics.retain(|rm| !rm.scope_metrics.is_empty());
        let removed = before - self.metrics.len();
        if removed > 0 {
            let _ = self.event_tx.send(StoreEvent::MetricsCleared);
        }
        removed
    }
}

/// Check whether a metric has any data points remaining.
fn has_metric_data_points(data: &Option<crate::otel::metrics::v1::metric::Data>) -> bool {
    use crate::otel::metrics::v1::metric;
    match data {
        Some(metric::Data::Gauge(g)) => !g.data_points.is_empty(),
        Some(metric::Data::Sum(s)) => !s.data_points.is_empty(),
        Some(metric::Data::Histogram(h)) => !h.data_points.is_empty(),
        Some(metric::Data::ExponentialHistogram(h)) => !h.data_points.is_empty(),
        Some(metric::Data::Summary(s)) => !s.data_points.is_empty(),
        None => false,
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

    fn make_resource_spans_with_service(
        trace_id: &[u8],
        span_name: &str,
        service_name: &str,
    ) -> ResourceSpans {
        ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(crate::otel::common::v1::AnyValue {
                        value: Some(crate::otel::common::v1::any_value::Value::StringValue(
                            service_name.into(),
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
    fn test_sample_rate_1_0_keeps_all() {
        let (mut store, _rx) = Store::new_with_sampling(100, 100, 100, None, 1.0, vec![]);
        for i in 0..10u8 {
            store.insert_traces(vec![make_resource_spans(&[i; 16], "span")]);
        }
        assert_eq!(store.trace_count(), 10);
        assert_eq!(store.traces_dropped, 0);
    }

    #[test]
    fn test_sample_rate_0_0_drops_all() {
        let (mut store, _rx) = Store::new_with_sampling(100, 100, 100, None, 0.0, vec![]);
        for i in 0..10u8 {
            store.insert_traces(vec![make_resource_spans(&[i; 16], "span")]);
        }
        assert_eq!(store.trace_count(), 0);
        assert_eq!(store.span_count(), 0);
        assert_eq!(store.traces_dropped, 10);
    }

    #[test]
    fn test_sample_rate_deterministic() {
        // Same trace_id should always get the same sampling decision
        let (mut store1, _rx1) = Store::new_with_sampling(100, 100, 100, None, 0.5, vec![]);
        let (mut store2, _rx2) = Store::new_with_sampling(100, 100, 100, None, 0.5, vec![]);
        let tid = [42u8; 16];
        store1.insert_traces(vec![make_resource_spans(&tid, "span")]);
        store2.insert_traces(vec![make_resource_spans(&tid, "span")]);
        assert_eq!(store1.trace_count(), store2.trace_count());
    }

    #[test]
    fn test_sample_rate_approximate() {
        // With enough traces, ~50% should be kept at rate 0.5
        let (mut store, _rx) = Store::new_with_sampling(100000, 100, 100, None, 0.5, vec![]);
        for i in 0..1000u32 {
            let tid: Vec<u8> = i.to_be_bytes().repeat(4); // 16 bytes
            store.insert_traces(vec![make_resource_spans(&tid, "span")]);
        }
        let kept = store.trace_count();
        // Allow 40-60% range for statistical tolerance
        assert!(
            kept > 400 && kept < 600,
            "kept {kept} out of 1000, expected ~500"
        );
    }

    #[test]
    fn test_sample_rate_logs_not_sampled() {
        use crate::otel::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

        let (mut store, _rx) = Store::new_with_sampling(100, 100, 100, None, 0.0, vec![]);
        // Even with 0% trace sampling, logs should be stored
        store.insert_logs(vec![ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    severity_text: "INFO".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }]);
        assert_eq!(store.log_count(), 1);
    }

    #[test]
    fn test_should_sample_consistency() {
        // A trace_id should always get the same result
        let (store, _rx) = Store::new_with_sampling(100, 100, 100, None, 0.5, vec![]);
        let tid = [7u8; 16];
        let result1 = store.should_sample(&tid);
        let result2 = store.should_sample(&tid);
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_pin_prevents_eviction() {
        let (mut store, _rx) = Store::new(2, 100, 100);
        store.insert_traces(vec![make_resource_spans(&[1; 16], "span1")]);
        store.insert_traces(vec![make_resource_spans(&[2; 16], "span2")]);

        // Pin the first trace
        store.pin_trace(vec![1; 16]);

        // Insert a third trace — should evict trace 2 (not pinned trace 1)
        store.insert_traces(vec![make_resource_spans(&[3; 16], "span3")]);

        assert_eq!(store.trace_count(), 2);
        assert!(store.trace_id_set.contains(&vec![1u8; 16])); // pinned, still here
        assert!(!store.trace_id_set.contains(&vec![2u8; 16])); // evicted
        assert!(store.trace_id_set.contains(&vec![3u8; 16])); // newly added
    }

    #[test]
    fn test_unpin_allows_eviction() {
        let (mut store, _rx) = Store::new(2, 100, 100);
        store.insert_traces(vec![make_resource_spans(&[1; 16], "span1")]);
        store.pin_trace(vec![1; 16]);
        store.insert_traces(vec![make_resource_spans(&[2; 16], "span2")]);
        store.insert_traces(vec![make_resource_spans(&[3; 16], "span3")]);

        // Trace 1 is still pinned (trace 2 was evicted to make room for trace 3)
        assert!(store.trace_id_set.contains(&vec![1u8; 16]));
        assert!(store.trace_id_set.contains(&vec![3u8; 16]));

        // Unpin it
        store.unpin_trace(&vec![1; 16]);

        // Now adding another trace should evict trace 3 (next in eviction order,
        // since trace 1 was moved to the back of the queue while pinned)
        store.insert_traces(vec![make_resource_spans(&[4; 16], "span4")]);
        assert!(store.trace_id_set.contains(&vec![1u8; 16])); // moved to back, still here
        assert!(!store.trace_id_set.contains(&vec![3u8; 16])); // evicted (front of queue)
        assert!(store.trace_id_set.contains(&vec![4u8; 16])); // newly added
    }

    #[test]
    fn test_toggle_pin() {
        let (mut store, _rx) = Store::new(100, 100, 100);
        store.insert_traces(vec![make_resource_spans(&[1; 16], "span1")]);

        assert!(!store.is_pinned(&vec![1; 16]));
        assert!(store.toggle_pin(vec![1; 16])); // now pinned
        assert!(store.is_pinned(&vec![1; 16]));
        assert!(!store.toggle_pin(vec![1; 16])); // now unpinned
        assert!(!store.is_pinned(&vec![1; 16]));
    }

    #[test]
    fn test_all_pinned_stops_eviction() {
        let (mut store, _rx) = Store::new(2, 100, 100);
        store.insert_traces(vec![make_resource_spans(&[1; 16], "span1")]);
        store.insert_traces(vec![make_resource_spans(&[2; 16], "span2")]);
        store.pin_trace(vec![1; 16]);
        store.pin_trace(vec![2; 16]);

        // Insert a third trace — both existing are pinned, so eviction should stop
        store.insert_traces(vec![make_resource_spans(&[3; 16], "span3")]);
        assert_eq!(store.trace_count(), 3); // all three still present
    }

    #[test]
    fn test_clear_traces_clears_pins() {
        let (mut store, _rx) = Store::new(100, 100, 100);
        store.insert_traces(vec![make_resource_spans(&[1; 16], "span1")]);
        store.pin_trace(vec![1; 16]);
        store.clear_traces();
        assert!(store.pinned_trace_ids.is_empty());
    }

    #[test]
    fn test_sample_always_bypasses_sampling() {
        let (mut store, _rx) = Store::new_with_sampling(
            100,
            100,
            100,
            None,
            0.0,
            vec!["critical-service".to_string()],
        );
        // Traces from critical-service should be kept even at 0% sampling
        store.insert_traces(vec![make_resource_spans_with_service(
            &[1; 16],
            "span",
            "critical-service",
        )]);
        assert_eq!(store.trace_count(), 1);
        assert_eq!(store.traces_dropped, 0);

        // Traces from other services should be dropped
        store.insert_traces(vec![make_resource_spans_with_service(
            &[2; 16],
            "span",
            "other-service",
        )]);
        assert_eq!(store.trace_count(), 1);
        assert_eq!(store.traces_dropped, 1);
    }

    pub fn make_resource_spans_with_ts(
        trace_id: &[u8],
        span_name: &str,
        start_ns: u64,
        end_ns: u64,
    ) -> ResourceSpans {
        use crate::otel::common::v1::{AnyValue, any_value::Value};
        ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("test-service".into())),
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
                    start_time_unix_nano: start_ns,
                    end_time_unix_nano: end_ns,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn make_resource_logs_with_ts(body: &str, time_ns: u64) -> crate::otel::logs::v1::ResourceLogs {
        use crate::otel::common::v1::{AnyValue, any_value::Value};
        use crate::otel::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
        ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("test-service".into())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: time_ns,
                    observed_time_unix_nano: time_ns,
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

    fn make_resource_metrics_with_ts(
        name: &str,
        value: f64,
        time_ns: u64,
    ) -> crate::otel::metrics::v1::ResourceMetrics {
        use crate::otel::common::v1::{AnyValue, any_value::Value};
        use crate::otel::metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, number_data_point,
        };
        ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("test-service".into())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: name.to_string(),
                    description: "test metric".to_string(),
                    unit: "1".to_string(),
                    data: Some(crate::otel::metrics::v1::metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: time_ns,
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

    #[test]
    fn test_evict_traces_by_age() {
        let (mut store, _rx) = Store::new(100, 100, 100);
        // Old trace (end_time = 1_000_000_000)
        store.insert_traces(vec![make_resource_spans_with_ts(
            &[1; 16],
            "old-span",
            500_000_000,
            1_000_000_000,
        )]);
        // Recent trace (end_time = 5_000_000_000)
        store.insert_traces(vec![make_resource_spans_with_ts(
            &[2; 16],
            "new-span",
            4_000_000_000,
            5_000_000_000,
        )]);
        assert_eq!(store.trace_count(), 2);

        // Evict traces older than cutoff=2_000_000_000
        let removed = store.evict_traces_by_age(2_000_000_000);
        assert_eq!(removed, 1);
        assert_eq!(store.trace_count(), 1);
        assert_eq!(store.span_count(), 1);
        // Old trace should be gone
        assert!(!store.trace_id_set.contains(&vec![1u8; 16]));
        // New trace should remain
        assert!(store.trace_id_set.contains(&vec![2u8; 16]));
    }

    #[test]
    fn test_evict_traces_rebuilds_tracking() {
        let (mut store, _rx) = Store::new(100, 100, 100);
        store.insert_traces(vec![make_resource_spans_with_ts(
            &[1; 16], "span-a", 100, 200,
        )]);
        store.insert_traces(vec![make_resource_spans_with_ts(
            &[2; 16], "span-b", 300, 400,
        )]);
        store.insert_traces(vec![make_resource_spans_with_ts(
            &[3; 16], "span-c", 500, 600,
        )]);

        // Evict first two traces
        store.evict_traces_by_age(500);
        assert_eq!(store.trace_count(), 1);
        assert_eq!(store.trace_id_order.len(), 1);
        assert_eq!(store.trace_id_set.len(), 1);
        assert!(store.trace_id_set.contains(&vec![3u8; 16]));
    }

    #[test]
    fn test_evict_logs_by_age() {
        let (mut store, _rx) = Store::new(100, 100, 100);
        store.insert_logs(vec![make_resource_logs_with_ts("old log", 1_000_000_000)]);
        store.insert_logs(vec![make_resource_logs_with_ts("new log", 5_000_000_000)]);
        assert_eq!(store.log_count(), 2);

        let removed = store.evict_logs_by_age(2_000_000_000);
        assert_eq!(removed, 1);
        assert_eq!(store.log_count(), 1);
    }

    #[test]
    fn test_evict_metrics_by_age() {
        let (mut store, _rx) = Store::new(100, 100, 100);
        store.insert_metrics(vec![make_resource_metrics_with_ts(
            "old_metric",
            1.0,
            1_000_000_000,
        )]);
        store.insert_metrics(vec![make_resource_metrics_with_ts(
            "new_metric",
            2.0,
            5_000_000_000,
        )]);
        assert_eq!(store.metric_count(), 2);

        let removed = store.evict_metrics_by_age(2_000_000_000);
        assert_eq!(removed, 1);
        assert_eq!(store.metric_count(), 1);
    }

    #[test]
    fn test_evict_no_items_removed() {
        let (mut store, _rx) = Store::new(100, 100, 100);
        store.insert_traces(vec![make_resource_spans_with_ts(
            &[1; 16],
            "span",
            5_000_000_000,
            6_000_000_000,
        )]);
        // Cutoff is before all data
        let removed = store.evict_traces_by_age(1_000_000_000);
        assert_eq!(removed, 0);
        assert_eq!(store.trace_count(), 1);
    }

    #[test]
    fn test_count_and_age_eviction_coexist() {
        // max_traces=2, insert 3 traces, then age-evict
        let (mut store, _rx) = Store::new(2, 100, 100);
        store.insert_traces(vec![make_resource_spans_with_ts(
            &[1; 16], "span1", 100, 200,
        )]);
        store.insert_traces(vec![make_resource_spans_with_ts(
            &[2; 16], "span2", 300, 400,
        )]);
        store.insert_traces(vec![make_resource_spans_with_ts(
            &[3; 16], "span3", 500, 600,
        )]);
        // Count eviction should have kept only 2 traces (trace 2 and 3)
        assert_eq!(store.trace_count(), 2);

        // Now age-evict trace 2 (end_time=400, cutoff=450)
        let removed = store.evict_traces_by_age(450);
        assert_eq!(removed, 1);
        assert_eq!(store.trace_count(), 1);
        assert!(store.trace_id_set.contains(&vec![3u8; 16]));
    }
}
