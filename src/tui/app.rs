use std::collections::VecDeque;

use crate::otel::{
    common::v1::{AnyValue, KeyValue, any_value},
    logs::v1::ResourceLogs,
    metrics::v1::{ResourceMetrics, metric},
    trace::v1::ResourceSpans,
};
use crate::store::{SharedStore, StoreEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Traces,
    Logs,
    Metrics,
}

impl Tab {
    pub fn all() -> &'static [Tab] {
        &[Tab::Traces, Tab::Logs, Tab::Metrics]
    }

    pub fn index(self) -> usize {
        match self {
            Tab::Traces => 0,
            Tab::Logs => 1,
            Tab::Metrics => 2,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Tab::Traces => "Traces",
            Tab::Logs => "Logs",
            Tab::Metrics => "Metrics",
        }
    }
}

/// A flattened span row for display in the traces table.
#[derive(Clone)]
pub struct SpanRow {
    pub time_nano: u64,
    pub service_name: String,
    pub span_name: String,
    pub duration_ns: u64,
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub parent_span_id: Vec<u8>,
    pub kind: i32,
    pub status_code: i32,
    pub status_message: String,
    pub attributes: Vec<KeyValue>,
    pub resource_attributes: Vec<KeyValue>,
    pub events_count: usize,
    pub links_count: usize,
}

/// A flattened log row for display in the logs table.
#[derive(Clone)]
pub struct LogRow {
    pub time_nano: u64,
    pub service_name: String,
    pub severity_text: String,
    pub severity_number: i32,
    pub body: String,
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub attributes: Vec<KeyValue>,
    pub resource_attributes: Vec<KeyValue>,
}

/// A flattened metric row for display in the metrics table.
#[derive(Clone)]
pub struct MetricRow {
    pub time_nano: u64,
    pub service_name: String,
    pub metric_name: String,
    pub metric_type: String,
    pub value: String,
    pub unit: String,
    pub description: String,
    pub attributes: Vec<KeyValue>,
    pub resource_attributes: Vec<KeyValue>,
}

pub struct TabState {
    pub selected: usize,
    pub offset: usize,
    pub dirty: bool,
}

impl TabState {
    pub fn new() -> Self {
        Self {
            selected: 0,
            offset: 0,
            dirty: true,
        }
    }
}

pub struct App {
    pub current_tab: Tab,
    pub tab_states: [TabState; 3],
    pub detail_open: bool,
    pub detail_scroll: u16,
    pub should_quit: bool,

    // Cached flattened rows
    pub span_rows: Vec<SpanRow>,
    pub log_rows: Vec<LogRow>,
    pub metric_rows: Vec<MetricRow>,

    // Counts from store
    pub trace_count: usize,
    pub span_count: usize,
    pub log_count: usize,
    pub metric_count: usize,
}

impl App {
    pub fn new() -> Self {
        Self {
            current_tab: Tab::Traces,
            tab_states: [TabState::new(), TabState::new(), TabState::new()],
            detail_open: false,
            detail_scroll: 0,
            should_quit: false,
            span_rows: Vec::new(),
            log_rows: Vec::new(),
            metric_rows: Vec::new(),
            trace_count: 0,
            span_count: 0,
            log_count: 0,
            metric_count: 0,
        }
    }

    pub fn current_tab_state(&self) -> &TabState {
        &self.tab_states[self.current_tab.index()]
    }

    pub fn current_tab_state_mut(&mut self) -> &mut TabState {
        &mut self.tab_states[self.current_tab.index()]
    }

    pub fn current_row_count(&self) -> usize {
        match self.current_tab {
            Tab::Traces => self.span_rows.len(),
            Tab::Logs => self.log_rows.len(),
            Tab::Metrics => self.metric_rows.len(),
        }
    }

    pub fn any_dirty(&self) -> bool {
        self.tab_states.iter().any(|s| s.dirty)
    }

    pub fn next_tab(&mut self) {
        self.current_tab = match self.current_tab {
            Tab::Traces => Tab::Logs,
            Tab::Logs => Tab::Metrics,
            Tab::Metrics => Tab::Traces,
        };
    }

    pub fn prev_tab(&mut self) {
        self.current_tab = match self.current_tab {
            Tab::Traces => Tab::Metrics,
            Tab::Logs => Tab::Traces,
            Tab::Metrics => Tab::Logs,
        };
    }

    pub fn select_tab(&mut self, tab: Tab) {
        self.current_tab = tab;
    }

    pub fn move_up(&mut self) {
        let state = self.current_tab_state_mut();
        if state.selected > 0 {
            state.selected -= 1;
        }
    }

    pub fn move_down(&mut self) {
        let count = self.current_row_count();
        let state = self.current_tab_state_mut();
        if count > 0 && state.selected < count - 1 {
            state.selected += 1;
        }
    }

    pub fn page_up(&mut self, page_size: usize) {
        let state = self.current_tab_state_mut();
        state.selected = state.selected.saturating_sub(page_size);
    }

    pub fn page_down(&mut self, page_size: usize) {
        let count = self.current_row_count();
        let state = self.current_tab_state_mut();
        if count > 0 {
            state.selected = (state.selected + page_size).min(count - 1);
        }
    }

    pub fn home(&mut self) {
        self.current_tab_state_mut().selected = 0;
    }

    pub fn end(&mut self) {
        let count = self.current_row_count();
        if count > 0 {
            self.current_tab_state_mut().selected = count - 1;
        }
    }

    pub fn toggle_detail(&mut self) {
        if self.current_row_count() > 0 {
            self.detail_open = !self.detail_open;
            if self.detail_open {
                self.detail_scroll = 0;
            }
        }
    }

    pub fn handle_store_event(&mut self, event: &StoreEvent) {
        match event {
            StoreEvent::TracesInserted(_) | StoreEvent::TracesCleared => {
                self.tab_states[Tab::Traces.index()].dirty = true;
            }
            StoreEvent::LogsInserted(_) | StoreEvent::LogsCleared => {
                self.tab_states[Tab::Logs.index()].dirty = true;
            }
            StoreEvent::MetricsInserted(_) | StoreEvent::MetricsCleared => {
                self.tab_states[Tab::Metrics.index()].dirty = true;
            }
        }
    }

    /// Refresh cached data from the store for all dirty tabs.
    pub async fn refresh_from_store(&mut self, store: &SharedStore) {
        let guard = store.read().await;

        if self.tab_states[Tab::Traces.index()].dirty {
            self.span_rows = flatten_traces(&guard.traces);
            self.trace_count = guard.trace_count();
            self.span_count = guard.span_count();
            self.tab_states[Tab::Traces.index()].dirty = false;
            // Clamp selection
            if !self.span_rows.is_empty() {
                let ts = &mut self.tab_states[Tab::Traces.index()];
                ts.selected = ts.selected.min(self.span_rows.len() - 1);
            } else {
                self.tab_states[Tab::Traces.index()].selected = 0;
            }
        }

        if self.tab_states[Tab::Logs.index()].dirty {
            self.log_rows = flatten_logs(&guard.logs);
            self.log_count = guard.log_count();
            self.tab_states[Tab::Logs.index()].dirty = false;
            if !self.log_rows.is_empty() {
                let ts = &mut self.tab_states[Tab::Logs.index()];
                ts.selected = ts.selected.min(self.log_rows.len() - 1);
            } else {
                self.tab_states[Tab::Logs.index()].selected = 0;
            }
        }

        if self.tab_states[Tab::Metrics.index()].dirty {
            self.metric_rows = flatten_metrics(&guard.metrics);
            self.metric_count = guard.metric_count();
            self.tab_states[Tab::Metrics.index()].dirty = false;
            if !self.metric_rows.is_empty() {
                let ts = &mut self.tab_states[Tab::Metrics.index()];
                ts.selected = ts.selected.min(self.metric_rows.len() - 1);
            } else {
                self.tab_states[Tab::Metrics.index()].selected = 0;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers to flatten nested proto structures into display rows
// ---------------------------------------------------------------------------

fn extract_service_name(attrs: &[KeyValue]) -> String {
    for kv in attrs {
        if kv.key == "service.name" {
            return format_any_value(kv.value.as_ref());
        }
    }
    "<unknown>".to_string()
}

pub fn format_any_value(v: Option<&AnyValue>) -> String {
    match v.and_then(|a| a.value.as_ref()) {
        Some(any_value::Value::StringValue(s)) => s.clone(),
        Some(any_value::Value::IntValue(i)) => i.to_string(),
        Some(any_value::Value::DoubleValue(d)) => format!("{d:.6}"),
        Some(any_value::Value::BoolValue(b)) => b.to_string(),
        Some(any_value::Value::BytesValue(b)) => hex::encode(b),
        Some(any_value::Value::ArrayValue(arr)) => {
            let items: Vec<String> = arr
                .values
                .iter()
                .map(|v| format_any_value(Some(v)))
                .collect();
            format!("[{}]", items.join(", "))
        }
        Some(any_value::Value::KvlistValue(kvl)) => {
            let items: Vec<String> = kvl
                .values
                .iter()
                .map(|kv| format!("{}={}", kv.key, format_any_value(kv.value.as_ref())))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
        _ => "".to_string(),
    }
}

fn flatten_traces(traces: &VecDeque<ResourceSpans>) -> Vec<SpanRow> {
    let mut rows = Vec::new();
    for rs in traces {
        let resource_attrs = rs
            .resource
            .as_ref()
            .map(|r| r.attributes.clone())
            .unwrap_or_default();
        let service = extract_service_name(&resource_attrs);
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                let duration = span
                    .end_time_unix_nano
                    .saturating_sub(span.start_time_unix_nano);
                rows.push(SpanRow {
                    time_nano: span.start_time_unix_nano,
                    service_name: service.clone(),
                    span_name: span.name.clone(),
                    duration_ns: duration,
                    trace_id: span.trace_id.clone(),
                    span_id: span.span_id.clone(),
                    parent_span_id: span.parent_span_id.clone(),
                    kind: span.kind,
                    status_code: span.status.as_ref().map_or(0, |s| s.code),
                    status_message: span
                        .status
                        .as_ref()
                        .map_or_else(String::new, |s| s.message.clone()),
                    attributes: span.attributes.clone(),
                    resource_attributes: resource_attrs.clone(),
                    events_count: span.events.len(),
                    links_count: span.links.len(),
                });
            }
        }
    }
    // Sort by time descending (newest first)
    rows.sort_by(|a, b| b.time_nano.cmp(&a.time_nano));
    rows
}

fn severity_text(num: i32, text: &str) -> String {
    if !text.is_empty() {
        return text.to_string();
    }
    match num {
        1..=4 => "TRACE".to_string(),
        5..=8 => "DEBUG".to_string(),
        9..=12 => "INFO".to_string(),
        13..=16 => "WARN".to_string(),
        17..=20 => "ERROR".to_string(),
        21..=24 => "FATAL".to_string(),
        _ => "UNSPECIFIED".to_string(),
    }
}

fn flatten_logs(logs: &VecDeque<ResourceLogs>) -> Vec<LogRow> {
    let mut rows = Vec::new();
    for rl in logs {
        let resource_attrs = rl
            .resource
            .as_ref()
            .map(|r| r.attributes.clone())
            .unwrap_or_default();
        let service = extract_service_name(&resource_attrs);
        for sl in &rl.scope_logs {
            for rec in &sl.log_records {
                let ts = if rec.time_unix_nano > 0 {
                    rec.time_unix_nano
                } else {
                    rec.observed_time_unix_nano
                };
                rows.push(LogRow {
                    time_nano: ts,
                    service_name: service.clone(),
                    severity_text: severity_text(rec.severity_number, &rec.severity_text),
                    severity_number: rec.severity_number,
                    body: format_any_value(rec.body.as_ref()),
                    trace_id: rec.trace_id.clone(),
                    span_id: rec.span_id.clone(),
                    attributes: rec.attributes.clone(),
                    resource_attributes: resource_attrs.clone(),
                });
            }
        }
    }
    rows.sort_by(|a, b| b.time_nano.cmp(&a.time_nano));
    rows
}

fn flatten_metrics(metrics: &VecDeque<ResourceMetrics>) -> Vec<MetricRow> {
    let mut rows = Vec::new();
    for rm in metrics {
        let resource_attrs = rm
            .resource
            .as_ref()
            .map(|r| r.attributes.clone())
            .unwrap_or_default();
        let service = extract_service_name(&resource_attrs);
        for sm in &rm.scope_metrics {
            for m in &sm.metrics {
                let metric_name = m.name.clone();
                let unit = m.unit.clone();
                let description = m.description.clone();

                match &m.data {
                    Some(metric::Data::Gauge(g)) => {
                        for dp in &g.data_points {
                            rows.push(MetricRow {
                                time_nano: dp.time_unix_nano,
                                service_name: service.clone(),
                                metric_name: metric_name.clone(),
                                metric_type: "Gauge".to_string(),
                                value: format_number_value(&dp.value),
                                unit: unit.clone(),
                                description: description.clone(),
                                attributes: dp.attributes.clone(),
                                resource_attributes: resource_attrs.clone(),
                            });
                        }
                    }
                    Some(metric::Data::Sum(s)) => {
                        for dp in &s.data_points {
                            rows.push(MetricRow {
                                time_nano: dp.time_unix_nano,
                                service_name: service.clone(),
                                metric_name: metric_name.clone(),
                                metric_type: "Sum".to_string(),
                                value: format_number_value(&dp.value),
                                unit: unit.clone(),
                                description: description.clone(),
                                attributes: dp.attributes.clone(),
                                resource_attributes: resource_attrs.clone(),
                            });
                        }
                    }
                    Some(metric::Data::Histogram(h)) => {
                        for dp in &h.data_points {
                            rows.push(MetricRow {
                                time_nano: dp.time_unix_nano,
                                service_name: service.clone(),
                                metric_name: metric_name.clone(),
                                metric_type: "Histogram".to_string(),
                                value: format!(
                                    "count={} sum={:.3}",
                                    dp.count,
                                    dp.sum.unwrap_or(0.0)
                                ),
                                unit: unit.clone(),
                                description: description.clone(),
                                attributes: dp.attributes.clone(),
                                resource_attributes: resource_attrs.clone(),
                            });
                        }
                    }
                    Some(metric::Data::ExponentialHistogram(h)) => {
                        for dp in &h.data_points {
                            rows.push(MetricRow {
                                time_nano: dp.time_unix_nano,
                                service_name: service.clone(),
                                metric_name: metric_name.clone(),
                                metric_type: "ExpHistogram".to_string(),
                                value: format!(
                                    "count={} sum={:.3}",
                                    dp.count,
                                    dp.sum.unwrap_or(0.0)
                                ),
                                unit: unit.clone(),
                                description: description.clone(),
                                attributes: dp.attributes.clone(),
                                resource_attributes: resource_attrs.clone(),
                            });
                        }
                    }
                    Some(metric::Data::Summary(s)) => {
                        for dp in &s.data_points {
                            rows.push(MetricRow {
                                time_nano: dp.time_unix_nano,
                                service_name: service.clone(),
                                metric_name: metric_name.clone(),
                                metric_type: "Summary".to_string(),
                                value: format!("count={} sum={:.3}", dp.count, dp.sum),
                                unit: unit.clone(),
                                description: description.clone(),
                                attributes: dp.attributes.clone(),
                                resource_attributes: resource_attrs.clone(),
                            });
                        }
                    }
                    None => {}
                };
            }
        }
    }
    rows.sort_by(|a, b| b.time_nano.cmp(&a.time_nano));
    rows
}

fn format_number_value(v: &Option<crate::otel::metrics::v1::number_data_point::Value>) -> String {
    use crate::otel::metrics::v1::number_data_point::Value;
    match v {
        Some(Value::AsDouble(d)) => format!("{d:.6}"),
        Some(Value::AsInt(i)) => i.to_string(),
        None => "".to_string(),
    }
}
