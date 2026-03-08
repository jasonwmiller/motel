use std::collections::{HashMap, VecDeque};

use ratatui::style::Color;

use crate::otel::{
    common::v1::{AnyValue, KeyValue, any_value},
    logs::v1::ResourceLogs,
    metrics::v1::{ResourceMetrics, metric},
    trace::v1::ResourceSpans,
};
use crate::store::{SharedStore, StoreEvent};

// ---------------------------------------------------------------------------
// Tab ordering: 1:Logs, 2:Traces, 3:Metrics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Logs,
    Traces,
    Metrics,
}

impl Tab {
    pub fn all() -> &'static [Tab] {
        &[Tab::Logs, Tab::Traces, Tab::Metrics]
    }

    pub fn index(self) -> usize {
        match self {
            Tab::Logs => 0,
            Tab::Traces => 1,
            Tab::Metrics => 2,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Tab::Logs => "Logs",
            Tab::Traces => "Traces",
            Tab::Metrics => "Metrics",
        }
    }

    pub fn number(self) -> usize {
        self.index() + 1
    }
}

// ---------------------------------------------------------------------------
// Trace view modes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceView {
    List,
    Timeline(Vec<u8>), // trace_id being viewed
    Diff,
}

// ---------------------------------------------------------------------------
// Data row types
// ---------------------------------------------------------------------------

/// A trace group: all spans sharing a trace_id, with computed metadata.
#[derive(Clone)]
pub struct TraceGroup {
    pub trace_id: Vec<u8>,
    pub service_name: String,
    pub root_span_name: String,
    pub span_count: usize,
    pub duration_ns: u64,
    pub start_time_nano: u64,
    pub spans: Vec<SpanRow>,
    pub pinned: bool,
}

/// A flattened span row for display.
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

/// A tree node for the timeline waterfall view.
#[derive(Clone)]
pub struct SpanTreeNode {
    pub span: SpanRow,
    pub depth: usize,
}

/// A flattened log row for display.
#[derive(Clone)]
pub struct LogRow {
    pub time_nano: u64,
    pub service_name: String,
    pub severity_text: String,
    pub severity_number: i32,
    pub body: String,
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub scope_name: String,
    pub attributes: Vec<KeyValue>,
    pub resource_attributes: Vec<KeyValue>,
}

/// An aggregated metric: grouped by name + service.
#[derive(Clone)]
pub struct AggregatedMetric {
    pub metric_name: String,
    pub metric_type: String,
    pub service_name: String,
    pub unit: String,
    pub description: String,
    pub data_points: Vec<MetricDataPoint>,
    pub resource_attributes: Vec<KeyValue>,
}

impl AggregatedMetric {
    /// Display value for the metrics table. For gauge/sum with numeric values,
    /// shows the sum across all data points. Otherwise shows the latest value.
    pub fn display_value(&self) -> String {
        if self.data_points.is_empty() {
            return "-".to_string();
        }
        if self.data_points.len() == 1 {
            return self.data_points[0].value.clone();
        }
        // For gauge/sum types, try to sum numeric values
        if self.metric_type == "gauge" || self.metric_type == "sum" {
            let mut total: f64 = 0.0;
            let mut all_int = true;
            let mut ok = true;
            for dp in &self.data_points {
                if let Ok(v) = dp.value.parse::<f64>() {
                    total += v;
                    if dp.value.contains('.') {
                        all_int = false;
                    }
                } else {
                    ok = false;
                    break;
                }
            }
            if ok {
                return if all_int {
                    format!("{}", total as i64)
                } else {
                    format!("{:.6}", total)
                };
            }
        }
        // Fallback: latest value
        self.data_points[0].value.clone()
    }
}

/// A single metric data point within an aggregated metric.
#[derive(Clone)]
pub struct MetricDataPoint {
    pub time_nano: u64,
    pub value: String,
    pub attributes: Vec<KeyValue>,
}

// ---------------------------------------------------------------------------
// Tab state
// ---------------------------------------------------------------------------

pub struct TabState {
    pub selected: usize,
    pub dirty: bool,
}

impl TabState {
    pub fn new() -> Self {
        Self {
            selected: 0,
            dirty: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Service color palette
// ---------------------------------------------------------------------------

const SERVICE_COLORS: [Color; 8] = [
    Color::Rgb(97, 175, 239),  // soft blue
    Color::Rgb(152, 195, 121), // soft green
    Color::Rgb(229, 192, 123), // warm yellow
    Color::Rgb(198, 120, 221), // purple
    Color::Rgb(86, 182, 194),  // teal
    Color::Rgb(224, 108, 117), // soft red
    Color::Rgb(209, 154, 102), // orange
    Color::Rgb(190, 190, 190), // light gray
];

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

pub struct App {
    pub current_tab: Tab,
    pub tab_states: [TabState; 3],
    pub should_quit: bool,

    // Follow mode: auto-scroll to newest
    pub follow_mode: bool,

    // Detail scroll for side panels
    pub detail_scroll: u16,

    // Trace view state
    pub trace_view: TraceView,
    pub timeline_selected: usize,
    pub timeline_nodes: Vec<SpanTreeNode>,

    // Cached data
    pub trace_groups: Vec<TraceGroup>,
    pub log_rows: Vec<LogRow>,
    pub aggregated_metrics: Vec<AggregatedMetric>,

    // Counts
    pub trace_count: usize,
    pub span_count: usize,
    pub log_count: usize,
    pub metric_count: usize,

    // Metric graph mode (toggle with 'g')
    pub metric_graph_mode: bool,

    // Diff view state
    pub diff_result: Option<crate::diff::DiffResult>,
    pub diff_selected: usize,
    pub marked_trace_id: Option<Vec<u8>>,

    // Service -> color mapping
    pub service_colors: HashMap<String, Color>,

    // Multi-server mode: true when viewing data from multiple servers
    pub multi_server: bool,
}

impl App {
    pub fn new() -> Self {
        Self {
            current_tab: Tab::Logs,
            tab_states: [TabState::new(), TabState::new(), TabState::new()],
            should_quit: false,
            follow_mode: true,
            detail_scroll: 0,
            trace_view: TraceView::List,
            timeline_selected: 0,
            timeline_nodes: Vec::new(),
            trace_groups: Vec::new(),
            log_rows: Vec::new(),
            aggregated_metrics: Vec::new(),
            metric_graph_mode: false,
            diff_result: None,
            diff_selected: 0,
            marked_trace_id: None,
            trace_count: 0,
            span_count: 0,
            log_count: 0,
            metric_count: 0,
            service_colors: HashMap::new(),
            multi_server: false,
        }
    }

    pub fn service_color(&mut self, service: &str) -> Color {
        if let Some(&color) = self.service_colors.get(service) {
            return color;
        }
        let idx = self.service_colors.len() % SERVICE_COLORS.len();
        let color = SERVICE_COLORS[idx];
        self.service_colors.insert(service.to_string(), color);
        color
    }

    pub fn current_row_count(&self) -> usize {
        match self.current_tab {
            Tab::Traces => match &self.trace_view {
                TraceView::List => self.trace_groups.len(),
                TraceView::Diff => self
                    .diff_result
                    .as_ref()
                    .map(|d| d.span_diffs.len())
                    .unwrap_or(0),
                TraceView::Timeline(_) => self.timeline_nodes.len(),
            },
            Tab::Logs => self.log_rows.len(),
            Tab::Metrics => self.aggregated_metrics.len(),
        }
    }

    pub fn any_dirty(&self) -> bool {
        self.tab_states.iter().any(|s| s.dirty)
    }

    pub fn next_tab(&mut self) {
        self.current_tab = match self.current_tab {
            Tab::Logs => Tab::Traces,
            Tab::Traces => Tab::Metrics,
            Tab::Metrics => Tab::Logs,
        };
        self.detail_scroll = 0;
    }

    pub fn prev_tab(&mut self) {
        self.current_tab = match self.current_tab {
            Tab::Logs => Tab::Metrics,
            Tab::Traces => Tab::Logs,
            Tab::Metrics => Tab::Traces,
        };
        self.detail_scroll = 0;
    }

    pub fn select_tab(&mut self, tab: Tab) {
        self.current_tab = tab;
        self.detail_scroll = 0;
    }

    /// Returns a mutable reference to the "selected index" for the current
    /// non-list trace sub-view (timeline or diff), or None if in list mode.
    fn trace_subview_selected(&mut self) -> Option<&mut usize> {
        if self.current_tab == Tab::Traces {
            match self.trace_view {
                TraceView::Timeline(_) => Some(&mut self.timeline_selected),
                TraceView::Diff => Some(&mut self.diff_selected),
                TraceView::List => None,
            }
        } else {
            None
        }
    }

    pub fn move_up(&mut self) {
        if let Some(sel) = self.trace_subview_selected() {
            if *sel > 0 {
                *sel -= 1;
                self.detail_scroll = 0;
            }
            return;
        }
        let state = &mut self.tab_states[self.current_tab.index()];
        if state.selected > 0 {
            state.selected -= 1;
            self.detail_scroll = 0;
        }
    }

    pub fn move_down(&mut self) {
        let count = self.current_row_count();
        if let Some(sel) = self.trace_subview_selected() {
            if count > 0 && *sel < count - 1 {
                *sel += 1;
                self.detail_scroll = 0;
            }
            return;
        }
        let state = &mut self.tab_states[self.current_tab.index()];
        if count > 0 && state.selected < count - 1 {
            state.selected += 1;
            self.detail_scroll = 0;
        }
    }

    pub fn page_up(&mut self, page_size: usize) {
        if let Some(sel) = self.trace_subview_selected() {
            *sel = sel.saturating_sub(page_size);
            self.detail_scroll = 0;
            return;
        }
        let state = &mut self.tab_states[self.current_tab.index()];
        state.selected = state.selected.saturating_sub(page_size);
        self.detail_scroll = 0;
    }

    pub fn page_down(&mut self, page_size: usize) {
        let count = self.current_row_count();
        if let Some(sel) = self.trace_subview_selected() {
            if count > 0 {
                *sel = (*sel + page_size).min(count - 1);
                self.detail_scroll = 0;
            }
            return;
        }
        let state = &mut self.tab_states[self.current_tab.index()];
        if count > 0 {
            state.selected = (state.selected + page_size).min(count - 1);
            self.detail_scroll = 0;
        }
    }

    pub fn home(&mut self) {
        if let Some(sel) = self.trace_subview_selected() {
            *sel = 0;
        } else {
            self.tab_states[self.current_tab.index()].selected = 0;
        }
        self.detail_scroll = 0;
    }

    pub fn end(&mut self) {
        let count = self.current_row_count();
        if count > 0 {
            if let Some(sel) = self.trace_subview_selected() {
                *sel = count - 1;
            } else {
                self.tab_states[self.current_tab.index()].selected = count - 1;
            }
        }
        self.detail_scroll = 0;
    }

    pub fn toggle_follow(&mut self) {
        self.follow_mode = !self.follow_mode;
    }

    /// Open the selected trace in timeline view.
    pub fn open_trace(&mut self) {
        if self.current_tab != Tab::Traces || self.trace_view != TraceView::List {
            return;
        }
        let idx = self.tab_states[Tab::Traces.index()].selected;
        if let Some(group) = self.trace_groups.get(idx) {
            let trace_id = group.trace_id.clone();
            self.timeline_nodes = build_span_tree(&group.spans);
            self.timeline_selected = 0;
            self.detail_scroll = 0;
            self.trace_view = TraceView::Timeline(trace_id);
        }
    }

    /// Go back from timeline or diff to trace list.
    pub fn close_timeline(&mut self) {
        self.trace_view = TraceView::List;
        self.detail_scroll = 0;
    }

    /// Mark the currently selected trace for diffing.
    pub fn mark_trace(&mut self) {
        if self.current_tab != Tab::Traces || self.trace_view != TraceView::List {
            return;
        }
        let idx = self.tab_states[Tab::Traces.index()].selected;
        if let Some(group) = self.trace_groups.get(idx) {
            self.marked_trace_id = Some(group.trace_id.clone());
        }
    }

    /// Compute diff between the marked trace and the currently selected trace.
    pub fn diff_traces(&mut self) {
        if self.current_tab != Tab::Traces || self.trace_view != TraceView::List {
            return;
        }
        let Some(ref marked_id) = self.marked_trace_id else {
            return;
        };
        let idx = self.tab_states[Tab::Traces.index()].selected;
        let Some(current_group) = self.trace_groups.get(idx) else {
            return;
        };
        // Don't diff a trace with itself
        if *marked_id == current_group.trace_id {
            return;
        }
        let Some(marked_group) = self.trace_groups.iter().find(|g| g.trace_id == *marked_id) else {
            return;
        };

        let diff = crate::diff::compute_diff(&marked_group.spans, &current_group.spans);
        self.diff_result = Some(diff);
        self.diff_selected = 0;
        self.detail_scroll = 0;
        self.trace_view = TraceView::Diff;
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

    /// Get the trace_id of the currently selected trace (if in Traces list view).
    pub fn get_selected_trace_id(&self) -> Option<Vec<u8>> {
        if !matches!(self.current_tab, Tab::Traces) || !matches!(self.trace_view, TraceView::List) {
            return None;
        }
        let selected = self.tab_states[Tab::Traces.index()].selected;
        let group = self.trace_groups.get(selected)?;
        Some(group.trace_id.clone())
    }

    pub async fn refresh_from_store(&mut self, store: &SharedStore) {
        let guard = store.read().await;

        if self.tab_states[Tab::Traces.index()].dirty {
            let all_spans = flatten_traces(&guard.traces);
            self.trace_groups = group_traces(all_spans);
            // Populate pinned state from store
            for group in &mut self.trace_groups {
                group.pinned = guard.is_pinned(&group.trace_id);
            }
            self.trace_count = guard.trace_count();
            self.span_count = guard.span_count();
            self.tab_states[Tab::Traces.index()].dirty = false;

            if self.follow_mode && !self.trace_groups.is_empty() {
                self.tab_states[Tab::Traces.index()].selected = self.trace_groups.len() - 1;
            } else if !self.trace_groups.is_empty() {
                let ts = &mut self.tab_states[Tab::Traces.index()];
                ts.selected = ts.selected.min(self.trace_groups.len() - 1);
            } else {
                self.tab_states[Tab::Traces.index()].selected = 0;
            }

            // Refresh timeline if viewing one
            if let TraceView::Timeline(ref tid) = self.trace_view
                && let Some(group) = self.trace_groups.iter().find(|g| g.trace_id == *tid)
            {
                self.timeline_nodes = build_span_tree(&group.spans);
                if self.timeline_selected >= self.timeline_nodes.len() {
                    self.timeline_selected = self.timeline_nodes.len().saturating_sub(1);
                }
            }
        }

        if self.tab_states[Tab::Logs.index()].dirty {
            self.log_rows = flatten_logs(&guard.logs);
            self.log_count = guard.log_count();
            self.tab_states[Tab::Logs.index()].dirty = false;

            if self.follow_mode && !self.log_rows.is_empty() {
                self.tab_states[Tab::Logs.index()].selected = self.log_rows.len() - 1;
            } else if !self.log_rows.is_empty() {
                let ts = &mut self.tab_states[Tab::Logs.index()];
                ts.selected = ts.selected.min(self.log_rows.len() - 1);
            } else {
                self.tab_states[Tab::Logs.index()].selected = 0;
            }
        }

        if self.tab_states[Tab::Metrics.index()].dirty {
            self.aggregated_metrics = aggregate_metrics(&guard.metrics);
            self.metric_count = guard.metric_count();
            self.tab_states[Tab::Metrics.index()].dirty = false;

            if !self.aggregated_metrics.is_empty() {
                let ts = &mut self.tab_states[Tab::Metrics.index()];
                ts.selected = ts.selected.min(self.aggregated_metrics.len() - 1);
            } else {
                self.tab_states[Tab::Metrics.index()].selected = 0;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn extract_service_name(attrs: &[KeyValue]) -> String {
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

// ---------------------------------------------------------------------------
// Flatten + group traces
// ---------------------------------------------------------------------------

pub fn flatten_traces(traces: &VecDeque<ResourceSpans>) -> Vec<SpanRow> {
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
    rows
}

pub fn group_traces(spans: Vec<SpanRow>) -> Vec<TraceGroup> {
    let mut groups: HashMap<Vec<u8>, Vec<SpanRow>> = HashMap::new();
    for span in spans {
        groups.entry(span.trace_id.clone()).or_default().push(span);
    }

    let mut trace_groups: Vec<TraceGroup> = groups
        .into_iter()
        .map(|(trace_id, mut spans)| {
            // Sort spans by start time
            spans.sort_by_key(|s| s.time_nano);

            // Find root span (empty parent) or earliest span
            let root = spans
                .iter()
                .find(|s| s.parent_span_id.is_empty())
                .unwrap_or(&spans[0]);

            let service_name = root.service_name.clone();
            let root_span_name = root.span_name.clone();
            let start_time = root.time_nano;

            // Duration: from earliest start to latest end
            let min_start = spans.iter().map(|s| s.time_nano).min().unwrap_or(0);
            let max_end = spans
                .iter()
                .map(|s| s.time_nano + s.duration_ns)
                .max()
                .unwrap_or(0);
            let duration = max_end.saturating_sub(min_start);

            let span_count = spans.len();

            TraceGroup {
                trace_id,
                service_name,
                root_span_name,
                span_count,
                duration_ns: duration,
                start_time_nano: start_time,
                spans,
                pinned: false,
            }
        })
        .collect();

    // Sort by start time ascending (oldest first, newest last)
    trace_groups.sort_by_key(|g| g.start_time_nano);
    trace_groups
}

/// Build a depth-first span tree for the timeline waterfall.
pub fn build_span_tree(spans: &[SpanRow]) -> Vec<SpanTreeNode> {
    if spans.is_empty() {
        return Vec::new();
    }

    // Build parent -> children map
    let mut children: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    let mut roots = Vec::new();

    for (i, span) in spans.iter().enumerate() {
        if span.parent_span_id.is_empty() {
            roots.push(i);
        } else {
            children
                .entry(span.parent_span_id.clone())
                .or_default()
                .push(i);
        }
    }

    // If no root found, use earliest span
    if roots.is_empty() {
        roots.push(0);
    }

    let mut result = Vec::new();
    let mut stack: Vec<(usize, usize)> = Vec::new(); // (span_index, depth)

    // Start with roots
    for &root_idx in roots.iter().rev() {
        stack.push((root_idx, 0));
    }

    while let Some((idx, depth)) = stack.pop() {
        result.push(SpanTreeNode {
            span: spans[idx].clone(),
            depth,
        });

        // Push children in reverse order so they come out in order
        if let Some(child_indices) = children.get(&spans[idx].span_id) {
            let mut sorted_children: Vec<usize> = child_indices.clone();
            sorted_children.sort_by_key(|&i| spans[i].time_nano);
            for &child_idx in sorted_children.iter().rev() {
                stack.push((child_idx, depth + 1));
            }
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Flatten logs
// ---------------------------------------------------------------------------

pub fn flatten_logs(logs: &VecDeque<ResourceLogs>) -> Vec<LogRow> {
    let mut rows = Vec::new();
    for rl in logs {
        let resource_attrs = rl
            .resource
            .as_ref()
            .map(|r| r.attributes.clone())
            .unwrap_or_default();
        let service = extract_service_name(&resource_attrs);
        for sl in &rl.scope_logs {
            let scope_name = sl
                .scope
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_default();
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
                    scope_name: scope_name.clone(),
                    attributes: rec.attributes.clone(),
                    resource_attributes: resource_attrs.clone(),
                });
            }
        }
    }
    // Sort ascending (oldest first, newest last)
    rows.sort_by_key(|r| r.time_nano);
    rows
}

// ---------------------------------------------------------------------------
// Aggregate metrics
// ---------------------------------------------------------------------------

pub fn aggregate_metrics(metrics: &VecDeque<ResourceMetrics>) -> Vec<AggregatedMetric> {
    // Key: (metric_name, service_name)
    let mut map: HashMap<(String, String), AggregatedMetric> = HashMap::new();

    for rm in metrics {
        let resource_attrs = rm
            .resource
            .as_ref()
            .map(|r| r.attributes.clone())
            .unwrap_or_default();
        let service = extract_service_name(&resource_attrs);
        for sm in &rm.scope_metrics {
            for m in &sm.metrics {
                let key = (m.name.clone(), service.clone());
                let entry = map.entry(key).or_insert_with(|| AggregatedMetric {
                    metric_name: m.name.clone(),
                    metric_type: String::new(),
                    service_name: service.clone(),
                    unit: m.unit.clone(),
                    description: m.description.clone(),
                    data_points: Vec::new(),
                    resource_attributes: resource_attrs.clone(),
                });

                match &m.data {
                    Some(metric::Data::Gauge(g)) => {
                        entry.metric_type = "gauge".to_string();
                        for dp in &g.data_points {
                            entry.data_points.push(MetricDataPoint {
                                time_nano: dp.time_unix_nano,
                                value: format_number_value(&dp.value),
                                attributes: dp.attributes.clone(),
                            });
                        }
                    }
                    Some(metric::Data::Sum(s)) => {
                        entry.metric_type = "sum".to_string();
                        for dp in &s.data_points {
                            entry.data_points.push(MetricDataPoint {
                                time_nano: dp.time_unix_nano,
                                value: format_number_value(&dp.value),
                                attributes: dp.attributes.clone(),
                            });
                        }
                    }
                    Some(metric::Data::Histogram(h)) => {
                        entry.metric_type = "histogram".to_string();
                        for dp in &h.data_points {
                            entry.data_points.push(MetricDataPoint {
                                time_nano: dp.time_unix_nano,
                                value: format!(
                                    "count={} sum={:.3}",
                                    dp.count,
                                    dp.sum.unwrap_or(0.0)
                                ),
                                attributes: dp.attributes.clone(),
                            });
                        }
                    }
                    Some(metric::Data::ExponentialHistogram(h)) => {
                        entry.metric_type = "exp_histogram".to_string();
                        for dp in &h.data_points {
                            entry.data_points.push(MetricDataPoint {
                                time_nano: dp.time_unix_nano,
                                value: format!(
                                    "count={} sum={:.3}",
                                    dp.count,
                                    dp.sum.unwrap_or(0.0)
                                ),
                                attributes: dp.attributes.clone(),
                            });
                        }
                    }
                    Some(metric::Data::Summary(s)) => {
                        entry.metric_type = "summary".to_string();
                        for dp in &s.data_points {
                            entry.data_points.push(MetricDataPoint {
                                time_nano: dp.time_unix_nano,
                                value: format!("count={} sum={:.3}", dp.count, dp.sum),
                                attributes: dp.attributes.clone(),
                            });
                        }
                    }
                    None => {
                        if entry.metric_type.is_empty() {
                            entry.metric_type = "unknown".to_string();
                        }
                    }
                };
            }
        }
    }

    let mut result: Vec<AggregatedMetric> = map.into_values().collect();
    // Sort data points within each metric: newest first
    for m in &mut result {
        m.data_points.sort_by(|a, b| b.time_nano.cmp(&a.time_nano));
    }
    // Sort metrics alphabetically
    result.sort_by(|a, b| a.metric_name.cmp(&b.metric_name));
    result
}

fn format_number_value(v: &Option<crate::otel::metrics::v1::number_data_point::Value>) -> String {
    use crate::otel::metrics::v1::number_data_point::Value;
    match v {
        Some(Value::AsDouble(d)) => format!("{d:.6}"),
        Some(Value::AsInt(i)) => i.to_string(),
        None => "".to_string(),
    }
}
