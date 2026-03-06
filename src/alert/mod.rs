pub mod notifier;
pub mod rule;

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use tokio::sync::broadcast;

use crate::otel::metrics::v1::metric;
use crate::store::StoreEvent;

use notifier::{AlertNotification, NotificationTarget};
use rule::{AlertCondition, AlertRule};

/// Default cooldown between repeated fires of the same rule.
const DEFAULT_COOLDOWN: Duration = Duration::from_secs(60);

/// The alert engine evaluates rules against incoming store events.
pub struct AlertEngine {
    rules: Vec<AlertRule>,
    targets: Vec<NotificationTarget>,
    /// Track last fire time per rule index for cooldown.
    last_fired: HashMap<usize, Instant>,
    /// For error_rate tracking: timestamps of recent error spans.
    error_timestamps: VecDeque<Instant>,
    /// Cooldown duration between repeated fires of the same rule.
    cooldown: Duration,
}

impl AlertEngine {
    pub fn new(rules: Vec<AlertRule>, targets: Vec<NotificationTarget>) -> Self {
        Self {
            rules,
            targets,
            last_fired: HashMap::new(),
            error_timestamps: VecDeque::new(),
            cooldown: DEFAULT_COOLDOWN,
        }
    }

    /// Set a custom cooldown duration (useful for testing).
    #[cfg(test)]
    pub fn with_cooldown(mut self, cooldown: Duration) -> Self {
        self.cooldown = cooldown;
        self
    }

    /// Main loop: subscribe to StoreEvent broadcast channel, evaluate rules on each event.
    #[tracing::instrument(skip_all, name = "alert.run")]
    pub async fn run(mut self, mut event_rx: broadcast::Receiver<StoreEvent>) {
        tracing::info!("alert engine started with {} rules", self.rules.len());
        loop {
            match event_rx.recv().await {
                Ok(event) => self.evaluate(&event).await,
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("alert engine lagged by {n} events");
                    continue;
                }
            }
        }
        tracing::info!("alert engine stopped");
    }

    #[tracing::instrument(skip_all, name = "alert.evaluate")]
    async fn evaluate(&mut self, event: &StoreEvent) {
        // Collect pending alerts first (to avoid borrow conflicts with self.rules)
        let mut pending: Vec<(usize, String, String)> = Vec::new();

        match event {
            StoreEvent::TracesInserted(resource_spans) => {
                // Handle error_rate bookkeeping outside the rules loop
                let now = Instant::now();
                let mut new_errors = 0u64;
                for rs in resource_spans {
                    for ss in &rs.scope_spans {
                        for span in &ss.spans {
                            let status_code = span.status.as_ref().map_or(0, |s| s.code);
                            if status_code == 2 {
                                new_errors += 1;
                            }
                        }
                    }
                }
                for _ in 0..new_errors {
                    self.error_timestamps.push_back(now);
                }

                for (rule_idx, rule) in self.rules.iter().enumerate() {
                    match &rule.condition {
                        AlertCondition::SpanDuration { threshold } => {
                            for rs in resource_spans {
                                for ss in &rs.scope_spans {
                                    for span in &ss.spans {
                                        let duration_ns = span
                                            .end_time_unix_nano
                                            .saturating_sub(span.start_time_unix_nano);
                                        let duration = Duration::from_nanos(duration_ns);
                                        if duration > *threshold {
                                            pending.push((
                                                rule_idx,
                                                rule.raw.clone(),
                                                format!(
                                                    "span '{}' duration {:.3}s exceeds threshold {:.3}s",
                                                    span.name,
                                                    duration.as_secs_f64(),
                                                    threshold.as_secs_f64()
                                                ),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        AlertCondition::ErrorRate { max_count, window } => {
                            // Prune old timestamps outside window
                            let cutoff = now - *window;
                            while self.error_timestamps.front().is_some_and(|t| *t < cutoff) {
                                self.error_timestamps.pop_front();
                            }
                            if self.error_timestamps.len() as u64 > *max_count {
                                pending.push((
                                    rule_idx,
                                    rule.raw.clone(),
                                    format!(
                                        "error rate {} exceeds threshold {} in {:?} window",
                                        self.error_timestamps.len(),
                                        max_count,
                                        window
                                    ),
                                ));
                            }
                        }
                        _ => {}
                    }
                }
            }
            StoreEvent::LogsInserted(resource_logs) => {
                for (rule_idx, rule) in self.rules.iter().enumerate() {
                    match &rule.condition {
                        AlertCondition::LogBodyContains { pattern } => {
                            for rl in resource_logs {
                                for sl in &rl.scope_logs {
                                    for lr in &sl.log_records {
                                        let body_text = lr
                                            .body
                                            .as_ref()
                                            .and_then(|v| v.value.as_ref())
                                            .map(|v| format!("{v:?}"))
                                            .unwrap_or_default();
                                        if body_text.contains(pattern.as_str()) {
                                            pending.push((
                                                rule_idx,
                                                rule.raw.clone(),
                                                format!(
                                                    "log body contains '{}': {}",
                                                    pattern,
                                                    truncate(&body_text, 100)
                                                ),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        AlertCondition::LogSeverityAtLeast { min_severity } => {
                            for rl in resource_logs {
                                for sl in &rl.scope_logs {
                                    for lr in &sl.log_records {
                                        if lr.severity_number >= *min_severity {
                                            let body_text = lr
                                                .body
                                                .as_ref()
                                                .and_then(|v| v.value.as_ref())
                                                .map(|v| format!("{v:?}"))
                                                .unwrap_or_default();
                                            pending.push((
                                                rule_idx,
                                                rule.raw.clone(),
                                                format!(
                                                    "log severity {} >= {}: {}",
                                                    lr.severity_number,
                                                    min_severity,
                                                    truncate(&body_text, 100)
                                                ),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            StoreEvent::MetricsInserted(resource_metrics) => {
                for (rule_idx, rule) in self.rules.iter().enumerate() {
                    if let AlertCondition::MetricThreshold {
                        metric_name,
                        op,
                        threshold,
                    } = &rule.condition
                    {
                        for rm in resource_metrics {
                            for sm in &rm.scope_metrics {
                                for m in &sm.metrics {
                                    if m.name != *metric_name {
                                        continue;
                                    }
                                    let values = extract_metric_values(&m.data);
                                    for value in values {
                                        if op.eval(value, *threshold) {
                                            pending.push((
                                                rule_idx,
                                                rule.raw.clone(),
                                                format!(
                                                    "metric '{}' value {value} triggered (threshold {threshold})",
                                                    metric_name,
                                                ),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Clear events don't trigger alerts
            StoreEvent::TracesCleared | StoreEvent::LogsCleared | StoreEvent::MetricsCleared => {}
        }

        // Fire all pending alerts
        for (rule_idx, rule, message) in pending {
            self.fire(rule_idx, &rule, message).await;
        }
    }

    /// Fire a notification for a rule, respecting cooldown.
    async fn fire(&mut self, rule_idx: usize, rule: &str, message: String) {
        let now = Instant::now();
        if self
            .last_fired
            .get(&rule_idx)
            .is_some_and(|last| now.duration_since(*last) < self.cooldown)
        {
            return; // Still in cooldown
        }
        self.last_fired.insert(rule_idx, now);

        let notification = AlertNotification {
            rule: rule.to_string(),
            message,
            timestamp: chrono::Utc::now().to_rfc3339(),
        };

        for target in &self.targets {
            target.send(&notification).await;
        }
    }
}

/// Extract numeric values from metric data points.
fn extract_metric_values(data: &Option<metric::Data>) -> Vec<f64> {
    let mut values = Vec::new();
    match data {
        Some(metric::Data::Gauge(g)) => {
            for dp in &g.data_points {
                if let Some(v) = number_data_point_value(&dp.value) {
                    values.push(v);
                }
            }
        }
        Some(metric::Data::Sum(s)) => {
            for dp in &s.data_points {
                if let Some(v) = number_data_point_value(&dp.value) {
                    values.push(v);
                }
            }
        }
        Some(metric::Data::Histogram(h)) => {
            for dp in &h.data_points {
                // Use sum as representative value for histograms
                if dp.sum.is_some() {
                    values.push(dp.sum());
                }
            }
        }
        _ => {}
    }
    values
}

fn number_data_point_value(
    v: &Option<crate::otel::metrics::v1::number_data_point::Value>,
) -> Option<f64> {
    use crate::otel::metrics::v1::number_data_point::Value;
    match v {
        Some(Value::AsDouble(d)) => Some(*d),
        Some(Value::AsInt(i)) => Some(*i as f64),
        None => None,
    }
}

/// Truncate a string to max_len characters, appending "..." if truncated.
fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otel::common::v1::{AnyValue, KeyValue, any_value};
    use crate::otel::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use crate::otel::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, number_data_point,
    };
    use crate::otel::resource::v1::Resource;
    use crate::otel::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

    /// Helper to create an engine with stderr target and zero cooldown for testing.
    fn test_engine(rules: Vec<AlertRule>) -> AlertEngine {
        AlertEngine::new(rules, vec![NotificationTarget::Stderr]).with_cooldown(Duration::ZERO)
    }

    fn make_span(name: &str, start_ns: u64, end_ns: u64, status_code: i32) -> Span {
        Span {
            trace_id: vec![1; 16],
            span_id: vec![1; 8],
            name: name.to_string(),
            start_time_unix_nano: start_ns,
            end_time_unix_nano: end_ns,
            status: Some(Status {
                code: status_code,
                message: String::new(),
            }),
            ..Default::default()
        }
    }

    fn make_trace_event(spans: Vec<Span>) -> StoreEvent {
        StoreEvent::TracesInserted(vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".into())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }])
    }

    fn make_log_event(body: &str, severity_number: i32) -> StoreEvent {
        StoreEvent::LogsInserted(vec![ResourceLogs {
            resource: Some(Resource::default()),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(body.to_string())),
                    }),
                    severity_number,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }])
    }

    fn make_metric_event(name: &str, value: f64) -> StoreEvent {
        StoreEvent::MetricsInserted(vec![ResourceMetrics {
            resource: Some(Resource::default()),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: name.to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            value: Some(number_data_point::Value::AsDouble(value)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }])
    }

    #[tokio::test]
    async fn test_span_duration_fires() {
        let rule = AlertRule::parse("span_duration > 1s").unwrap();
        let mut engine = test_engine(vec![rule]);

        // Span with 2s duration should fire
        let event = make_trace_event(vec![make_span(
            "slow-op",
            0,
            2_000_000_000, // 2s in nanos
            0,
        )]);
        engine.evaluate(&event).await;
        assert!(engine.last_fired.contains_key(&0));
    }

    #[tokio::test]
    async fn test_span_duration_no_fire_under_threshold() {
        let rule = AlertRule::parse("span_duration > 5s").unwrap();
        let mut engine = test_engine(vec![rule]);

        // Span with 1s duration should NOT fire
        let event = make_trace_event(vec![make_span("fast-op", 0, 1_000_000_000, 0)]);
        engine.evaluate(&event).await;
        assert!(!engine.last_fired.contains_key(&0));
    }

    #[tokio::test]
    async fn test_error_rate_fires() {
        let rule = AlertRule::parse("error_rate > 2/1m").unwrap();
        let mut engine = test_engine(vec![rule]);

        // Insert 3 error spans
        for _ in 0..3 {
            let event = make_trace_event(vec![make_span("op", 0, 1_000_000, 2)]);
            engine.evaluate(&event).await;
        }
        assert!(engine.last_fired.contains_key(&0));
    }

    #[tokio::test]
    async fn test_error_rate_no_fire_under_threshold() {
        let rule = AlertRule::parse("error_rate > 5/1m").unwrap();
        let mut engine = test_engine(vec![rule]);

        // Insert 2 error spans (under threshold of 5)
        for _ in 0..2 {
            let event = make_trace_event(vec![make_span("op", 0, 1_000_000, 2)]);
            engine.evaluate(&event).await;
        }
        assert!(!engine.last_fired.contains_key(&0));
    }

    #[tokio::test]
    async fn test_log_body_contains_fires() {
        let rule = AlertRule::parse("log_body contains 'panic'").unwrap();
        let mut engine = test_engine(vec![rule]);

        let event = make_log_event("something panic happened", 17);
        engine.evaluate(&event).await;
        assert!(engine.last_fired.contains_key(&0));
    }

    #[tokio::test]
    async fn test_log_body_no_match() {
        let rule = AlertRule::parse("log_body contains 'panic'").unwrap();
        let mut engine = test_engine(vec![rule]);

        let event = make_log_event("everything is fine", 9);
        engine.evaluate(&event).await;
        assert!(!engine.last_fired.contains_key(&0));
    }

    #[tokio::test]
    async fn test_log_severity_fires() {
        let rule = AlertRule::parse("log_severity >= ERROR").unwrap();
        let mut engine = test_engine(vec![rule]);

        let event = make_log_event("an error", 17); // ERROR = 17
        engine.evaluate(&event).await;
        assert!(engine.last_fired.contains_key(&0));
    }

    #[tokio::test]
    async fn test_log_severity_no_fire_under() {
        let rule = AlertRule::parse("log_severity >= ERROR").unwrap();
        let mut engine = test_engine(vec![rule]);

        let event = make_log_event("info message", 9); // INFO = 9
        engine.evaluate(&event).await;
        assert!(!engine.last_fired.contains_key(&0));
    }

    #[tokio::test]
    async fn test_metric_threshold_fires() {
        let rule = AlertRule::parse("metric cpu.usage > 90.0").unwrap();
        let mut engine = test_engine(vec![rule]);

        let event = make_metric_event("cpu.usage", 95.0);
        engine.evaluate(&event).await;
        assert!(engine.last_fired.contains_key(&0));
    }

    #[tokio::test]
    async fn test_metric_threshold_no_fire() {
        let rule = AlertRule::parse("metric cpu.usage > 90.0").unwrap();
        let mut engine = test_engine(vec![rule]);

        let event = make_metric_event("cpu.usage", 50.0);
        engine.evaluate(&event).await;
        assert!(!engine.last_fired.contains_key(&0));
    }

    #[tokio::test]
    async fn test_metric_wrong_name_no_fire() {
        let rule = AlertRule::parse("metric cpu.usage > 90.0").unwrap();
        let mut engine = test_engine(vec![rule]);

        let event = make_metric_event("memory.usage", 95.0);
        engine.evaluate(&event).await;
        assert!(!engine.last_fired.contains_key(&0));
    }

    #[tokio::test]
    async fn test_cooldown_prevents_refire() {
        let rule = AlertRule::parse("span_duration > 1s").unwrap();
        let mut engine = AlertEngine::new(vec![rule], vec![NotificationTarget::Stderr])
            .with_cooldown(Duration::from_secs(60));

        let event = make_trace_event(vec![make_span("slow", 0, 2_000_000_000, 0)]);

        // First fire
        engine.evaluate(&event).await;
        let first_fire = engine.last_fired[&0];

        // Second evaluation - should not update last_fired due to cooldown
        engine.evaluate(&event).await;
        assert_eq!(engine.last_fired[&0], first_fire);
    }

    #[tokio::test]
    async fn test_multiple_rules_independent() {
        let rules = vec![
            AlertRule::parse("span_duration > 1s").unwrap(),
            AlertRule::parse("log_severity >= ERROR").unwrap(),
        ];
        let mut engine = test_engine(rules);

        // Only trace event fires rule 0, not rule 1
        let event = make_trace_event(vec![make_span("slow", 0, 2_000_000_000, 0)]);
        engine.evaluate(&event).await;
        assert!(engine.last_fired.contains_key(&0));
        assert!(!engine.last_fired.contains_key(&1));

        // Log event fires rule 1
        let event = make_log_event("error!", 17);
        engine.evaluate(&event).await;
        assert!(engine.last_fired.contains_key(&1));
    }
}
