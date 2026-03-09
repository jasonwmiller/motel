use std::collections::HashSet;
use std::fmt::Write;

use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};

use crate::otel::common::v1::KeyValue;
use crate::otel::metrics::v1::{ResourceMetrics, metric, number_data_point};
use crate::store::SharedStore;

const PROM_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Build an Axum router with the Prometheus scrape endpoint.
pub fn router(store: SharedStore) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(store)
}

#[tracing::instrument(name = "prometheus.scrape", skip_all)]
async fn metrics_handler(State(store): State<SharedStore>) -> impl IntoResponse {
    let store = store.read().await;
    let body = render_prometheus(&store);
    (StatusCode::OK, [("content-type", PROM_CONTENT_TYPE)], body)
}

/// Render all stored OTLP metrics plus motel internal stats in Prometheus
/// text exposition format.
fn render_prometheus(store: &crate::store::Store) -> String {
    let mut out = String::new();
    let mut seen_types: HashSet<String> = HashSet::new();

    for rm in &store.metrics {
        let service_name = extract_service_name(rm);
        for sm in &rm.scope_metrics {
            for metric in &sm.metrics {
                let prom_name = sanitize_metric_name(&metric.name);

                // Emit TYPE and HELP lines once per metric name
                if seen_types.insert(prom_name.clone()) {
                    let type_str = match &metric.data {
                        Some(metric::Data::Gauge(_)) => "gauge",
                        Some(metric::Data::Sum(s)) if s.is_monotonic => "counter",
                        Some(metric::Data::Sum(_)) => "gauge",
                        Some(metric::Data::Histogram(_)) => "histogram",
                        Some(metric::Data::Summary(_)) => "summary",
                        _ => "untyped",
                    };
                    if !metric.description.is_empty() {
                        writeln!(
                            out,
                            "# HELP {prom_name} {}",
                            escape_help(&metric.description)
                        )
                        .ok();
                    }
                    writeln!(out, "# TYPE {prom_name} {type_str}").ok();
                }

                match &metric.data {
                    Some(metric::Data::Gauge(g)) => {
                        for dp in &g.data_points {
                            let labels = format_labels(&service_name, &dp.attributes);
                            let val = extract_number_value(&dp.value);
                            writeln!(out, "{prom_name}{labels} {}", format_value(val)).ok();
                        }
                    }
                    Some(metric::Data::Sum(s)) => {
                        for dp in &s.data_points {
                            let labels = format_labels(&service_name, &dp.attributes);
                            let val = extract_number_value(&dp.value);
                            writeln!(out, "{prom_name}{labels} {}", format_value(val)).ok();
                        }
                    }
                    Some(metric::Data::Histogram(h)) => {
                        for dp in &h.data_points {
                            let base_labels = format_labels(&service_name, &dp.attributes);
                            let mut cumulative_count: u64 = 0;
                            for (i, bound) in dp.explicit_bounds.iter().enumerate() {
                                cumulative_count += dp.bucket_counts.get(i).copied().unwrap_or(0);
                                let le_label =
                                    insert_label(&base_labels, "le", &format_value(*bound));
                                writeln!(out, "{prom_name}_bucket{le_label} {cumulative_count}")
                                    .ok();
                            }
                            // +Inf bucket
                            let inf_label = insert_label(&base_labels, "le", "+Inf");
                            writeln!(out, "{prom_name}_bucket{inf_label} {}", dp.count).ok();
                            writeln!(
                                out,
                                "{prom_name}_sum{base_labels} {}",
                                format_value(dp.sum.unwrap_or(0.0))
                            )
                            .ok();
                            writeln!(out, "{prom_name}_count{base_labels} {}", dp.count).ok();
                        }
                    }
                    Some(metric::Data::Summary(s)) => {
                        for dp in &s.data_points {
                            let base_labels = format_labels(&service_name, &dp.attributes);
                            for qv in &dp.quantile_values {
                                let q_label = insert_label(
                                    &base_labels,
                                    "quantile",
                                    &format_value(qv.quantile),
                                );
                                writeln!(out, "{prom_name}{q_label} {}", format_value(qv.value))
                                    .ok();
                            }
                            writeln!(out, "{prom_name}_sum{base_labels} {}", format_value(dp.sum))
                                .ok();
                            writeln!(out, "{prom_name}_count{base_labels} {}", dp.count).ok();
                        }
                    }
                    _ => {} // ExponentialHistogram and others — skip
                }
            }
        }
    }

    // Internal motel stats
    writeln!(
        out,
        "# HELP motel_traces_total Number of unique trace IDs stored"
    )
    .ok();
    writeln!(out, "# TYPE motel_traces_total gauge").ok();
    writeln!(out, "motel_traces_total {}", store.trace_count()).ok();

    writeln!(out, "# HELP motel_spans_total Number of spans stored").ok();
    writeln!(out, "# TYPE motel_spans_total gauge").ok();
    writeln!(out, "motel_spans_total {}", store.span_count()).ok();

    writeln!(out, "# HELP motel_logs_total Number of log records stored").ok();
    writeln!(out, "# TYPE motel_logs_total gauge").ok();
    writeln!(out, "motel_logs_total {}", store.log_count()).ok();

    writeln!(
        out,
        "# HELP motel_metrics_total Number of metric data points stored"
    )
    .ok();
    writeln!(out, "# TYPE motel_metrics_total gauge").ok();
    writeln!(out, "motel_metrics_total {}", store.metric_count()).ok();

    out
}

/// Sanitize a metric name for Prometheus: replace invalid characters with
/// underscores, strip leading digits.
fn sanitize_metric_name(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    for (i, ch) in name.chars().enumerate() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == ':' {
            result.push(ch);
        } else {
            result.push('_');
        }
        // Strip leading digits by replacing them with underscores
        if i == 0 && ch.is_ascii_digit() {
            result.clear();
            result.push('_');
        }
    }
    if result.is_empty() {
        result.push_str("_unnamed");
    }
    result
}

/// Extract `service.name` from resource attributes.
fn extract_service_name(rm: &ResourceMetrics) -> String {
    rm.resource
        .as_ref()
        .map(|r| {
            for kv in &r.attributes {
                if kv.key == "service.name"
                    && let Some(ref v) = kv.value
                    && let Some(crate::otel::common::v1::any_value::Value::StringValue(ref s)) =
                        v.value
                {
                    return s.clone();
                }
            }
            String::new()
        })
        .unwrap_or_default()
}

/// Format a label set as `{service_name="foo",key="val"}`.
/// Returns an empty string if there are no labels.
fn format_labels(service_name: &str, attrs: &[KeyValue]) -> String {
    let mut parts: Vec<String> = Vec::new();
    if !service_name.is_empty() {
        parts.push(format!(
            "service_name=\"{}\"",
            escape_label_value(service_name)
        ));
    }
    for kv in attrs {
        let key = sanitize_label_name(&kv.key);
        if let Some(ref v) = kv.value {
            let val = extract_string_value(v);
            parts.push(format!("{key}=\"{}\"", escape_label_value(&val)));
        }
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("{{{}}}", parts.join(","))
    }
}

/// Insert an additional label into an existing label string.
/// For example, insert "le" into `{service_name="foo"}` to get
/// `{service_name="foo",le="0.5"}`.
fn insert_label(base_labels: &str, key: &str, value: &str) -> String {
    if base_labels.is_empty() {
        format!("{{{key}=\"{value}\"}}")
    } else {
        // base_labels is like `{a="b",c="d"}` — insert before the closing brace
        format!(
            "{},{key}=\"{value}\"}}",
            &base_labels[..base_labels.len() - 1]
        )
    }
}

/// Sanitize a label name: must match `[a-zA-Z_][a-zA-Z0-9_]*`.
fn sanitize_label_name(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    for (i, ch) in name.chars().enumerate() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            result.push(ch);
        } else {
            result.push('_');
        }
        if i == 0 && ch.is_ascii_digit() {
            result.clear();
            result.push('_');
        }
    }
    if result.is_empty() {
        result.push('_');
    }
    result
}

/// Escape a label value for Prometheus (backslash, double-quote, newline).
fn escape_label_value(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Escape a HELP description line (backslash, newline).
fn escape_help(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\n', "\\n")
}

/// Extract a string representation from an AnyValue.
fn extract_string_value(v: &crate::otel::common::v1::AnyValue) -> String {
    use crate::otel::common::v1::any_value::Value;
    match &v.value {
        Some(Value::StringValue(s)) => s.clone(),
        Some(Value::IntValue(i)) => i.to_string(),
        Some(Value::DoubleValue(d)) => format_value(*d),
        Some(Value::BoolValue(b)) => b.to_string(),
        _ => String::new(),
    }
}

/// Extract a numeric value from a NumberDataPoint value.
fn extract_number_value(v: &Option<number_data_point::Value>) -> f64 {
    match v {
        Some(number_data_point::Value::AsDouble(d)) => *d,
        Some(number_data_point::Value::AsInt(i)) => *i as f64,
        None => 0.0,
    }
}

/// Format a float value for Prometheus output.
/// Integers are rendered without a decimal point (e.g., "42" not "42.0").
fn format_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v.is_sign_positive() {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else if v == v.trunc() && v.abs() < 1e15 {
        // Render as integer if it has no fractional part
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otel::common::v1::{AnyValue, KeyValue, any_value::Value};
    use crate::otel::metrics::v1::{
        Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum, number_data_point,
    };
    use crate::otel::resource::v1::Resource;
    use crate::store::Store;

    fn make_resource(service: &str) -> Option<Resource> {
        Some(Resource {
            attributes: vec![KeyValue {
                key: "service.name".into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(service.into())),
                }),
                ..Default::default()
            }],
            ..Default::default()
        })
    }

    fn make_gauge_metric(name: &str, description: &str, value: f64) -> Metric {
        Metric {
            name: name.to_string(),
            description: description.to_string(),
            unit: "1".to_string(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: 1_000_000_000,
                    value: Some(number_data_point::Value::AsDouble(value)),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        }
    }

    fn make_counter_metric(name: &str, value: f64) -> Metric {
        Metric {
            name: name.to_string(),
            description: String::new(),
            unit: "1".to_string(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: 1_000_000_000,
                    value: Some(number_data_point::Value::AsDouble(value)),
                    ..Default::default()
                }],
                aggregation_temporality: 2, // CUMULATIVE
                is_monotonic: true,
            })),
            ..Default::default()
        }
    }

    fn make_histogram_metric(name: &str) -> Metric {
        Metric {
            name: name.to_string(),
            description: "A histogram".to_string(),
            unit: "ms".to_string(),
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: 1_000_000_000,
                    count: 10,
                    sum: Some(123.5),
                    bucket_counts: vec![2, 3, 5],
                    explicit_bounds: vec![10.0, 50.0],
                    ..Default::default()
                }],
                aggregation_temporality: 2,
            })),
            ..Default::default()
        }
    }

    #[test]
    fn test_sanitize_metric_name() {
        assert_eq!(
            sanitize_metric_name("http.request.duration"),
            "http_request_duration"
        );
        assert_eq!(sanitize_metric_name("my-metric"), "my_metric");
        assert_eq!(
            sanitize_metric_name("0starts_with_digit"),
            "_starts_with_digit"
        );
        assert_eq!(sanitize_metric_name("valid_name"), "valid_name");
        assert_eq!(sanitize_metric_name("with:colon"), "with:colon");
        assert_eq!(sanitize_metric_name(""), "_unnamed");
    }

    #[test]
    fn test_sanitize_label_name() {
        assert_eq!(sanitize_label_name("service.name"), "service_name");
        assert_eq!(sanitize_label_name("http.method"), "http_method");
        assert_eq!(sanitize_label_name("0bad"), "_bad");
    }

    #[test]
    fn test_escape_label_value() {
        assert_eq!(escape_label_value("hello"), "hello");
        assert_eq!(escape_label_value("he\"llo"), "he\\\"llo");
        assert_eq!(escape_label_value("he\\llo"), "he\\\\llo");
        assert_eq!(escape_label_value("he\nllo"), "he\\nllo");
    }

    #[test]
    fn test_format_labels() {
        let labels = format_labels("my-svc", &[]);
        assert_eq!(labels, "{service_name=\"my-svc\"}");

        let labels = format_labels("", &[]);
        assert_eq!(labels, "");

        let attrs = vec![KeyValue {
            key: "http.method".into(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("GET".into())),
            }),
            ..Default::default()
        }];
        let labels = format_labels("svc", &attrs);
        assert_eq!(labels, "{service_name=\"svc\",http_method=\"GET\"}");
    }

    #[test]
    fn test_insert_label() {
        assert_eq!(
            insert_label("{service_name=\"svc\"}", "le", "10"),
            "{service_name=\"svc\",le=\"10\"}"
        );
        assert_eq!(insert_label("", "le", "+Inf"), "{le=\"+Inf\"}");
    }

    #[test]
    fn test_render_gauge() {
        let (mut store, _rx) = Store::new(100, 100, 100);
        store.insert_metrics(vec![ResourceMetrics {
            resource: make_resource("test-svc"),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![make_gauge_metric("cpu.usage", "CPU usage percentage", 42.5)],
                ..Default::default()
            }],
            ..Default::default()
        }]);

        let output = render_prometheus(&store);
        assert!(output.contains("# HELP cpu_usage CPU usage percentage"));
        assert!(output.contains("# TYPE cpu_usage gauge"));
        assert!(output.contains("cpu_usage{service_name=\"test-svc\"} 42.5"));
    }

    #[test]
    fn test_render_counter() {
        let (mut store, _rx) = Store::new(100, 100, 100);
        store.insert_metrics(vec![ResourceMetrics {
            resource: make_resource("test-svc"),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![make_counter_metric("http.requests", 100.0)],
                ..Default::default()
            }],
            ..Default::default()
        }]);

        let output = render_prometheus(&store);
        assert!(output.contains("# TYPE http_requests counter"));
        assert!(output.contains("http_requests{service_name=\"test-svc\"} 100"));
    }

    #[test]
    fn test_render_histogram() {
        let (mut store, _rx) = Store::new(100, 100, 100);
        store.insert_metrics(vec![ResourceMetrics {
            resource: make_resource("test-svc"),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![make_histogram_metric("http.request.duration")],
                ..Default::default()
            }],
            ..Default::default()
        }]);

        let output = render_prometheus(&store);
        assert!(output.contains("# TYPE http_request_duration histogram"));
        assert!(
            output.contains("http_request_duration_bucket{service_name=\"test-svc\",le=\"10\"} 2")
        );
        // Cumulative: 2 + 3 = 5
        assert!(
            output.contains("http_request_duration_bucket{service_name=\"test-svc\",le=\"50\"} 5")
        );
        assert!(
            output
                .contains("http_request_duration_bucket{service_name=\"test-svc\",le=\"+Inf\"} 10")
        );
        assert!(output.contains("http_request_duration_sum{service_name=\"test-svc\"} 123.5"));
        assert!(output.contains("http_request_duration_count{service_name=\"test-svc\"} 10"));
    }

    #[test]
    fn test_render_multiple_services() {
        let (mut store, _rx) = Store::new(100, 100, 100);
        store.insert_metrics(vec![
            ResourceMetrics {
                resource: make_resource("svc-a"),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![make_gauge_metric("cpu.usage", "CPU", 10.0)],
                    ..Default::default()
                }],
                ..Default::default()
            },
            ResourceMetrics {
                resource: make_resource("svc-b"),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![make_gauge_metric("cpu.usage", "CPU", 20.0)],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ]);

        let output = render_prometheus(&store);
        assert!(output.contains("cpu_usage{service_name=\"svc-a\"} 10"));
        assert!(output.contains("cpu_usage{service_name=\"svc-b\"} 20"));
        // TYPE line should appear only once
        assert_eq!(output.matches("# TYPE cpu_usage").count(), 1);
    }

    #[test]
    fn test_render_empty_store() {
        let (store, _rx) = Store::new(100, 100, 100);
        let output = render_prometheus(&store);
        assert!(output.contains("motel_traces_total 0"));
        assert!(output.contains("motel_spans_total 0"));
        assert!(output.contains("motel_logs_total 0"));
        assert!(output.contains("motel_metrics_total 0"));
    }

    #[test]
    fn test_internal_metrics_reflect_store_state() {
        let (mut store, _rx) = Store::new(100, 100, 100);
        store.insert_traces_no_persist(vec![crate::store::tests::make_resource_spans(
            &[1; 16],
            "test-span",
        )]);
        let output = render_prometheus(&store);
        assert!(output.contains("motel_traces_total 1"));
        assert!(output.contains("motel_spans_total 1"));
    }

    #[test]
    fn test_format_value() {
        assert_eq!(format_value(42.0), "42");
        assert_eq!(format_value(42.5), "42.5");
        assert_eq!(format_value(0.0), "0");
        assert_eq!(format_value(f64::NAN), "NaN");
        assert_eq!(format_value(f64::INFINITY), "+Inf");
        assert_eq!(format_value(f64::NEG_INFINITY), "-Inf");
    }
}
