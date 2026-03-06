use std::sync::Arc;

use datafusion::arrow::array::{
    Float64Builder, Int32Builder, Int64Builder, RecordBatch, StringBuilder,
};

use crate::otel::common::v1::{AnyValue, KeyValue, any_value};
use crate::otel::logs::v1::ResourceLogs;
use crate::otel::metrics::v1::{ResourceMetrics, metric, number_data_point};
use crate::otel::trace::v1::ResourceSpans;

use super::arrow_schema;

/// Serialize a list of KeyValue attributes to a JSON object string.
fn kvs_to_json(kvs: &[KeyValue]) -> String {
    let map: serde_json::Map<String, serde_json::Value> = kvs
        .iter()
        .map(|kv| (kv.key.clone(), any_value_to_json(kv.value.as_ref())))
        .collect();
    serde_json::Value::Object(map).to_string()
}

fn any_value_to_json(v: Option<&AnyValue>) -> serde_json::Value {
    match v.and_then(|av| av.value.as_ref()) {
        Some(any_value::Value::StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(any_value::Value::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(any_value::Value::IntValue(i)) => serde_json::json!(*i),
        Some(any_value::Value::DoubleValue(d)) => serde_json::json!(*d),
        Some(any_value::Value::ArrayValue(arr)) => {
            let vals: Vec<serde_json::Value> = arr
                .values
                .iter()
                .map(|v| any_value_to_json(Some(v)))
                .collect();
            serde_json::Value::Array(vals)
        }
        Some(any_value::Value::KvlistValue(kvl)) => {
            let map: serde_json::Map<String, serde_json::Value> = kvl
                .values
                .iter()
                .map(|kv| (kv.key.clone(), any_value_to_json(kv.value.as_ref())))
                .collect();
            serde_json::Value::Object(map)
        }
        Some(any_value::Value::BytesValue(b)) => serde_json::Value::String(hex::encode(b)),
        _ => serde_json::Value::Null,
    }
}

/// Extract the service.name from resource attributes.
fn extract_service_name(resource: Option<&crate::otel::resource::v1::Resource>) -> String {
    resource
        .map(|r| {
            r.attributes
                .iter()
                .find(|kv| kv.key == "service.name")
                .and_then(|kv| kv.value.as_ref())
                .and_then(|v| match &v.value {
                    Some(any_value::Value::StringValue(s)) => Some(s.clone()),
                    _ => None,
                })
                .unwrap_or_default()
        })
        .unwrap_or_default()
}

/// Serialize resource attributes to JSON string.
fn resource_to_json(resource: Option<&crate::otel::resource::v1::Resource>) -> String {
    match resource {
        Some(r) => kvs_to_json(&r.attributes),
        None => "{}".to_string(),
    }
}

/// Convert ResourceSpans to an Arrow RecordBatch.
pub fn resource_spans_to_batch(data: &[ResourceSpans]) -> Result<RecordBatch, String> {
    let schema = arrow_schema::traces_schema();

    let mut service_name = StringBuilder::new();
    let mut span_name = StringBuilder::new();
    let mut trace_id = StringBuilder::new();
    let mut span_id = StringBuilder::new();
    let mut parent_span_id = StringBuilder::new();
    let mut kind = Int32Builder::new();
    let mut start_time = Int64Builder::new();
    let mut end_time = Int64Builder::new();
    let mut duration_ns = Int64Builder::new();
    let mut status_code = Int32Builder::new();
    let mut status_message = StringBuilder::new();
    let mut attributes = StringBuilder::new();
    let mut resource = StringBuilder::new();

    for rs in data {
        let svc = extract_service_name(rs.resource.as_ref());
        let res_json = resource_to_json(rs.resource.as_ref());

        for scope_spans in &rs.scope_spans {
            for span in &scope_spans.spans {
                service_name.append_value(&svc);
                span_name.append_value(&span.name);
                trace_id.append_value(hex::encode(&span.trace_id));
                span_id.append_value(hex::encode(&span.span_id));
                parent_span_id.append_value(hex::encode(&span.parent_span_id));
                kind.append_value(span.kind);
                start_time
                    .append_value(i64::try_from(span.start_time_unix_nano).unwrap_or(i64::MAX));
                end_time.append_value(i64::try_from(span.end_time_unix_nano).unwrap_or(i64::MAX));
                let dur = span
                    .end_time_unix_nano
                    .saturating_sub(span.start_time_unix_nano);
                duration_ns.append_value(i64::try_from(dur).unwrap_or(i64::MAX));

                let (sc, sm) = span
                    .status
                    .as_ref()
                    .map(|s| (s.code, s.message.clone()))
                    .unwrap_or((0, String::new()));
                status_code.append_value(sc);
                status_message.append_value(&sm);
                attributes.append_value(kvs_to_json(&span.attributes));
                resource.append_value(&res_json);
            }
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(service_name.finish()),
            Arc::new(span_name.finish()),
            Arc::new(trace_id.finish()),
            Arc::new(span_id.finish()),
            Arc::new(parent_span_id.finish()),
            Arc::new(kind.finish()),
            Arc::new(start_time.finish()),
            Arc::new(end_time.finish()),
            Arc::new(duration_ns.finish()),
            Arc::new(status_code.finish()),
            Arc::new(status_message.finish()),
            Arc::new(attributes.finish()),
            Arc::new(resource.finish()),
        ],
    )
    .map_err(|e| e.to_string())
}

/// Convert ResourceLogs to an Arrow RecordBatch.
pub fn resource_logs_to_batch(data: &[ResourceLogs]) -> Result<RecordBatch, String> {
    let schema = arrow_schema::logs_schema();

    let mut service_name = StringBuilder::new();
    let mut timestamp = Int64Builder::new();
    let mut severity_number = Int32Builder::new();
    let mut severity_text = StringBuilder::new();
    let mut body = StringBuilder::new();
    let mut trace_id = StringBuilder::new();
    let mut span_id = StringBuilder::new();
    let mut attributes = StringBuilder::new();
    let mut resource = StringBuilder::new();

    for rl in data {
        let svc = extract_service_name(rl.resource.as_ref());
        let res_json = resource_to_json(rl.resource.as_ref());

        for scope_logs in &rl.scope_logs {
            for lr in &scope_logs.log_records {
                service_name.append_value(&svc);
                timestamp.append_value(i64::try_from(lr.time_unix_nano).unwrap_or(i64::MAX));
                severity_number.append_value(lr.severity_number);
                severity_text.append_value(&lr.severity_text);

                let body_str = lr
                    .body
                    .as_ref()
                    .map(|v| match &v.value {
                        Some(any_value::Value::StringValue(s)) => s.clone(),
                        Some(other) => format!("{:?}", other),
                        None => String::new(),
                    })
                    .unwrap_or_default();
                body.append_value(&body_str);

                trace_id.append_value(hex::encode(&lr.trace_id));
                span_id.append_value(hex::encode(&lr.span_id));
                attributes.append_value(kvs_to_json(&lr.attributes));
                resource.append_value(&res_json);
            }
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(service_name.finish()),
            Arc::new(timestamp.finish()),
            Arc::new(severity_number.finish()),
            Arc::new(severity_text.finish()),
            Arc::new(body.finish()),
            Arc::new(trace_id.finish()),
            Arc::new(span_id.finish()),
            Arc::new(attributes.finish()),
            Arc::new(resource.finish()),
        ],
    )
    .map_err(|e| e.to_string())
}

/// Convert ResourceMetrics to an Arrow RecordBatch.
pub fn resource_metrics_to_batch(data: &[ResourceMetrics]) -> Result<RecordBatch, String> {
    let schema = arrow_schema::metrics_schema();

    let mut service_name = StringBuilder::new();
    let mut metric_name = StringBuilder::new();
    let mut metric_type = StringBuilder::new();
    let mut timestamp = Int64Builder::new();
    let mut value = Float64Builder::new();
    let mut unit = StringBuilder::new();
    let mut attributes = StringBuilder::new();
    let mut resource = StringBuilder::new();
    let mut description = StringBuilder::new();

    for rm in data {
        let svc = extract_service_name(rm.resource.as_ref());
        let res_json = resource_to_json(rm.resource.as_ref());

        for scope_metrics in &rm.scope_metrics {
            for m in &scope_metrics.metrics {
                // Extract data points based on metric type
                match &m.data {
                    Some(metric::Data::Gauge(gauge)) => {
                        for dp in &gauge.data_points {
                            service_name.append_value(&svc);
                            metric_name.append_value(&m.name);
                            metric_type.append_value("gauge");
                            timestamp
                                .append_value(i64::try_from(dp.time_unix_nano).unwrap_or(i64::MAX));
                            value.append_value(extract_number_value(&dp.value));
                            unit.append_value(&m.unit);
                            attributes.append_value(kvs_to_json(&dp.attributes));
                            resource.append_value(&res_json);
                            description.append_value(&m.description);
                        }
                    }
                    Some(metric::Data::Sum(sum)) => {
                        for dp in &sum.data_points {
                            service_name.append_value(&svc);
                            metric_name.append_value(&m.name);
                            metric_type.append_value("sum");
                            timestamp
                                .append_value(i64::try_from(dp.time_unix_nano).unwrap_or(i64::MAX));
                            value.append_value(extract_number_value(&dp.value));
                            unit.append_value(&m.unit);
                            attributes.append_value(kvs_to_json(&dp.attributes));
                            resource.append_value(&res_json);
                            description.append_value(&m.description);
                        }
                    }
                    Some(metric::Data::Histogram(hist)) => {
                        for dp in &hist.data_points {
                            service_name.append_value(&svc);
                            metric_name.append_value(&m.name);
                            metric_type.append_value("histogram");
                            timestamp
                                .append_value(i64::try_from(dp.time_unix_nano).unwrap_or(i64::MAX));
                            value.append_value(dp.sum.unwrap_or(dp.count as f64));
                            unit.append_value(&m.unit);
                            attributes.append_value(kvs_to_json(&dp.attributes));
                            resource.append_value(&res_json);
                            description.append_value(&m.description);
                        }
                    }
                    Some(metric::Data::ExponentialHistogram(hist)) => {
                        for dp in &hist.data_points {
                            service_name.append_value(&svc);
                            metric_name.append_value(&m.name);
                            metric_type.append_value("exponential_histogram");
                            timestamp
                                .append_value(i64::try_from(dp.time_unix_nano).unwrap_or(i64::MAX));
                            value.append_value(dp.sum.unwrap_or(dp.count as f64));
                            unit.append_value(&m.unit);
                            attributes.append_value(kvs_to_json(&dp.attributes));
                            resource.append_value(&res_json);
                            description.append_value(&m.description);
                        }
                    }
                    Some(metric::Data::Summary(summary)) => {
                        for dp in &summary.data_points {
                            service_name.append_value(&svc);
                            metric_name.append_value(&m.name);
                            metric_type.append_value("summary");
                            timestamp
                                .append_value(i64::try_from(dp.time_unix_nano).unwrap_or(i64::MAX));
                            value.append_value(dp.sum);
                            unit.append_value(&m.unit);
                            attributes.append_value(kvs_to_json(&dp.attributes));
                            resource.append_value(&res_json);
                            description.append_value(&m.description);
                        }
                    }
                    None => {
                        // Metric with no data -- emit one row with null value
                        service_name.append_value(&svc);
                        metric_name.append_value(&m.name);
                        metric_type.append_value("unknown");
                        timestamp.append_value(0);
                        value.append_null();
                        unit.append_value(&m.unit);
                        attributes.append_value("{}");
                        resource.append_value(&res_json);
                        description.append_value(&m.description);
                    }
                }
            }
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(service_name.finish()),
            Arc::new(metric_name.finish()),
            Arc::new(metric_type.finish()),
            Arc::new(timestamp.finish()),
            Arc::new(value.finish()),
            Arc::new(unit.finish()),
            Arc::new(attributes.finish()),
            Arc::new(resource.finish()),
            Arc::new(description.finish()),
        ],
    )
    .map_err(|e| e.to_string())
}

fn extract_number_value(v: &Option<number_data_point::Value>) -> f64 {
    match v {
        Some(number_data_point::Value::AsDouble(d)) => *d,
        Some(number_data_point::Value::AsInt(i)) => *i as f64,
        None => 0.0,
    }
}
