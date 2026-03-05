use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};

/// Arrow schema for the "traces" table (13 columns).
pub fn traces_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, true),
        Field::new("span_name", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("kind", DataType::Int32, true),
        Field::new("start_time_unix_nano", DataType::Int64, true),
        Field::new("end_time_unix_nano", DataType::Int64, true),
        Field::new("duration_ns", DataType::Int64, true),
        Field::new("status_code", DataType::Int32, true),
        Field::new("status_message", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("resource", DataType::Utf8, true),
    ]))
}

/// Arrow schema for the "logs" table (9 columns).
pub fn logs_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, true),
        Field::new("timestamp_unix_nano", DataType::Int64, true),
        Field::new("severity_number", DataType::Int32, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("resource", DataType::Utf8, true),
    ]))
}

/// Arrow schema for the "metrics" table (9 columns).
pub fn metrics_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, true),
        Field::new("metric_name", DataType::Utf8, true),
        Field::new("metric_type", DataType::Utf8, true),
        Field::new("timestamp_unix_nano", DataType::Int64, true),
        Field::new("value", DataType::Float64, true),
        Field::new("unit", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("resource", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
    ]))
}
