use std::collections::HashMap;

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};

use crate::otel::common::v1::any_value::Value;
use crate::store::SharedStore;

/// Shared state for query HTTP handlers.
#[derive(Clone)]
pub struct QueryHttpState {
    pub store: SharedStore,
    pub session_ctx: SessionContext,
}

// ---------------------------------------------------------------------------
// Query parameter structs
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct TraceQueryParams {
    pub service: Option<String>,
    pub span_name: Option<String>,
    pub trace_id: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Deserialize)]
pub struct LogQueryParams {
    pub service: Option<String>,
    pub severity: Option<String>,
    pub body: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Deserialize)]
pub struct MetricQueryParams {
    pub service: Option<String>,
    pub name: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Deserialize)]
pub struct SqlRequest {
    pub query: String,
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct TraceSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub name: String,
    pub service_name: String,
    pub kind: i32,
    pub start_time_unix_nano: u64,
    pub end_time_unix_nano: u64,
    pub duration_ns: u64,
    pub status_code: i32,
    pub status_message: String,
    pub attributes: HashMap<String, serde_json::Value>,
}

#[derive(Serialize)]
pub struct LogRecord {
    pub time_unix_nano: u64,
    pub service_name: String,
    pub severity_text: String,
    pub severity_number: i32,
    pub body: String,
    pub trace_id: String,
    pub span_id: String,
    pub attributes: HashMap<String, serde_json::Value>,
}

#[derive(Serialize)]
pub struct MetricRecord {
    pub name: String,
    pub service_name: String,
    pub description: String,
    pub unit: String,
    pub data_type: String,
}

#[derive(Serialize)]
pub struct SqlResponse {
    pub columns: Vec<SqlColumn>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Serialize)]
pub struct SqlColumn {
    pub name: String,
    pub data_type: String,
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub trace_count: i64,
    pub span_count: i64,
    pub log_count: i64,
    pub metric_count: i64,
}

#[derive(Serialize)]
pub struct ClearResponse {
    pub cleared_count: i64,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn attributes_to_map(
    attrs: &[crate::otel::common::v1::KeyValue],
) -> HashMap<String, serde_json::Value> {
    let mut map = HashMap::new();
    for kv in attrs {
        let val = match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
            Some(Value::StringValue(s)) => serde_json::Value::String(s.clone()),
            Some(Value::IntValue(i)) => serde_json::json!(i),
            Some(Value::DoubleValue(d)) => serde_json::json!(d),
            Some(Value::BoolValue(b)) => serde_json::json!(b),
            _ => serde_json::Value::Null,
        };
        map.insert(kv.key.clone(), val);
    }
    map
}

fn extract_service_name(resource: Option<&crate::otel::resource::v1::Resource>) -> String {
    resource
        .iter()
        .flat_map(|r| r.attributes.iter())
        .find(|kv| kv.key == "service.name")
        .and_then(|kv| kv.value.as_ref())
        .and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

fn matches_service_name(
    resource: Option<&crate::otel::resource::v1::Resource>,
    service: &str,
) -> bool {
    resource.is_some_and(|r| {
        r.attributes.iter().any(|kv| {
            kv.key == "service.name"
                && kv.value.as_ref().is_some_and(
                    |v| matches!(&v.value, Some(Value::StringValue(s)) if s == service),
                )
        })
    })
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

#[tracing::instrument(name = "query.http.traces", skip_all)]
async fn query_traces(
    State(state): State<QueryHttpState>,
    Query(params): Query<TraceQueryParams>,
) -> Result<Json<Vec<TraceSpan>>, (StatusCode, String)> {
    let store = state.store.read().await;
    let mut resource_spans: Vec<_> = store.traces.iter().cloned().collect();
    drop(store);

    // Filter by service_name
    if let Some(ref service) = params.service
        && !service.is_empty()
    {
        resource_spans.retain(|rs| matches_service_name(rs.resource.as_ref(), service));
    }

    // Filter by span_name
    if let Some(ref span_name) = params.span_name
        && !span_name.is_empty()
    {
        for rs in &mut resource_spans {
            for ss in &mut rs.scope_spans {
                ss.spans.retain(|s| &s.name == span_name);
            }
            rs.scope_spans.retain(|ss| !ss.spans.is_empty());
        }
        resource_spans.retain(|rs| !rs.scope_spans.is_empty());
    }

    // Filter by trace_id
    if let Some(ref trace_id) = params.trace_id
        && !trace_id.is_empty()
    {
        let trace_id_bytes = hex::decode(trace_id).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid trace_id hex: {e}"),
            )
        })?;
        for rs in &mut resource_spans {
            for ss in &mut rs.scope_spans {
                ss.spans.retain(|s| s.trace_id == trace_id_bytes);
            }
            rs.scope_spans.retain(|ss| !ss.spans.is_empty());
        }
        resource_spans.retain(|rs| !rs.scope_spans.is_empty());
    }

    // Apply limit
    if let Some(limit) = params.limit
        && limit > 0
    {
        resource_spans.truncate(limit as usize);
    }

    // Convert to JSON response
    let mut spans = Vec::new();
    for rs in &resource_spans {
        let service_name = extract_service_name(rs.resource.as_ref());
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                spans.push(TraceSpan {
                    trace_id: hex::encode(&span.trace_id),
                    span_id: hex::encode(&span.span_id),
                    parent_span_id: hex::encode(&span.parent_span_id),
                    name: span.name.clone(),
                    service_name: service_name.clone(),
                    kind: span.kind,
                    start_time_unix_nano: span.start_time_unix_nano,
                    end_time_unix_nano: span.end_time_unix_nano,
                    duration_ns: span
                        .end_time_unix_nano
                        .saturating_sub(span.start_time_unix_nano),
                    status_code: span.status.as_ref().map_or(0, |s| s.code),
                    status_message: span
                        .status
                        .as_ref()
                        .map_or_else(String::new, |s| s.message.clone()),
                    attributes: attributes_to_map(&span.attributes),
                });
            }
        }
    }
    Ok(Json(spans))
}

#[tracing::instrument(name = "query.http.logs", skip_all)]
async fn query_logs(
    State(state): State<QueryHttpState>,
    Query(params): Query<LogQueryParams>,
) -> Result<Json<Vec<LogRecord>>, (StatusCode, String)> {
    let store = state.store.read().await;
    let mut resource_logs: Vec<_> = store.logs.iter().cloned().collect();
    drop(store);

    // Filter by service_name
    if let Some(ref service) = params.service
        && !service.is_empty()
    {
        resource_logs.retain(|rl| matches_service_name(rl.resource.as_ref(), service));
    }

    // Filter by severity
    if let Some(ref severity) = params.severity
        && !severity.is_empty()
    {
        let severity_upper = severity.to_uppercase();
        for rl in &mut resource_logs {
            for sl in &mut rl.scope_logs {
                sl.log_records.retain(|lr| {
                    format!("{:?}", lr.severity_number())
                        .to_uppercase()
                        .contains(&severity_upper)
                });
            }
            rl.scope_logs.retain(|sl| !sl.log_records.is_empty());
        }
        resource_logs.retain(|rl| !rl.scope_logs.is_empty());
    }

    // Filter by body_contains
    if let Some(ref body) = params.body
        && !body.is_empty()
    {
        for rl in &mut resource_logs {
            for sl in &mut rl.scope_logs {
                sl.log_records.retain(|lr| {
                    lr.body
                        .as_ref()
                        .is_some_and(|b| format!("{:?}", b).contains(body.as_str()))
                });
            }
            rl.scope_logs.retain(|sl| !sl.log_records.is_empty());
        }
        resource_logs.retain(|rl| !rl.scope_logs.is_empty());
    }

    // Apply limit
    if let Some(limit) = params.limit
        && limit > 0
    {
        resource_logs.truncate(limit as usize);
    }

    // Convert to JSON response
    let mut records = Vec::new();
    for rl in &resource_logs {
        let service_name = extract_service_name(rl.resource.as_ref());
        for sl in &rl.scope_logs {
            for lr in &sl.log_records {
                let body_text = lr
                    .body
                    .as_ref()
                    .and_then(|b| match &b.value {
                        Some(Value::StringValue(s)) => Some(s.clone()),
                        Some(other) => Some(format!("{other:?}")),
                        None => None,
                    })
                    .unwrap_or_default();

                records.push(LogRecord {
                    time_unix_nano: lr.time_unix_nano,
                    service_name: service_name.clone(),
                    severity_text: lr.severity_text.clone(),
                    severity_number: lr.severity_number,
                    body: body_text,
                    trace_id: hex::encode(&lr.trace_id),
                    span_id: hex::encode(&lr.span_id),
                    attributes: attributes_to_map(&lr.attributes),
                });
            }
        }
    }
    Ok(Json(records))
}

#[tracing::instrument(name = "query.http.metrics", skip_all)]
async fn query_metrics(
    State(state): State<QueryHttpState>,
    Query(params): Query<MetricQueryParams>,
) -> Result<Json<Vec<MetricRecord>>, (StatusCode, String)> {
    let store = state.store.read().await;
    let mut resource_metrics: Vec<_> = store.metrics.iter().cloned().collect();
    drop(store);

    // Filter by service_name
    if let Some(ref service) = params.service
        && !service.is_empty()
    {
        resource_metrics.retain(|rm| matches_service_name(rm.resource.as_ref(), service));
    }

    // Filter by metric_name
    if let Some(ref name) = params.name
        && !name.is_empty()
    {
        for rm in &mut resource_metrics {
            for sm in &mut rm.scope_metrics {
                sm.metrics.retain(|m| &m.name == name);
            }
            rm.scope_metrics.retain(|sm| !sm.metrics.is_empty());
        }
        resource_metrics.retain(|rm| !rm.scope_metrics.is_empty());
    }

    // Apply limit
    if let Some(limit) = params.limit
        && limit > 0
    {
        resource_metrics.truncate(limit as usize);
    }

    // Convert to JSON response
    let mut records = Vec::new();
    for rm in &resource_metrics {
        let service_name = extract_service_name(rm.resource.as_ref());
        for sm in &rm.scope_metrics {
            for m in &sm.metrics {
                use crate::otel::metrics::v1::metric::Data;
                let data_type = match &m.data {
                    Some(Data::Gauge(_)) => "gauge",
                    Some(Data::Sum(_)) => "sum",
                    Some(Data::Histogram(_)) => "histogram",
                    Some(Data::ExponentialHistogram(_)) => "exponential_histogram",
                    Some(Data::Summary(_)) => "summary",
                    None => "unknown",
                };
                records.push(MetricRecord {
                    name: m.name.clone(),
                    service_name: service_name.clone(),
                    description: m.description.clone(),
                    unit: m.unit.clone(),
                    data_type: data_type.to_string(),
                });
            }
        }
    }
    Ok(Json(records))
}

#[tracing::instrument(name = "query.http.sql", skip_all)]
async fn sql_query(
    State(state): State<QueryHttpState>,
    Json(req): Json<SqlRequest>,
) -> Result<Json<SqlResponse>, (StatusCode, String)> {
    let (columns, rows) = crate::query::sql::execute_with_columns(&state.session_ctx, &req.query)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("SQL error: {e}")))?;

    Ok(Json(SqlResponse {
        columns: columns
            .into_iter()
            .map(|c| SqlColumn {
                name: c.name,
                data_type: c.data_type,
            })
            .collect(),
        rows: rows.into_iter().map(|r| r.values).collect(),
    }))
}

#[tracing::instrument(name = "query.http.status", skip_all)]
async fn status(State(state): State<QueryHttpState>) -> Json<StatusResponse> {
    let store = state.store.read().await;
    Json(StatusResponse {
        trace_count: store.trace_count() as i64,
        span_count: store.span_count() as i64,
        log_count: store.log_count() as i64,
        metric_count: store.metric_count() as i64,
    })
}

#[tracing::instrument(name = "query.http.clear_traces", skip_all)]
async fn clear_traces(State(state): State<QueryHttpState>) -> Json<ClearResponse> {
    let count = state.store.write().await.clear_traces();
    Json(ClearResponse {
        cleared_count: count as i64,
    })
}

#[tracing::instrument(name = "query.http.clear_logs", skip_all)]
async fn clear_logs(State(state): State<QueryHttpState>) -> Json<ClearResponse> {
    let count = state.store.write().await.clear_logs();
    Json(ClearResponse {
        cleared_count: count as i64,
    })
}

#[tracing::instrument(name = "query.http.clear_metrics", skip_all)]
async fn clear_metrics(State(state): State<QueryHttpState>) -> Json<ClearResponse> {
    let count = state.store.write().await.clear_metrics();
    Json(ClearResponse {
        cleared_count: count as i64,
    })
}

#[tracing::instrument(name = "query.http.clear_all", skip_all)]
async fn clear_all(State(state): State<QueryHttpState>) -> Json<ClearResponse> {
    let mut store = state.store.write().await;
    let count = store.clear_traces() + store.clear_logs() + store.clear_metrics();
    Json(ClearResponse {
        cleared_count: count as i64,
    })
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router(state: QueryHttpState) -> Router {
    Router::new()
        .route("/api/traces", get(query_traces))
        .route("/api/logs", get(query_logs))
        .route("/api/metrics", get(query_metrics))
        .route("/api/sql", post(sql_query))
        .route("/api/status", get(status))
        .route("/api/clear/traces", post(clear_traces))
        .route("/api/clear/logs", post(clear_logs))
        .route("/api/clear/metrics", post(clear_metrics))
        .route("/api/clear/all", post(clear_all))
        .with_state(state)
}
