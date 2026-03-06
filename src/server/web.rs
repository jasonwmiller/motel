use axum::{
    Router,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Json, Sse},
    routing::get,
};
use axum::response::sse::{Event, KeepAlive};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::store::{SharedStore, StoreEvent};
use crate::tui::app;

// Embed static assets at compile time
const INDEX_HTML: &str = include_str!("web_assets/index.html");
const APP_JS: &str = include_str!("web_assets/app.js");
const STYLE_CSS: &str = include_str!("web_assets/style.css");

#[derive(Clone)]
pub struct WebState {
    pub store: SharedStore,
    pub event_tx: broadcast::Sender<StoreEvent>,
    pub session_ctx: Arc<SessionContext>,
}

pub fn router(state: WebState) -> Router {
    Router::new()
        // Static assets
        .route("/", get(index_html))
        .route("/app.js", get(app_js))
        .route("/style.css", get(style_css))
        // REST API
        .route("/api/status", get(api_status))
        .route("/api/traces", get(api_traces))
        .route("/api/logs", get(api_logs))
        .route("/api/metrics", get(api_metrics))
        .route("/api/sql", get(api_sql))
        // Real-time updates
        .route("/api/events", get(sse_events))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Static asset handlers
// ---------------------------------------------------------------------------

async fn index_html() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], INDEX_HTML)
}

async fn app_js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/javascript; charset=utf-8")],
        APP_JS,
    )
}

async fn style_css() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], STYLE_CSS)
}

// ---------------------------------------------------------------------------
// JSON response types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct StatusResponse {
    trace_count: usize,
    span_count: usize,
    log_count: usize,
    metric_count: usize,
}

#[derive(Serialize)]
struct TraceGroupJson {
    trace_id: String,
    service_name: String,
    root_span_name: String,
    span_count: usize,
    duration: String,
    start_time: String,
    spans: Vec<SpanNodeJson>,
}

#[derive(Serialize)]
struct SpanNodeJson {
    span_id: String,
    parent_span_id: String,
    service_name: String,
    span_name: String,
    duration: String,
    duration_ns: u64,
    start_ns: u64,
    status_code: i32,
    depth: usize,
}

#[derive(Serialize)]
struct LogRowJson {
    time: String,
    service_name: String,
    severity_text: String,
    body: String,
}

#[derive(Serialize)]
struct MetricJson {
    metric_name: String,
    metric_type: String,
    service_name: String,
    unit: String,
    display_value: String,
    data_point_count: usize,
}

#[derive(Deserialize)]
struct SqlQuery {
    q: String,
}

#[derive(Serialize)]
struct SqlResponse {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn format_time_nano(ns: u64) -> String {
    if ns == 0 {
        return "-".to_string();
    }
    let secs = (ns / 1_000_000_000) as i64;
    let nanos = (ns % 1_000_000_000) as u32;
    if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
        dt.format("%H:%M:%S%.3f").to_string()
    } else {
        "-".to_string()
    }
}

fn format_duration(ns: u64) -> String {
    if ns >= 1_000_000_000 {
        format!("{:.3}s", ns as f64 / 1e9)
    } else if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.1}us", ns as f64 / 1e3)
    } else {
        format!("{ns}ns")
    }
}

// ---------------------------------------------------------------------------
// API endpoint handlers
// ---------------------------------------------------------------------------

async fn api_status(State(state): State<WebState>) -> Json<StatusResponse> {
    let store = state.store.read().await;
    Json(StatusResponse {
        trace_count: store.trace_count(),
        span_count: store.span_count(),
        log_count: store.log_count(),
        metric_count: store.metric_count(),
    })
}

async fn api_traces(State(state): State<WebState>) -> Json<Vec<TraceGroupJson>> {
    let store = state.store.read().await;
    let all_spans = app::flatten_traces(&store.traces);
    let groups = app::group_traces(all_spans);
    drop(store);

    let json_groups: Vec<TraceGroupJson> = groups
        .into_iter()
        .map(|g| {
            let tree_nodes = app::build_span_tree(&g.spans);
            let spans: Vec<SpanNodeJson> = tree_nodes
                .into_iter()
                .map(|node| SpanNodeJson {
                    span_id: hex::encode(&node.span.span_id),
                    parent_span_id: hex::encode(&node.span.parent_span_id),
                    service_name: node.span.service_name.clone(),
                    span_name: node.span.span_name.clone(),
                    duration: format_duration(node.span.duration_ns),
                    duration_ns: node.span.duration_ns,
                    start_ns: node.span.time_nano,
                    status_code: node.span.status_code,
                    depth: node.depth,
                })
                .collect();
            TraceGroupJson {
                trace_id: hex::encode(&g.trace_id),
                service_name: g.service_name,
                root_span_name: g.root_span_name,
                span_count: g.span_count,
                duration: format_duration(g.duration_ns),
                start_time: format_time_nano(g.start_time_nano),
                spans,
            }
        })
        .collect();

    Json(json_groups)
}

async fn api_logs(State(state): State<WebState>) -> Json<Vec<LogRowJson>> {
    let store = state.store.read().await;
    let log_rows = app::flatten_logs(&store.logs);
    drop(store);

    let json_logs: Vec<LogRowJson> = log_rows
        .into_iter()
        .map(|l| LogRowJson {
            time: format_time_nano(l.time_nano),
            service_name: l.service_name,
            severity_text: l.severity_text,
            body: l.body,
        })
        .collect();

    Json(json_logs)
}

async fn api_metrics(State(state): State<WebState>) -> Json<Vec<MetricJson>> {
    let store = state.store.read().await;
    let aggregated = app::aggregate_metrics(&store.metrics);
    drop(store);

    let json_metrics: Vec<MetricJson> = aggregated
        .into_iter()
        .map(|m| {
            let dp_count = m.data_points.len();
            MetricJson {
                metric_name: m.metric_name.clone(),
                metric_type: m.metric_type.clone(),
                service_name: m.service_name.clone(),
                unit: m.unit.clone(),
                display_value: m.display_value(),
                data_point_count: dp_count,
            }
        })
        .collect();

    Json(json_metrics)
}

async fn api_sql(
    State(state): State<WebState>,
    Query(params): Query<SqlQuery>,
) -> Result<Json<SqlResponse>, (StatusCode, String)> {
    let (columns, rows) = crate::query::sql::execute_with_columns(&state.session_ctx, &params.q)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("SQL error: {e}")))?;

    Ok(Json(SqlResponse {
        columns: columns.into_iter().map(|c| c.name).collect(),
        rows: rows.into_iter().map(|r| r.values).collect(),
    }))
}

// ---------------------------------------------------------------------------
// SSE event stream
// ---------------------------------------------------------------------------

async fn sse_events(
    State(state): State<WebState>,
) -> Sse<impl futures::stream::Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.event_tx.subscribe();

    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let event_type = match &event {
                        StoreEvent::TracesInserted(_) => "traces",
                        StoreEvent::LogsInserted(_) => "logs",
                        StoreEvent::MetricsInserted(_) => "metrics",
                        StoreEvent::TracesCleared => "traces_cleared",
                        StoreEvent::LogsCleared => "logs_cleared",
                        StoreEvent::MetricsCleared => "metrics_cleared",
                    };
                    yield Ok(Event::default().event(event_type).data("{}"));
                }
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn make_test_state() -> WebState {
        let (store, _rx) = crate::store::Store::new_shared(100, 100, 100);
        let event_tx = store.read().await.event_tx.clone();
        let session_ctx =
            crate::query::datafusion_ctx::create_context(store.clone())
                .await
                .unwrap();
        WebState {
            store,
            event_tx,
            session_ctx: Arc::new(session_ctx),
        }
    }

    #[tokio::test]
    async fn test_index_html() {
        let state = make_test_state().await;
        let app = router(state);
        let resp = app
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap();
        assert!(ct.contains("text/html"));
    }

    #[tokio::test]
    async fn test_app_js() {
        let state = make_test_state().await;
        let app = router(state);
        let resp = app
            .oneshot(Request::get("/app.js").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap();
        assert!(ct.contains("javascript"));
    }

    #[tokio::test]
    async fn test_style_css() {
        let state = make_test_state().await;
        let app = router(state);
        let resp = app
            .oneshot(Request::get("/style.css").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap();
        assert!(ct.contains("text/css"));
    }

    #[tokio::test]
    async fn test_api_status_empty() {
        let state = make_test_state().await;
        let app = router(state);
        let resp = app
            .oneshot(Request::get("/api/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let data: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(data["trace_count"], 0);
        assert_eq!(data["span_count"], 0);
        assert_eq!(data["log_count"], 0);
        assert_eq!(data["metric_count"], 0);
    }

    #[tokio::test]
    async fn test_api_traces_empty() {
        let state = make_test_state().await;
        let app = router(state);
        let resp = app
            .oneshot(Request::get("/api/traces").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let data: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(data.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_api_logs_empty() {
        let state = make_test_state().await;
        let app = router(state);
        let resp = app
            .oneshot(Request::get("/api/logs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let data: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(data.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_api_metrics_empty() {
        let state = make_test_state().await;
        let app = router(state);
        let resp = app
            .oneshot(Request::get("/api/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let data: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(data.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_api_sql_select_1() {
        let state = make_test_state().await;
        let app = router(state);
        let resp = app
            .oneshot(
                Request::get("/api/sql?q=SELECT%201%20as%20x")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let data: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(data["columns"][0], "x");
        assert!(!data["rows"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_api_sql_bad_query() {
        let state = make_test_state().await;
        let app = router(state);
        let resp = app
            .oneshot(
                Request::get("/api/sql?q=INVALID%20SQL%20GIBBERISH")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_api_status_with_data() {
        let state = make_test_state().await;

        // Insert test data
        {
            let mut store = state.store.write().await;
            let rs = crate::store::tests::make_resource_spans(&[1u8; 16], "test-span");
            store.insert_traces(vec![rs]);
        }

        let app = router(state);
        let resp = app
            .oneshot(Request::get("/api/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let data: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(data["trace_count"], 1);
        assert_eq!(data["span_count"], 1);
    }

    #[tokio::test]
    async fn test_api_traces_with_data() {
        let state = make_test_state().await;

        // Insert test data
        {
            let mut store = state.store.write().await;
            let rs = crate::store::tests::make_resource_spans(&[1u8; 16], "test-span");
            store.insert_traces(vec![rs]);
        }

        let app = router(state);
        let resp = app
            .oneshot(Request::get("/api/traces").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let data: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let traces = data.as_array().unwrap();
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0]["root_span_name"], "test-span");
        assert_eq!(traces[0]["span_count"], 1);
    }
}
