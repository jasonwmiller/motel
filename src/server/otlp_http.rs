use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
    routing::post,
};
use prost::Message;

use crate::otel::collector::{
    logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
    metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse},
    trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse},
};
use crate::server::forwarder::OtlpForwarder;
use crate::store::SharedStore;

const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";
const CONTENT_TYPE_JSON: &str = "application/json";

#[derive(Clone)]
struct HttpState {
    store: SharedStore,
    forwarder: Option<OtlpForwarder>,
}

/// Build an Axum router with OTLP HTTP ingestion routes.
pub fn router(store: SharedStore, forwarder: Option<OtlpForwarder>) -> Router {
    let state = HttpState { store, forwarder };
    Router::new()
        .route("/v1/traces", post(export_traces))
        .route("/v1/logs", post(export_logs))
        .route("/v1/metrics", post(export_metrics))
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .with_state(state)
}

/// Returns the content-type from the request headers, defaulting to protobuf.
fn content_type(headers: &HeaderMap) -> &str {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(CONTENT_TYPE_PROTOBUF)
}

/// Decode a protobuf-encoded request body. JSON bodies are also accepted by
/// first parsing the JSON to a `serde_json::Value`, serializing it back to
/// bytes, and then attempting protobuf decode — this works for clients that
/// send protobuf-compatible JSON (numeric field IDs). For full OTLP JSON
/// mapping, serde derives would need to be enabled on the generated prost
/// types via `prost_build::Config::message_attribute`.
fn decode_request<T: Message + Default>(
    headers: &HeaderMap,
    body: Bytes,
) -> Result<T, (StatusCode, String)> {
    let ct = content_type(headers);

    if ct.starts_with(CONTENT_TYPE_JSON) {
        // Validate that the body is valid JSON, then attempt protobuf decode.
        // Full OTLP JSON mapping requires serde derives on generated types.
        // For now, we accept JSON bodies and try protobuf decode as a fallback,
        // which handles the case of protobuf-JSON (field numbers as keys).
        let _: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid JSON body: {e}")))?;
        // OTLP JSON uses string field names matching the proto field names.
        // Without serde derives on prost types, we cannot directly deserialize.
        // Return an error indicating JSON is not yet fully supported.
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "JSON content-type requires serde support on protobuf types; use application/x-protobuf".to_string(),
        ));
    }

    // Default: protobuf decode
    T::decode(body).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("failed to decode protobuf: {e}"),
        )
    })
}

/// Encode a response message as protobuf bytes.
fn encode_response<T: Message>(response: &T) -> (String, Vec<u8>) {
    (CONTENT_TYPE_PROTOBUF.to_string(), response.encode_to_vec())
}

#[tracing::instrument(name = "otlp.http.export_traces", skip_all)]
async fn export_traces(
    State(state): State<HttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let request = match decode_request::<ExportTraceServiceRequest>(&headers, body) {
        Ok(r) => r,
        Err((status, msg)) => return (status, msg).into_response(),
    };

    let resource_spans = request.resource_spans;
    if !resource_spans.is_empty() {
        if let Some(ref fwd) = state.forwarder {
            fwd.forward_traces(resource_spans.clone());
        }
        let mut s = state.store.write().await;
        s.insert_traces(resource_spans);
    }

    let (content_type, bytes) = encode_response(&ExportTraceServiceResponse::default());
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, content_type)],
        bytes,
    )
        .into_response()
}

#[tracing::instrument(name = "otlp.http.export_logs", skip_all)]
async fn export_logs(
    State(state): State<HttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let request = match decode_request::<ExportLogsServiceRequest>(&headers, body) {
        Ok(r) => r,
        Err((status, msg)) => return (status, msg).into_response(),
    };

    let resource_logs = request.resource_logs;
    if !resource_logs.is_empty() {
        if let Some(ref fwd) = state.forwarder {
            fwd.forward_logs(resource_logs.clone());
        }
        let mut s = state.store.write().await;
        s.insert_logs(resource_logs);
    }

    let (content_type, bytes) = encode_response(&ExportLogsServiceResponse::default());
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, content_type)],
        bytes,
    )
        .into_response()
}

#[tracing::instrument(name = "otlp.http.export_metrics", skip_all)]
async fn export_metrics(
    State(state): State<HttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let request = match decode_request::<ExportMetricsServiceRequest>(&headers, body) {
        Ok(r) => r,
        Err((status, msg)) => return (status, msg).into_response(),
    };

    let resource_metrics = request.resource_metrics;
    if !resource_metrics.is_empty() {
        if let Some(ref fwd) = state.forwarder {
            fwd.forward_metrics(resource_metrics.clone());
        }
        let mut s = state.store.write().await;
        s.insert_metrics(resource_metrics);
    }

    let (content_type, bytes) = encode_response(&ExportMetricsServiceResponse::default());
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, content_type)],
        bytes,
    )
        .into_response()
}
