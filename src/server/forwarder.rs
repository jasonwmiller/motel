use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tonic::transport::Channel;

use crate::otel::collector::{
    logs::v1::{ExportLogsServiceRequest, logs_service_client::LogsServiceClient},
    metrics::v1::{ExportMetricsServiceRequest, metrics_service_client::MetricsServiceClient},
    trace::v1::{ExportTraceServiceRequest, trace_service_client::TraceServiceClient},
};
use crate::otel::{logs::v1::ResourceLogs, metrics::v1::ResourceMetrics, trace::v1::ResourceSpans};

/// Manages forwarding OTLP data to one or more upstream endpoints.
/// All forwarding is non-blocking (spawns tokio tasks) and failures
/// are logged but never block ingestion.
#[derive(Clone)]
pub struct OtlpForwarder {
    endpoints: Arc<Vec<ForwardEndpoint>>,
}

struct ForwardEndpoint {
    url: String,
    headers: HashMap<String, String>,
    timeout: Duration,
}

impl OtlpForwarder {
    /// Create a new forwarder. Returns None if no endpoints are configured.
    pub fn new(urls: &[String], header_args: &[String], timeout_secs: u64) -> Option<Self> {
        if urls.is_empty() {
            return None;
        }

        let headers = parse_headers(header_args);
        let timeout = Duration::from_secs(timeout_secs);

        let endpoints: Vec<ForwardEndpoint> = urls
            .iter()
            .map(|url| ForwardEndpoint {
                url: url.clone(),
                headers: headers.clone(),
                timeout,
            })
            .collect();

        Some(Self {
            endpoints: Arc::new(endpoints),
        })
    }

    /// Forward traces asynchronously (non-blocking).
    /// Spawns a tokio task per endpoint.
    pub fn forward_traces(&self, resource_spans: Vec<ResourceSpans>) {
        for endpoint in self.endpoints.iter() {
            let spans = resource_spans.clone();
            let url = endpoint.url.clone();
            let headers = endpoint.headers.clone();
            let timeout = endpoint.timeout;
            tokio::spawn(async move {
                if let Err(e) = forward_traces_to(&url, &headers, timeout, spans).await {
                    tracing::warn!(endpoint = %url, error = %e, "failed to forward traces");
                }
            });
        }
    }

    /// Forward logs asynchronously (non-blocking).
    pub fn forward_logs(&self, resource_logs: Vec<ResourceLogs>) {
        for endpoint in self.endpoints.iter() {
            let logs = resource_logs.clone();
            let url = endpoint.url.clone();
            let headers = endpoint.headers.clone();
            let timeout = endpoint.timeout;
            tokio::spawn(async move {
                if let Err(e) = forward_logs_to(&url, &headers, timeout, logs).await {
                    tracing::warn!(endpoint = %url, error = %e, "failed to forward logs");
                }
            });
        }
    }

    /// Forward metrics asynchronously (non-blocking).
    pub fn forward_metrics(&self, resource_metrics: Vec<ResourceMetrics>) {
        for endpoint in self.endpoints.iter() {
            let metrics = resource_metrics.clone();
            let url = endpoint.url.clone();
            let headers = endpoint.headers.clone();
            let timeout = endpoint.timeout;
            tokio::spawn(async move {
                if let Err(e) = forward_metrics_to(&url, &headers, timeout, metrics).await {
                    tracing::warn!(endpoint = %url, error = %e, "failed to forward metrics");
                }
            });
        }
    }
}

#[tracing::instrument(name = "forward.traces", skip_all, fields(endpoint = %url))]
async fn forward_traces_to(
    url: &str,
    headers: &HashMap<String, String>,
    timeout: Duration,
    resource_spans: Vec<ResourceSpans>,
) -> anyhow::Result<()> {
    let channel = Channel::from_shared(url.to_string())?
        .timeout(timeout)
        .connect()
        .await?;
    let mut client = TraceServiceClient::new(channel);

    let mut request = tonic::Request::new(ExportTraceServiceRequest { resource_spans });
    apply_headers(request.metadata_mut(), headers)?;

    client.export(request).await?;
    Ok(())
}

#[tracing::instrument(name = "forward.logs", skip_all, fields(endpoint = %url))]
async fn forward_logs_to(
    url: &str,
    headers: &HashMap<String, String>,
    timeout: Duration,
    resource_logs: Vec<ResourceLogs>,
) -> anyhow::Result<()> {
    let channel = Channel::from_shared(url.to_string())?
        .timeout(timeout)
        .connect()
        .await?;
    let mut client = LogsServiceClient::new(channel);

    let mut request = tonic::Request::new(ExportLogsServiceRequest { resource_logs });
    apply_headers(request.metadata_mut(), headers)?;

    client.export(request).await?;
    Ok(())
}

#[tracing::instrument(name = "forward.metrics", skip_all, fields(endpoint = %url))]
async fn forward_metrics_to(
    url: &str,
    headers: &HashMap<String, String>,
    timeout: Duration,
    resource_metrics: Vec<ResourceMetrics>,
) -> anyhow::Result<()> {
    let channel = Channel::from_shared(url.to_string())?
        .timeout(timeout)
        .connect()
        .await?;
    let mut client = MetricsServiceClient::new(channel);

    let mut request = tonic::Request::new(ExportMetricsServiceRequest { resource_metrics });
    apply_headers(request.metadata_mut(), headers)?;

    client.export(request).await?;
    Ok(())
}

fn apply_headers(
    metadata: &mut tonic::metadata::MetadataMap,
    headers: &HashMap<String, String>,
) -> anyhow::Result<()> {
    for (k, v) in headers {
        metadata.insert(
            k.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>()?,
            v.parse()?,
        );
    }
    Ok(())
}

fn parse_headers(args: &[String]) -> HashMap<String, String> {
    args.iter()
        .filter_map(|s| s.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_forwarder_none_when_no_urls() {
        let fwd = OtlpForwarder::new(&[], &[], 10);
        assert!(fwd.is_none());
    }

    #[test]
    fn test_parse_headers() {
        let headers = parse_headers(&[
            "x-api-key=secret123".to_string(),
            "x-team=myteam".to_string(),
        ]);
        assert_eq!(headers.get("x-api-key"), Some(&"secret123".to_string()));
        assert_eq!(headers.get("x-team"), Some(&"myteam".to_string()));
    }

    #[test]
    fn test_parse_headers_with_equals_in_value() {
        let headers = parse_headers(&["auth=token=abc=123".to_string()]);
        assert_eq!(headers.get("auth"), Some(&"token=abc=123".to_string()));
    }

    #[test]
    fn test_forwarder_created_with_urls() {
        let fwd = OtlpForwarder::new(&["http://localhost:4317".to_string()], &[], 10);
        assert!(fwd.is_some());
    }

    #[test]
    fn test_parse_headers_empty() {
        let headers = parse_headers(&[]);
        assert!(headers.is_empty());
    }

    #[test]
    fn test_parse_headers_ignores_invalid() {
        let headers = parse_headers(&["no-equals-sign".to_string(), "valid=value".to_string()]);
        assert_eq!(headers.len(), 1);
        assert_eq!(headers.get("valid"), Some(&"value".to_string()));
    }
}
