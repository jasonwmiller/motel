use anyhow::Result;

use crate::cli::{ReplayArgs, ReplaySignal};
use crate::otel::collector::logs::v1::logs_service_client::LogsServiceClient;
use crate::otel::collector::logs::v1::ExportLogsServiceRequest;
use crate::otel::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use crate::otel::collector::metrics::v1::ExportMetricsServiceRequest;
use crate::otel::collector::trace::v1::trace_service_client::TraceServiceClient;
use crate::otel::collector::trace::v1::ExportTraceServiceRequest;
use crate::query_proto::query_service_client::QueryServiceClient;
use crate::query_proto::{QueryLogsRequest, QueryMetricsRequest, QueryTracesRequest};

pub async fn run(args: ReplayArgs) -> Result<()> {
    let send_traces = matches!(args.signal, ReplaySignal::All | ReplaySignal::Traces);
    let send_logs = matches!(args.signal, ReplaySignal::All | ReplaySignal::Logs);
    let send_metrics = matches!(args.signal, ReplaySignal::All | ReplaySignal::Metrics);

    let mut query_client = QueryServiceClient::connect(args.addr.clone()).await?;

    // Query data from source
    let mut trace_batches = Vec::new();
    let mut span_count: usize = 0;
    if send_traces {
        let request = QueryTracesRequest {
            service_name: args.service.clone().unwrap_or_default(),
            since: args.since.clone().unwrap_or_default(),
            ..Default::default()
        };
        let response = query_client.query_traces(request).await?;
        let resource_spans = response.into_inner().resource_spans;
        for rs in &resource_spans {
            for ss in &rs.scope_spans {
                span_count += ss.spans.len();
            }
        }
        trace_batches = resource_spans;
    }

    let mut log_batches = Vec::new();
    let mut log_record_count: usize = 0;
    if send_logs {
        let request = QueryLogsRequest {
            service_name: args.service.clone().unwrap_or_default(),
            since: args.since.clone().unwrap_or_default(),
            ..Default::default()
        };
        let response = query_client.query_logs(request).await?;
        let resource_logs = response.into_inner().resource_logs;
        for rl in &resource_logs {
            for sl in &rl.scope_logs {
                log_record_count += sl.log_records.len();
            }
        }
        log_batches = resource_logs;
    }

    let mut metric_batches = Vec::new();
    let mut metric_count: usize = 0;
    if send_metrics {
        let request = QueryMetricsRequest {
            service_name: args.service.clone().unwrap_or_default(),
            since: args.since.clone().unwrap_or_default(),
            ..Default::default()
        };
        let response = query_client.query_metrics(request).await?;
        let resource_metrics = response.into_inner().resource_metrics;
        for rm in &resource_metrics {
            for sm in &rm.scope_metrics {
                metric_count += sm.metrics.len();
            }
        }
        metric_batches = resource_metrics;
    }

    if args.dry_run {
        println!(
            "[dry-run] Would replay {} trace batches ({} spans), {} log batches ({} records), {} metric batches ({} metrics) to {}",
            trace_batches.len(),
            span_count,
            log_batches.len(),
            log_record_count,
            metric_batches.len(),
            metric_count,
            args.target,
        );
        return Ok(());
    }

    // Send data to target
    if !trace_batches.is_empty() {
        let mut trace_client = TraceServiceClient::connect(args.target.clone()).await?;
        for chunk in trace_batches.chunks(100) {
            let request = ExportTraceServiceRequest {
                resource_spans: chunk.to_vec(),
            };
            trace_client.export(request).await?;
        }
    }

    if !log_batches.is_empty() {
        let mut logs_client = LogsServiceClient::connect(args.target.clone()).await?;
        for chunk in log_batches.chunks(100) {
            let request = ExportLogsServiceRequest {
                resource_logs: chunk.to_vec(),
            };
            logs_client.export(request).await?;
        }
    }

    if !metric_batches.is_empty() {
        let mut metrics_client = MetricsServiceClient::connect(args.target.clone()).await?;
        for chunk in metric_batches.chunks(100) {
            let request = ExportMetricsServiceRequest {
                resource_metrics: chunk.to_vec(),
            };
            metrics_client.export(request).await?;
        }
    }

    println!(
        "Replayed {} trace batches ({} spans), {} log batches ({} records), {} metric batches ({} metrics) to {}",
        trace_batches.len(),
        span_count,
        log_batches.len(),
        log_record_count,
        metric_batches.len(),
        metric_count,
        args.target,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::cli::ReplaySignal;

    #[test]
    fn test_signal_matching() {
        // Verify signal filtering logic used in run()
        let all = ReplaySignal::All;
        assert!(matches!(all, ReplaySignal::All | ReplaySignal::Traces));
        assert!(matches!(all, ReplaySignal::All | ReplaySignal::Logs));
        assert!(matches!(all, ReplaySignal::All | ReplaySignal::Metrics));

        let traces = ReplaySignal::Traces;
        assert!(matches!(traces, ReplaySignal::All | ReplaySignal::Traces));
        assert!(!matches!(traces, ReplaySignal::All | ReplaySignal::Logs));
        assert!(!matches!(
            traces,
            ReplaySignal::All | ReplaySignal::Metrics
        ));

        let logs = ReplaySignal::Logs;
        assert!(!matches!(logs, ReplaySignal::All | ReplaySignal::Traces));
        assert!(matches!(logs, ReplaySignal::All | ReplaySignal::Logs));

        let metrics = ReplaySignal::Metrics;
        assert!(!matches!(
            metrics,
            ReplaySignal::All | ReplaySignal::Traces
        ));
        assert!(matches!(
            metrics,
            ReplaySignal::All | ReplaySignal::Metrics
        ));
    }
}
