use tokio::sync::broadcast;
use tonic::{Request, Response, Status, async_trait};

use crate::opentelemetry::proto::collector::{
    logs::v1::{
        ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService,
    },
    metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        metrics_service_server::MetricsService,
    },
    trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse, trace_service_server::TraceService,
    },
};
use crate::store::{SharedStore, StoreEvent};

#[derive(Clone)]
pub struct OtlpGrpcServer {
    pub store: SharedStore,
    pub event_tx: broadcast::Sender<StoreEvent>,
}

#[async_trait]
impl TraceService for OtlpGrpcServer {
    #[tracing::instrument(name = "otlp.grpc.export_traces", skip_all)]
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let req = request.into_inner();
        let resource_spans = req.resource_spans;
        self.store.write().await.insert_traces(resource_spans);
        Ok(Response::new(ExportTraceServiceResponse {
            ..Default::default()
        }))
    }
}

#[async_trait]
impl LogsService for OtlpGrpcServer {
    #[tracing::instrument(name = "otlp.grpc.export_logs", skip_all)]
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let req = request.into_inner();
        let resource_logs = req.resource_logs;
        self.store.write().await.insert_logs(resource_logs);
        Ok(Response::new(ExportLogsServiceResponse {
            ..Default::default()
        }))
    }
}

#[async_trait]
impl MetricsService for OtlpGrpcServer {
    #[tracing::instrument(name = "otlp.grpc.export_metrics", skip_all)]
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let req = request.into_inner();
        let resource_metrics = req.resource_metrics;
        self.store.write().await.insert_metrics(resource_metrics);
        Ok(Response::new(ExportMetricsServiceResponse {
            ..Default::default()
        }))
    }
}
