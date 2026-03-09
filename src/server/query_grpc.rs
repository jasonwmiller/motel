use std::pin::Pin;

use tokio::sync::{broadcast, mpsc, oneshot};
use tonic::{Request, Response, Status};

use datafusion::prelude::SessionContext;

use crate::query_proto::query_service_server::QueryService;
use crate::query_proto::*;
use crate::store::{SharedStore, StoreEvent};

pub struct QueryServiceImpl {
    pub store: SharedStore,
    pub event_tx: broadcast::Sender<StoreEvent>,
    pub shutdown_tx: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    pub session_ctx: SessionContext,
}

type ResponseStream<T> = Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl QueryService for QueryServiceImpl {
    #[tracing::instrument(skip_all)]
    async fn query_traces(
        &self,
        request: Request<QueryTracesRequest>,
    ) -> Result<Response<QueryTracesResponse>, Status> {
        let req = request.into_inner();
        let store = self.store.read().await;
        let mut resource_spans: Vec<_> = store.traces.iter().cloned().collect();
        drop(store);

        // Filter by service_name
        if !req.service_name.is_empty() {
            resource_spans.retain(|rs| {
                rs.resource.as_ref().is_some_and(|r| {
                    r.attributes.iter().any(|kv| {
                        kv.key == "service.name"
                            && kv.value.as_ref().is_some_and(|v| {
                                matches!(&v.value, Some(crate::otel::common::v1::any_value::Value::StringValue(s)) if s == &req.service_name)
                            })
                    })
                })
            });
        }

        // Filter by span_name
        if !req.span_name.is_empty() {
            for rs in &mut resource_spans {
                for ss in &mut rs.scope_spans {
                    ss.spans.retain(|s| s.name == req.span_name);
                }
                rs.scope_spans.retain(|ss| !ss.spans.is_empty());
            }
            resource_spans.retain(|rs| !rs.scope_spans.is_empty());
        }

        // Filter by trace_id
        if !req.trace_id.is_empty() {
            let trace_id_bytes = hex::decode(&req.trace_id)
                .map_err(|e| Status::invalid_argument(format!("invalid trace_id hex: {e}")))?;
            for rs in &mut resource_spans {
                for ss in &mut rs.scope_spans {
                    ss.spans.retain(|s| s.trace_id == trace_id_bytes);
                }
                rs.scope_spans.retain(|ss| !ss.spans.is_empty());
            }
            resource_spans.retain(|rs| !rs.scope_spans.is_empty());
        }

        // Apply limit
        if req.limit > 0 {
            resource_spans.truncate(req.limit as usize);
        }

        Ok(Response::new(QueryTracesResponse { resource_spans }))
    }

    #[tracing::instrument(skip_all)]
    async fn query_logs(
        &self,
        request: Request<QueryLogsRequest>,
    ) -> Result<Response<QueryLogsResponse>, Status> {
        let req = request.into_inner();
        let store = self.store.read().await;
        let mut resource_logs: Vec<_> = store.logs.iter().cloned().collect();
        drop(store);

        // Filter by service_name
        if !req.service_name.is_empty() {
            resource_logs.retain(|rl| {
                rl.resource.as_ref().is_some_and(|r| {
                    r.attributes.iter().any(|kv| {
                        kv.key == "service.name"
                            && kv.value.as_ref().is_some_and(|v| {
                                matches!(&v.value, Some(crate::otel::common::v1::any_value::Value::StringValue(s)) if s == &req.service_name)
                            })
                    })
                })
            });
        }

        // Filter by severity
        if !req.severity.is_empty() {
            let severity_upper = req.severity.to_uppercase();
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
        if !req.body_contains.is_empty() {
            for rl in &mut resource_logs {
                for sl in &mut rl.scope_logs {
                    sl.log_records.retain(|lr| {
                        lr.body
                            .as_ref()
                            .is_some_and(|body| format!("{:?}", body).contains(&req.body_contains))
                    });
                }
                rl.scope_logs.retain(|sl| !sl.log_records.is_empty());
            }
            resource_logs.retain(|rl| !rl.scope_logs.is_empty());
        }

        // Apply limit
        if req.limit > 0 {
            resource_logs.truncate(req.limit as usize);
        }

        Ok(Response::new(QueryLogsResponse { resource_logs }))
    }

    #[tracing::instrument(skip_all)]
    async fn query_metrics(
        &self,
        request: Request<QueryMetricsRequest>,
    ) -> Result<Response<QueryMetricsResponse>, Status> {
        let req = request.into_inner();
        let store = self.store.read().await;
        let mut resource_metrics: Vec<_> = store.metrics.iter().cloned().collect();
        drop(store);

        // Filter by service_name
        if !req.service_name.is_empty() {
            resource_metrics.retain(|rm| {
                rm.resource.as_ref().is_some_and(|r| {
                    r.attributes.iter().any(|kv| {
                        kv.key == "service.name"
                            && kv.value.as_ref().is_some_and(|v| {
                                matches!(&v.value, Some(crate::otel::common::v1::any_value::Value::StringValue(s)) if s == &req.service_name)
                            })
                    })
                })
            });
        }

        // Filter by metric_name
        if !req.metric_name.is_empty() {
            for rm in &mut resource_metrics {
                for sm in &mut rm.scope_metrics {
                    sm.metrics.retain(|m| m.name == req.metric_name);
                }
                rm.scope_metrics.retain(|sm| !sm.metrics.is_empty());
            }
            resource_metrics.retain(|rm| !rm.scope_metrics.is_empty());
        }

        // Apply limit
        if req.limit > 0 {
            resource_metrics.truncate(req.limit as usize);
        }

        Ok(Response::new(QueryMetricsResponse { resource_metrics }))
    }

    type FollowTracesStream = ResponseStream<FollowTracesResponse>;

    #[tracing::instrument(skip_all, name = "query.follow_traces")]
    async fn follow_traces(
        &self,
        _request: Request<FollowRequest>,
    ) -> Result<Response<Self::FollowTracesStream>, Status> {
        let mut rx = self.event_tx.subscribe();
        let (tx, mpsc_rx) = mpsc::channel(256);

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(StoreEvent::TracesInserted(resource_spans)) => {
                        let resp = FollowTracesResponse { resource_spans };
                        if tx.send(Ok(resp)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    _ => continue,
                }
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(mpsc_rx),
        )))
    }

    type FollowLogsStream = ResponseStream<FollowLogsResponse>;

    #[tracing::instrument(skip_all, name = "query.follow_logs")]
    async fn follow_logs(
        &self,
        _request: Request<FollowRequest>,
    ) -> Result<Response<Self::FollowLogsStream>, Status> {
        let mut rx = self.event_tx.subscribe();
        let (tx, mpsc_rx) = mpsc::channel(256);

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(StoreEvent::LogsInserted(resource_logs)) => {
                        let resp = FollowLogsResponse { resource_logs };
                        if tx.send(Ok(resp)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    _ => continue,
                }
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(mpsc_rx),
        )))
    }

    type FollowMetricsStream = ResponseStream<FollowMetricsResponse>;

    #[tracing::instrument(skip_all, name = "query.follow_metrics")]
    async fn follow_metrics(
        &self,
        _request: Request<FollowRequest>,
    ) -> Result<Response<Self::FollowMetricsStream>, Status> {
        let mut rx = self.event_tx.subscribe();
        let (tx, mpsc_rx) = mpsc::channel(256);

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(StoreEvent::MetricsInserted(resource_metrics)) => {
                        let resp = FollowMetricsResponse { resource_metrics };
                        if tx.send(Ok(resp)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    _ => continue,
                }
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(mpsc_rx),
        )))
    }

    #[tracing::instrument(skip_all, name = "query.sql_query")]
    async fn sql_query(
        &self,
        request: Request<SqlQueryRequest>,
    ) -> Result<Response<SqlQueryResponse>, Status> {
        let req = request.into_inner();
        let (columns, rows) =
            crate::query::sql::execute_with_columns(&self.session_ctx, &req.query)
                .await
                .map_err(|e| Status::internal(format!("SQL error: {e}")))?;
        Ok(Response::new(SqlQueryResponse { columns, rows }))
    }

    #[tracing::instrument(skip_all, name = "query.clear_traces")]
    async fn clear_traces(
        &self,
        _request: Request<ClearRequest>,
    ) -> Result<Response<ClearResponse>, Status> {
        let count = self.store.write().await.clear_traces();
        Ok(Response::new(ClearResponse {
            cleared_count: count as i64,
        }))
    }

    #[tracing::instrument(skip_all, name = "query.clear_logs")]
    async fn clear_logs(
        &self,
        _request: Request<ClearRequest>,
    ) -> Result<Response<ClearResponse>, Status> {
        let count = self.store.write().await.clear_logs();
        Ok(Response::new(ClearResponse {
            cleared_count: count as i64,
        }))
    }

    #[tracing::instrument(skip_all, name = "query.clear_metrics")]
    async fn clear_metrics(
        &self,
        _request: Request<ClearRequest>,
    ) -> Result<Response<ClearResponse>, Status> {
        let count = self.store.write().await.clear_metrics();
        Ok(Response::new(ClearResponse {
            cleared_count: count as i64,
        }))
    }

    #[tracing::instrument(skip_all, name = "query.clear_all")]
    async fn clear_all(
        &self,
        _request: Request<ClearRequest>,
    ) -> Result<Response<ClearResponse>, Status> {
        let mut store = self.store.write().await;
        let count = store.clear_traces() + store.clear_logs() + store.clear_metrics();
        Ok(Response::new(ClearResponse {
            cleared_count: count as i64,
        }))
    }

    #[tracing::instrument(skip_all, name = "query.pin_trace")]
    async fn pin_trace(
        &self,
        request: Request<PinTraceRequest>,
    ) -> Result<Response<PinTraceResponse>, Status> {
        let trace_id = request.into_inner().trace_id;
        let mut store = self.store.write().await;
        let pinned = store.pin_trace(trace_id);
        Ok(Response::new(PinTraceResponse { pinned }))
    }

    #[tracing::instrument(skip_all, name = "query.unpin_trace")]
    async fn unpin_trace(
        &self,
        request: Request<UnpinTraceRequest>,
    ) -> Result<Response<UnpinTraceResponse>, Status> {
        let trace_id = request.into_inner().trace_id;
        let mut store = self.store.write().await;
        let was_pinned = store.unpin_trace(&trace_id);
        Ok(Response::new(UnpinTraceResponse { was_pinned }))
    }

    #[tracing::instrument(skip_all, name = "query.status")]
    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let store = self.store.read().await;
        Ok(Response::new(StatusResponse {
            trace_count: store.trace_count() as i64,
            span_count: store.span_count() as i64,
            log_count: store.log_count() as i64,
            metric_count: store.metric_count() as i64,
            sample_rate: store.sample_rate,
            traces_dropped: store.traces_dropped as i64,
        }))
    }

    #[tracing::instrument(skip_all, name = "query.shutdown")]
    async fn shutdown(
        &self,
        _request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        let tx = self.shutdown_tx.lock().unwrap().take();
        if let Some(tx) = tx {
            let _ = tx.send(());
            Ok(Response::new(ShutdownResponse {
                message: "Server shutting down".into(),
            }))
        } else {
            Err(Status::failed_precondition("Shutdown already in progress"))
        }
    }
}
