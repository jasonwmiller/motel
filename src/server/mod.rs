pub mod forwarder;
pub mod otlp_grpc;
pub mod otlp_http;
pub mod prometheus;
pub mod query_grpc;
pub mod query_http;
pub mod web;

use anyhow::Context;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use tokio::sync::oneshot;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

use crate::cli::{PersistFormat, ResolvedServerArgs};
use crate::otel::collector::{
    logs::v1::logs_service_server::LogsServiceServer,
    metrics::v1::metrics_service_server::MetricsServiceServer,
    trace::v1::trace_service_server::TraceServiceServer,
};
use crate::persist::SharedPersistBackend;
use crate::query_proto::query_service_server::QueryServiceServer;
use crate::store::Store;

use self::query_grpc::QueryServiceImpl;

pub async fn run(args: ResolvedServerArgs) -> anyhow::Result<()> {
    // Set up tracing subscriber
    init_tracing(&args)?;

    // Initialize persistence if configured
    let persist: Option<SharedPersistBackend> = if let Some(ref path) = args.persist {
        let backend: SharedPersistBackend = match args.persist_format {
            PersistFormat::Sqlite => {
                std::sync::Arc::new(crate::persist::sqlite::SqlitePersist::open(path)?)
            }
            PersistFormat::Parquet => {
                std::sync::Arc::new(crate::persist::parquet::ParquetPersist::open(path)?)
            }
        };
        tracing::info!(
            "Persistence enabled: {} (format: {:?})",
            path,
            args.persist_format
        );
        Some(backend)
    } else {
        None
    };

    // Create shared store with optional persistence backend and sampling
    let (store, _event_rx) = Store::new_shared_with_sampling(
        args.max_traces as usize,
        args.max_logs as usize,
        args.max_metrics as usize,
        persist.clone(),
        args.sample_rate,
        args.sample_always.clone(),
    );

    // Load persisted data on startup
    if let Some(ref backend) = persist {
        tracing::info!("Loading persisted data...");
        let mut s = store.write().await;

        let traces = backend.load_traces().await?;
        if !traces.is_empty() {
            tracing::info!("Loaded {} persisted trace batches", traces.len());
            s.insert_traces_no_persist(traces);
        }

        let logs = backend.load_logs().await?;
        if !logs.is_empty() {
            tracing::info!("Loaded {} persisted log batches", logs.len());
            s.insert_logs_no_persist(logs);
        }

        let metrics = backend.load_metrics().await?;
        if !metrics.is_empty() {
            tracing::info!("Loaded {} persisted metric batches", metrics.len());
            s.insert_metrics_no_persist(metrics);
        }
    }

    // Get the broadcast sender for event subscriptions
    let event_tx = store.read().await.event_tx.clone();

    // Create forwarder if configured
    let forwarder = forwarder::OtlpForwarder::new(
        &args.forward_to,
        &args.forward_headers,
        args.forward_timeout,
    );

    if forwarder.is_some() {
        tracing::info!(
            endpoints = ?args.forward_to,
            "OTLP forwarding enabled"
        );
    }

    // Shutdown channel from query service
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // Build OTLP gRPC service
    let otlp_grpc_store = store.clone();
    let otlp_event_tx = event_tx.clone();
    let grpc_forwarder = forwarder.clone();
    let grpc_addr = args.grpc_addr.parse().context("invalid gRPC address")?;
    let otlp_grpc_handle = tokio::spawn(async move {
        tracing::info!("OTLP gRPC listening on {}", grpc_addr);
        let otlp_server = otlp_grpc::OtlpGrpcServer {
            store: otlp_grpc_store,
            event_tx: otlp_event_tx,
            forwarder: grpc_forwarder,
        };
        const MAX_MSG: usize = 16 * 1024 * 1024;
        Server::builder()
            .add_service(
                TraceServiceServer::new(otlp_server.clone()).max_decoding_message_size(MAX_MSG),
            )
            .add_service(
                LogsServiceServer::new(otlp_server.clone()).max_decoding_message_size(MAX_MSG),
            )
            .add_service(MetricsServiceServer::new(otlp_server).max_decoding_message_size(MAX_MSG))
            .serve(grpc_addr)
            .await
            .context("OTLP gRPC server failed")
    });

    // Build DataFusion context for SQL queries (needed by both HTTP and gRPC query services)
    let session_ctx = crate::query::datafusion_ctx::create_context(store.clone()).await?;

    // Build OTLP HTTP service merged with query HTTP API
    let http_addr: std::net::SocketAddr = args.http_addr.parse().context("invalid HTTP address")?;
    let otlp_http_store = store.clone();
    let query_http_state = query_http::QueryHttpState {
        store: store.clone(),
        session_ctx: session_ctx.clone(),
    };
    let http_forwarder = forwarder.clone();
    let otlp_http_handle = tokio::spawn(async move {
        tracing::info!("OTLP HTTP listening on {}", http_addr);
        let otlp_router = otlp_http::router(otlp_http_store, http_forwarder);
        let query_router = query_http::router(query_http_state);
        let router = otlp_router.merge(query_router);
        let listener = tokio::net::TcpListener::bind(http_addr)
            .await
            .context("failed to bind OTLP HTTP address")?;
        axum::serve(listener, router)
            .await
            .context("OTLP HTTP server failed")
    });

    // Build Query gRPC service
    let query_addr = args.query_addr.parse().context("invalid query address")?;
    let query_service = QueryServiceImpl {
        store: store.clone(),
        event_tx: event_tx.clone(),
        shutdown_tx: std::sync::Mutex::new(Some(shutdown_tx)),
        session_ctx,
    };
    let query_grpc_handle = tokio::spawn(async move {
        tracing::info!("Query gRPC listening on {}", query_addr);
        Server::builder()
            .add_service(
                QueryServiceServer::new(query_service).max_decoding_message_size(16 * 1024 * 1024),
            )
            .serve(query_addr)
            .await
            .context("Query gRPC server failed")
    });

    // Start TUI if enabled
    let tui_handle = if !args.no_tui {
        let tui_store = store.clone();
        let tui_event_rx = event_tx.subscribe();
        Some(tokio::spawn(async move {
            crate::tui::run(tui_store, tui_event_rx).await
        }))
    } else {
        None
    };

    // Start web UI if enabled
    let web_handle = if args.web {
        let web_addr: std::net::SocketAddr =
            args.web_addr.parse().context("invalid web address")?;
        let web_session_ctx = crate::query::datafusion_ctx::create_context(store.clone()).await?;
        let web_state = web::WebState {
            store: store.clone(),
            event_tx: event_tx.clone(),
            session_ctx: std::sync::Arc::new(web_session_ctx),
        };
        let web_router = web::router(web_state);
        Some(tokio::spawn(async move {
            tracing::info!("Web UI listening on http://{}", web_addr);
            let listener = tokio::net::TcpListener::bind(web_addr)
                .await
                .context("failed to bind web UI address")?;
            axum::serve(listener, web_router)
                .await
                .context("Web UI server failed")
        }))
    } else {
        None
    };

    // Start file sink if configured
    let sink_handle = if let Some(ref sink_dir) = args.sink {
        let sink_rx = event_tx.subscribe();
        let sink_dir = std::path::PathBuf::from(sink_dir);
        let sink_format = args.sink_format.clone();
        let sink_max_size = args.sink_max_size;
        let sink_rotate_interval = crate::sink::parse_duration(&args.sink_rotate_interval)?;
        Some(tokio::spawn(async move {
            crate::sink::run(sink_rx, sink_dir, sink_format, sink_max_size, sink_rotate_interval)
                .await
        }))
    } else {
        None
    };

    // Start Prometheus scrape endpoint if enabled
    let prom_handle = if args.prometheus || args.prom_addr != "0.0.0.0:9090" {
        let prom_addr: std::net::SocketAddr = args
            .prom_addr
            .parse()
            .context("invalid Prometheus address")?;
        let prom_store = store.clone();
        Some(tokio::spawn(async move {
            tracing::info!("Prometheus endpoint listening on {}", prom_addr);
            let router = prometheus::router(prom_store);
            let listener = tokio::net::TcpListener::bind(prom_addr)
                .await
                .context("failed to bind Prometheus address")?;
            axum::serve(listener, router)
                .await
                .context("Prometheus server failed")
        }))
    } else {
        None
    };

    tracing::info!(
        "motel server started: gRPC={}, HTTP={}, Query={}{}{}",
        args.grpc_addr,
        args.http_addr,
        args.query_addr,
        if args.web {
            format!(", Web={}", args.web_addr)
        } else {
            String::new()
        },
        if args.prometheus || args.prom_addr != "0.0.0.0:9090" {
            format!(", Prometheus={}", args.prom_addr)
        } else {
            String::new()
        },
    );

    // Wait for shutdown signal (Ctrl+C, remote shutdown, or TUI exit)
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down");
        }
        _ = shutdown_rx => {
            tracing::info!("Received remote shutdown command");
        }
        result = otlp_grpc_handle => {
            result??;
        }
        result = otlp_http_handle => {
            result??;
        }
        result = query_grpc_handle => {
            result??;
        }
        result = async {
            if let Some(handle) = tui_handle {
                handle.await
            } else {
                // No TUI — never resolves
                std::future::pending().await
            }
        } => {
            result??;
            tracing::info!("TUI exited, shutting down");
        }
        result = async {
            if let Some(handle) = web_handle {
                handle.await
            } else {
                std::future::pending().await
            }
        } => {
            result??;
            tracing::info!("Web UI exited");
        }
        result = async {
            if let Some(handle) = sink_handle {
                handle.await
            } else {
                std::future::pending().await
            }
        } => {
            result??;
            tracing::warn!("Sink task exited unexpectedly");
        }
        result = async {
            if let Some(handle) = prom_handle {
                handle.await
            } else {
                std::future::pending().await
            }
        } => {
            result??;
            tracing::info!("Prometheus endpoint exited");
        }
    }

    tracing::info!("Server shutdown complete");
    Ok(())
}

fn init_tracing(args: &ResolvedServerArgs) -> anyhow::Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if let Some(ref endpoint) = args.otlp_endpoint {
        // Set up OpenTelemetry exporter for self-instrumentation
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .context("failed to create OTLP exporter")?;

        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(
                opentelemetry_sdk::Resource::builder()
                    .with_service_name("motel")
                    .build(),
            )
            .build();

        let telemetry_layer =
            tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("motel"));

        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().with_target(false))
            .with(telemetry_layer)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().with_target(false))
            .init();
    }

    Ok(())
}
