use anyhow::Result;

use crate::cli::{InitArgs, InitLang};

pub fn run(args: InitArgs) -> Result<()> {
    let content = match args.lang {
        None => generate_env(&args),
        Some(InitLang::Node) => generate_node(&args),
        Some(InitLang::Python) => generate_python(&args),
        Some(InitLang::Rust) => generate_rust(&args),
        Some(InitLang::Go) => generate_go(&args),
        Some(InitLang::Java) => generate_java(&args),
    };

    if let Some(ref path) = args.output {
        if std::path::Path::new(path).exists() {
            eprintln!("Warning: overwriting existing file {}", path);
        }
        std::fs::write(path, &content)?;
        eprintln!("Wrote config to {}", path);
    } else {
        print!("{}", content);
    }

    Ok(())
}

fn generate_env(args: &InitArgs) -> String {
    format!(
        "\
OTEL_EXPORTER_OTLP_ENDPOINT={endpoint}
OTEL_EXPORTER_OTLP_PROTOCOL=grpc
OTEL_SERVICE_NAME={service}
OTEL_TRACES_EXPORTER=otlp
OTEL_LOGS_EXPORTER=otlp
OTEL_METRICS_EXPORTER=otlp
",
        endpoint = args.endpoint,
        service = args.service_name,
    )
}

fn generate_node(args: &InitArgs) -> String {
    format!(
        r#"// OpenTelemetry OTLP setup for Node.js
// Run: npm install @opentelemetry/sdk-node @opentelemetry/exporter-trace-otlp-grpc \
//   @opentelemetry/exporter-logs-otlp-grpc @opentelemetry/exporter-metrics-otlp-grpc

const {{ NodeSDK }} = require('@opentelemetry/sdk-node');
const {{ OTLPTraceExporter }} = require('@opentelemetry/exporter-trace-otlp-grpc');
const {{ OTLPLogExporter }} = require('@opentelemetry/exporter-logs-otlp-grpc');
const {{ OTLPMetricExporter }} = require('@opentelemetry/exporter-metrics-otlp-grpc');
const {{ PeriodicExportingMetricReader }} = require('@opentelemetry/sdk-metrics');

const sdk = new NodeSDK({{
  serviceName: '{service}',
  traceExporter: new OTLPTraceExporter({{ url: '{endpoint}' }}),
  logExporter: new OTLPLogExporter({{ url: '{endpoint}' }}),
  metricReader: new PeriodicExportingMetricReader({{
    exporter: new OTLPMetricExporter({{ url: '{endpoint}' }}),
  }}),
}});

sdk.start();
process.on('SIGTERM', () => sdk.shutdown());
"#,
        endpoint = args.endpoint,
        service = args.service_name,
    )
}

fn generate_python(args: &InitArgs) -> String {
    format!(
        r#"# OpenTelemetry OTLP setup for Python
# Run: pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

resource = Resource.create({{"service.name": "{service}"}})

provider = TracerProvider(resource=resource)
exporter = OTLPSpanExporter(endpoint="{endpoint}", insecure=True)
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)

# Example usage:
# with tracer.start_as_current_span("my-operation"):
#     do_work()
"#,
        endpoint = args.endpoint,
        service = args.service_name,
    )
}

fn generate_rust(args: &InitArgs) -> String {
    format!(
        r#"// OpenTelemetry OTLP setup for Rust
// Run: cargo add opentelemetry opentelemetry-otlp opentelemetry_sdk tracing-opentelemetry tracing-subscriber

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::{{Resource, trace::SdkTracerProvider}};
use opentelemetry_sdk::trace::BatchSpanProcessor;
use opentelemetry_otlp::SpanExporter;

fn init_tracer() -> SdkTracerProvider {{
    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint("{endpoint}")
        .build()
        .expect("failed to create OTLP exporter");

    let provider = SdkTracerProvider::builder()
        .with_span_processor(BatchSpanProcessor::builder(exporter).build())
        .with_resource(Resource::builder().with_service_name("{service}").build())
        .build();

    let tracer = provider.tracer("{service}");

    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();

    provider
}}

// Call init_tracer() at startup and provider.shutdown() on exit.
"#,
        endpoint = args.endpoint,
        service = args.service_name,
    )
}

fn generate_go(args: &InitArgs) -> String {
    format!(
        r#"// OpenTelemetry OTLP setup for Go
// Run: go get go.opentelemetry.io/otel \
//   go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc \
//   go.opentelemetry.io/otel/sdk/trace

package main

import (
	"context"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func initTracer() (*sdktrace.TracerProvider, error) {{
	ctx := context.Background()

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint("{endpoint_host}"),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {{
		return nil, err
	}}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("{service}"),
		),
	)
	if err != nil {{
		return nil, err
	}}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
}}

// Call initTracer() at startup and tp.Shutdown(ctx) on exit.
"#,
        endpoint_host = args.endpoint.trim_start_matches("http://"),
        service = args.service_name,
    )
}

fn generate_java(args: &InitArgs) -> String {
    format!(
        r#"# OpenTelemetry OTLP setup for Java
#
# Option 1: Java agent (zero-code instrumentation)
# Download: https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases
#
# java -javaagent:opentelemetry-javaagent.jar \
#   -Dotel.exporter.otlp.endpoint={endpoint} \
#   -Dotel.service.name={service} \
#   -jar your-app.jar
#
# Option 2: Environment variables (works with the agent or SDK)
export OTEL_EXPORTER_OTLP_ENDPOINT={endpoint}
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
export OTEL_SERVICE_NAME={service}
export OTEL_TRACES_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=otlp
"#,
        endpoint = args.endpoint,
        service = args.service_name,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::InitArgs;

    fn default_args() -> InitArgs {
        InitArgs {
            lang: None,
            endpoint: "http://localhost:4317".to_string(),
            service_name: "test-svc".to_string(),
            output: None,
        }
    }

    #[test]
    fn test_generate_env_contains_endpoint() {
        let args = default_args();
        let content = generate_env(&args);
        assert!(content.contains("OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317"));
        assert!(content.contains("OTEL_SERVICE_NAME=test-svc"));
    }

    #[test]
    fn test_generate_env_custom_endpoint() {
        let mut args = default_args();
        args.endpoint = "http://collector:4317".to_string();
        let content = generate_env(&args);
        assert!(content.contains("http://collector:4317"));
    }

    #[test]
    fn test_generate_node_contains_service_name() {
        let args = default_args();
        let content = generate_node(&args);
        assert!(content.contains("test-svc"));
        assert!(content.contains("npm install") || content.contains("@opentelemetry"));
    }

    #[test]
    fn test_generate_python_contains_endpoint() {
        let args = default_args();
        let content = generate_python(&args);
        assert!(content.contains("localhost:4317"));
        assert!(content.contains("opentelemetry"));
    }

    #[test]
    fn test_generate_rust_contains_endpoint() {
        let args = default_args();
        let content = generate_rust(&args);
        assert!(content.contains("localhost:4317"));
    }

    #[test]
    fn test_generate_go_contains_service_name() {
        let args = default_args();
        let content = generate_go(&args);
        assert!(content.contains("test-svc"));
        assert!(content.contains("otlptracegrpc"));
    }

    #[test]
    fn test_generate_java_contains_endpoint() {
        let args = default_args();
        let content = generate_java(&args);
        assert!(content.contains("localhost:4317"));
        assert!(content.contains("test-svc"));
    }

    #[test]
    fn test_all_langs_produce_nonempty_output() {
        let args = default_args();
        assert!(!generate_env(&args).is_empty());
        assert!(!generate_node(&args).is_empty());
        assert!(!generate_python(&args).is_empty());
        assert!(!generate_rust(&args).is_empty());
        assert!(!generate_go(&args).is_empty());
        assert!(!generate_java(&args).is_empty());
    }

    #[test]
    fn test_output_to_file() {
        let dir = std::env::temp_dir().join("motel_test_init");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join(".env");
        let args = InitArgs {
            lang: None,
            endpoint: "http://localhost:4317".to_string(),
            service_name: "test-svc".to_string(),
            output: Some(path.to_string_lossy().to_string()),
        };
        run(args).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("OTEL_SERVICE_NAME=test-svc"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
