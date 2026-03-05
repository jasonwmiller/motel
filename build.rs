fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_include = "proto/opentelemetry-proto";

    let prost_config = prost_build::Config::new();
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_with_config(
            prost_config,
            &[
                "proto/query.proto",
                &format!(
                    "{proto_include}/opentelemetry/proto/collector/trace/v1/trace_service.proto"
                ),
                &format!(
                    "{proto_include}/opentelemetry/proto/collector/logs/v1/logs_service.proto"
                ),
                &format!(
                    "{proto_include}/opentelemetry/proto/collector/metrics/v1/metrics_service.proto"
                ),
            ],
            &["proto", proto_include],
        )?;

    Ok(())
}
