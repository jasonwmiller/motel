pub mod cli;
pub mod client;
pub mod install;
pub mod query;
pub mod server;
pub mod store;
pub mod tui;

// Generated protobuf types — module structure must match proto package paths
// so that cross-references between generated code resolve correctly.
pub mod opentelemetry {
    pub mod proto {
        pub mod common {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.common.v1");
            }
        }
        pub mod resource {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.resource.v1");
            }
        }
        pub mod trace {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.trace.v1");
            }
        }
        pub mod logs {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.logs.v1");
            }
        }
        pub mod metrics {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.metrics.v1");
            }
        }
        pub mod collector {
            pub mod trace {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.collector.trace.v1");
                }
            }
            pub mod logs {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.collector.logs.v1");
                }
            }
            pub mod metrics {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.collector.metrics.v1");
                }
            }
        }
    }
}

pub mod motel {
    pub mod query {
        tonic::include_proto!("motel.query");
    }
}

// Convenience aliases
pub use motel::query as query_proto;
pub use opentelemetry::proto as otel;

use clap::Parser;
use cli::{Cli, Command};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Server(args) => server::run(args).await,
        Command::View(args) => client::view::run(args).await,
        Command::Traces(args) => client::trace::run(args).await,
        Command::Logs(args) => client::log::run(args).await,
        Command::Metrics(args) => client::metrics::run(args).await,
        Command::Sql(args) => client::sql::run(args).await,
        Command::ServiceMap(args) => client::service_map::run(args).await,
        Command::Clear(args) => client::clear::run(args).await,
        Command::Status(args) => client::status::run(args).await,
        Command::Shutdown(args) => client::shutdown::run(args).await,
        Command::Replay(args) => client::replay::run(args).await,
        Command::SkillInstall(args) => install::run(args),
        Command::Init(args) => client::init::run(args),
    }
}
