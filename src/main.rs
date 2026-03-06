pub mod cli;
pub mod client;
pub mod config;
pub mod diff;
pub mod install;
pub mod mcp;
pub mod persist;
pub mod query;
pub mod server;
pub mod sink;
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
use cli::{Cli, Command, ConfigAction};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = config::load()?;

    match cli.command {
        Command::Server(args) => {
            let resolved = args.resolve(&config.server);
            server::run(resolved).await
        }
        Command::View(args) => {
            let resolved = args.resolve(&config);
            client::view::run(resolved).await
        }
        Command::Traces(args) => {
            let resolved = args.resolve(&config);
            client::trace::run(resolved).await
        }
        Command::Logs(args) => {
            let resolved = args.resolve(&config);
            client::log::run(resolved).await
        }
        Command::Metrics(args) => {
            let resolved = args.resolve(&config);
            client::metrics::run(resolved).await
        }
        Command::Sql(args) => {
            let resolved = args.resolve(&config);
            client::sql::run(resolved).await
        }
        Command::ServiceMap(args) => client::service_map::run(args).await,
        Command::Export(args) => client::export::run(args).await,
        Command::Latency(args) => client::latency::run(args).await,
        Command::Clear(args) => {
            let resolved = args.resolve(&config);
            client::clear::run(resolved).await
        }
        Command::Status(args) => {
            let resolved = args.resolve(&config);
            client::status::run(resolved).await
        }
        Command::Shutdown(args) => {
            let resolved = args.resolve(&config);
            client::shutdown::run(resolved).await
        }
        Command::Replay(args) => client::replay::run(args).await,
        Command::Import(args) => client::import::run(args).await,
        Command::Diff(args) => client::diff::run(args).await,
        Command::SkillInstall(args) => install::run(args),
        Command::Init(args) => client::init::run(args),
        Command::Mcp(args) => mcp::run(args).await,
        Command::Config(cmd) => match cmd.action {
            ConfigAction::Init => {
                let path = config::init()?;
                println!("Created config file at {}", path.display());
                Ok(())
            }
            ConfigAction::Path => {
                match config::config_path() {
                    Some(path) => println!("{}", path.display()),
                    None => anyhow::bail!("cannot determine config directory"),
                }
                Ok(())
            }
            ConfigAction::Show => {
                println!("{}", toml::to_string_pretty(&config)?);
                Ok(())
            }
        },
    }
}
