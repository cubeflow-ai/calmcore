mod commons;
mod graphql;
mod grpc;
mod service;
use std::sync::Arc;

use clap::Parser;
use commons::{load_config, network};
use log::info;
use service::Service;

#[derive(Default, Parser)]
pub struct Options {
    #[clap(short, long, default_value = "./config/config.toml")]
    pub config: String,
}

#[tokio::main]
async fn main() {
    println!("{}", version::version());
    let opts: Options = Options::parse();

    let mut config = load_config(&opts.config);

    // std::env::set_var("RUST_LOG", "debug");

    env_logger::init();

    if config.host.is_none() {
        let host = network::local_ip(&config.manager_addr).unwrap();
        info!("host is empty, set to ip:{}", host);
        config.host = Some(host);
    }

    println!("config: {:?}", config);

    let mut handers = vec![];

    let service = Arc::new(
        Service::new(config.manager_addr.clone(), config.data_path.clone())
            .expect("create service failed"),
    );

    if config.http_port > 0 {
        handers.push(tokio::spawn(graphql::start(
            config.host.clone().unwrap(),
            config.http_port,
            service.clone(),
        )));
    }
    if config.grpc_port > 0 {
        handers.push(tokio::spawn(grpc::start_grpc(
            config.host.clone().unwrap(),
            config.grpc_port,
            service.clone(),
        )));
    }

    for h in handers {
        h.await.unwrap();
    }
}

mod version {
    use version_macro::{build_git_branch, build_git_version, build_time};

    pub const BUILD_TIME: &str = build_time!();
    pub const BUILD_GIT_BRANCH: &str = build_git_branch!();
    pub const BUILD_GIT_VERSION: &str = build_git_version!();

    pub fn version() -> String {
        format!(
            "lark build info: [git_branch:{} , build_time:{} , git_version:{}]",
            BUILD_GIT_BRANCH, BUILD_TIME, BUILD_GIT_VERSION
        )
    }
}
