pub mod server;

use std::sync::Arc;

use crate::service::Service;
use log::info;

pub async fn start_grpc(host: String, port: u32, service: Arc<Service>) {
    //start server for grpc
    let addr = format!("{}:{}", host, port).parse().unwrap();

    info!("grpc server start on:{}", addr);

    tonic::transport::Server::builder()
        .add_service(proto::calmserver::server_server::ServerServer::new(
            server::GrpcServer::new(service),
        ))
        .serve(addr)
        .await
        .unwrap();
}
