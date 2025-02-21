use std::sync::Arc;

use async_graphql::{
    http::{playground_source, GraphQLPlaygroundConfig},
    EmptyMutation, EmptySubscription, Schema,
};
use async_graphql_poem::GraphQL;
use poem::{get, handler, listener::TcpListener, web::Html, IntoResponse, Route, Server};

use crate::{graphql::query::QueryRoot, service::Service};

mod models;
mod query;

#[handler]
fn graphql_playground() -> impl IntoResponse {
    Html(playground_source(GraphQLPlaygroundConfig::new("/")))
}

pub async fn start(host: String, port: u32, service: Arc<Service>) {
    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(service)
        .finish();

    let app = Route::new().at("/", get(graphql_playground).post(GraphQL::new(schema)));

    log::info!("graphql Playground: http://{}:{}", host, port);

    if let Err(e) = Server::new(TcpListener::bind(format!("{}:{}", host, port)))
        .run(app)
        .await
    {
        log::error!("Failed to start graphql on port:{} server: {:?}", port, e);
    };
}
