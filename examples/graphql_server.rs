use std::sync::Arc;

use calm::{engine::Engine, protocol::graphql::GraphQLServer};

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    env_logger::init();

    // 初始化 Engine
    let config = calm::engine::EngineConfig::default();
    let engine = Engine::new(config).expect("Failed to create engine");

    // 加载已存在的表
    engine
        .load_existing_tables()
        .await
        .expect("Failed to load existing tables");

    // 创建并启动 GraphQL 服务器
    let server = GraphQLServer::new(engine);
    server.start("127.0.0.1:8080").await
}
