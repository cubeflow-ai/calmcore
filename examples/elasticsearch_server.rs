use std::sync::Arc;

use calm::{engine::Engine, protocol::elasticsearch::ElasticsearchServer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化 Engine
    let config = calm::engine::EngineConfig::default();
    let engine = Engine::new(config).expect("Failed to create engine");

    // 启动 Elasticsearch Server
    let es_server = ElasticsearchServer::new(engine.clone());
    es_server.start("127.0.0.1:9200").await?;

    Ok(())
}
