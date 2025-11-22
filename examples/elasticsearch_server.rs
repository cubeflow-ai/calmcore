
use calm::{
    engine::Engine,
    protocol::{elasticsearch::ElasticsearchServer, graphql::GraphQLServer},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化 Engine
    let config = calm::engine::EngineConfig::default();
    let engine = Engine::new(config).expect("Failed to create engine");

    // 🔥 重要：加载磁盘上已有的表
    engine.load_existing_tables().await?;

    let _ = tokio::spawn({
        let engine = engine.clone();
        async move {
            let graphql_server = GraphQLServer::new(engine);
            graphql_server.start("127.0.0.1:9567").await
        }
    });

    // 启动 Elasticsearch Server
    let es_server = ElasticsearchServer::new(engine.clone());
    es_server.start("127.0.0.1:9200").await?;

    Ok(())
}
