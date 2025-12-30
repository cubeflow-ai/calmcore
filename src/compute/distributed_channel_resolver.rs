//! ChannelResolver implementation for CalmCore
//!
//! 将 datafusion-distributed 的网络抽象与我们的 CalmService RPC 层集成

use arrow_flight::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion_distributed::{BoxCloneSyncChannel, ChannelResolver};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use url::Url;

use crate::cluster::ClusterManager;

/// CalmCore 的 ChannelResolver 实现
///
/// **关键设计**：
/// - 基于特定表创建（不是全局的集群视图）
/// - 在创建时从 Catalog 获取该表的节点分布
/// - 返回"拥有该表数据的节点列表"
///
/// 这样 datafusion-distributed 会自动把查询路由到正确的节点
#[derive(Clone)]
pub struct CalmChannelResolver {
    /// 节点 URL 列表（拥有目标表数据的节点）
    node_urls: Arc<Vec<Url>>,
    /// 连接缓存
    cached_clients: Arc<RwLock<HashMap<String, FlightServiceClient<BoxCloneSyncChannel>>>>,
}

impl CalmChannelResolver {
    /// 为特定表创建 resolver（从 Catalog 获取节点分布）
    ///
    /// 这是推荐的方式：每个查询根据目标表创建对应的 ChannelResolver
    pub async fn new_for_table(
        catalog: &crate::catalog::Catalog,
        table_name: &str,
    ) -> crate::utils::error::CoreResult<Self> {
        use std::collections::HashSet;

        // 从 Catalog 获取表信息
        let table_info = catalog.get_or_load_table(table_name).await?;
        let partitions = table_info.partitions.read().await;

        // 提取所有拥有该表分区的节点
        let node_ids: HashSet<String> = partitions.values().map(|p| p.owner.clone()).collect();

        log::info!(
            "🗺️  [ChannelResolver] Table '{}' has data on {} nodes: {:?}",
            table_name,
            node_ids.len(),
            node_ids
        );

        // 转换为 Arrow Flight URL
        // node_id 格式: "timestamp_host_tarpc_port_custom_flight_port"
        // 例如: "20251225120019657_127.0.0.1_7951_52002"
        // custom_flight_port 是 do_put + SQL 端口
        // distributed Flight 端口 = custom_flight_port + 1

        let node_urls: Vec<Url> = node_ids
            .iter()
            .filter_map(|node_id| {
                // 使用 cluster keys 模块的解析函数提取 custom Flight 地址
                if let Some(mut socket_addr) =
                    crate::cluster::keys::parse_flight_address_from_node_id(node_id)
                {
                    // ChannelResolver 需要连接到 distributed Flight（+1）
                    socket_addr.set_port(socket_addr.port() + 1);

                    let flight_url = format!("http://{}", socket_addr);
                    match Url::parse(&flight_url) {
                        Ok(url) => {
                            log::debug!(
                                "🔗 [ChannelResolver] Mapped node {} -> {} (distributed Flight)",
                                node_id,
                                flight_url
                            );
                            return Some(url);
                        }
                        Err(e) => {
                            log::warn!(
                                "❌ [ChannelResolver] Failed to parse URL {}: {}",
                                flight_url,
                                e
                            );
                        }
                    }
                } else {
                    log::warn!("❌ [ChannelResolver] Failed to parse node_id: {}", node_id);
                }
                None
            })
            .collect();

        if node_urls.is_empty() {
            return Err(crate::utils::error::CoreError::Internal(format!(
                "No valid Flight endpoints found for table '{}'",
                table_name
            )));
        }

        Ok(Self {
            node_urls: Arc::new(node_urls),
            cached_clients: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// 创建分布式模式的 resolver（从 ClusterManager 获取所有节点）
    ///
    /// **注意**：这种方式不推荐，因为返回的是"所有节点"而不是"拥有数据的节点"
    /// 仅用于不确定表名的场景
    pub async fn new_distributed(cluster_manager: Arc<ClusterManager>) -> Self {
        // 从 ClusterManager 获取活跃节点
        let node_ids = cluster_manager.live_nodes().await;

        // 将节点 ID 转换为 Arrow Flight URL
        let node_urls: Vec<Url> = node_ids
            .iter()
            .map(|node_id| {
                Url::parse(&format!("http://{}:50051", node_id))
                    .unwrap_or_else(|_| Url::parse("http://localhost:50051").unwrap())
            })
            .collect();

        log::info!(
            "🌐 [ChannelResolver] Initialized with {} cluster nodes (all nodes mode)",
            node_urls.len()
        );

        Self {
            node_urls: Arc::new(node_urls),
            cached_clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 创建单机模式的 resolver（用于测试）
    pub fn new_local(ports: Vec<u16>) -> Self {
        let node_urls: Vec<Url> = ports
            .iter()
            .map(|port| {
                Url::parse(&format!("http://localhost:{}", port))
                    .expect("Failed to parse localhost URL")
            })
            .collect();

        log::info!(
            "🏠 [ChannelResolver] Initialized in local mode with {} ports",
            ports.len()
        );

        Self {
            node_urls: Arc::new(node_urls),
            cached_clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ChannelResolver for CalmChannelResolver {
    /// 返回缓存的节点 URL 列表（同步方法）
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        // 直接返回缓存的 URLs（在创建时已经获取）
        Ok((*self.node_urls).clone())
    }

    async fn get_flight_client_for_url(
        &self,
        url: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        let url_str = url.to_string();

        // 检查缓存
        {
            let cache = self.cached_clients.read().await;
            if let Some(client) = cache.get(&url_str) {
                log::debug!("📡 [ChannelResolver] Reusing cached client for {}", url_str);
                return Ok(client.clone());
            }
        }

        // 建立新连接
        log::info!(
            "📡 [ChannelResolver] Connecting to Arrow Flight endpoint: {}",
            url_str
        );

        let channel = Channel::from_shared(url_str.clone())
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .connect_lazy();

        let client = FlightServiceClient::new(BoxCloneSyncChannel::new(channel));

        // 缓存连接
        {
            let mut cache = self.cached_clients.write().await;
            cache.insert(url_str.clone(), client.clone());
        }

        log::info!("✅ [ChannelResolver] Connected to {}", url_str);
        Ok(client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_resolver() {
        let resolver = CalmChannelResolver::new_local(vec![8001, 8002, 8003]);
        let urls = resolver.get_urls().unwrap();

        assert_eq!(urls.len(), 3);
        assert_eq!(urls[0].as_str(), "http://localhost:8001/");
        assert_eq!(urls[1].as_str(), "http://localhost:8002/");
        assert_eq!(urls[2].as_str(), "http://localhost:8003/");
    }
}
