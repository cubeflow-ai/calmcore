use std::sync::Arc;

use tarpc::context::Context;
use tokio_serde::formats::Bincode;

use crate::{
    catalog::{Catalog, PartitionStrategy, TableMeta},
    cluster::ClusterManager,
    schema::Schema,
    utils::error::{CoreError, CoreResult},
};

/// DDL Service - 使用 tarpc 提供集群内部 RPC 服务
///
/// 职责：
/// 1. 如果是中央节点：执行 DDL → 写共享存储 → 分配分区 → Gossip 广播
/// 2. 如果不是中央节点：转发到中央节点
#[tarpc::service]
pub trait DDLService {
    /// 创建表
    async fn create_table(
        schema: Schema,
        partition_strategy: PartitionStrategy,
    ) -> Result<(), CoreError>;

    /// 删除表
    async fn drop_table(table_name: String) -> Result<(), CoreError>;

    /// 获取表元数据
    async fn get_table_meta(table_name: String) -> Result<TableMeta, CoreError>;

    /// 列出所有表
    async fn list_tables() -> Vec<String>;
}

/// DDL Service 实现
#[derive(Clone)]
pub struct DDLServiceImpl {
    catalog: Arc<Catalog>,
    cluster_manager: Arc<ClusterManager>,
}

impl DDLServiceImpl {
    pub fn new(catalog: Arc<Catalog>, cluster_manager: Arc<ClusterManager>) -> Self {
        Self {
            catalog,
            cluster_manager,
        }
    }

    /// 判断当前节点是否是中央节点
    async fn am_i_center_node(&self) -> bool {
        let center_node = self.cluster_manager.node_manager.get_center_node().await;
        match center_node {
            Some(center) => center.node_id == self.cluster_manager.node_id(),
            None => {
                log::warn!("⚠️  No center node elected yet");
                false
            }
        }
    }

    /// 作为协调者创建表
    async fn create_table_as_coordinator(
        &self,
        schema: Schema,
        partition_strategy: PartitionStrategy,
    ) -> CoreResult<()> {
        let table_name = schema.name.clone();

        // 1. 构建 TableMeta
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let table_meta = TableMeta {
            table_name: table_name.clone(),
            schema: schema.clone(),
            partition_strategy: partition_strategy.clone(),
            created_at: now,
            updated_at: now,
        };

        // 2. 创建表（会写入共享存储 data/tables/{name}/meta.json）
        let partitions = self.catalog.create_table(table_meta)?;

        // 3. 分配分区到各个节点
        for partition_name in &partitions {
            self.cluster_manager
                .call_create_partition(&table_name, partition_name)
                .await?;
        }

        // 4. 通过 Gossip 广播缓存失效消息
        self.cluster_manager
            .gossip_set(&format!("table_invalidate:{}", table_name), "1")
            .await;

        log::info!("✅ [CoordNode] Table '{}' created successfully", table_name);
        Ok(())
    }

    /// 作为协调者删除表
    async fn drop_table_as_coordinator(&self, table_name: &str) -> CoreResult<()> {
        // 1. 获取表信息
        let table_meta = self.catalog.get_table(table_name)?;
        let partitions = table_meta
            .partition_strategy
            .generate_partitions()
            .unwrap_or_default();

        // 2. 通知所有节点删除分区
        for partition_name in &partitions {
            // 获取分区所在的节点
            if let Some(node_id) = self.catalog.get_partition_owner(table_name, partition_name) {
                self.cluster_manager
                    .call_drop_partition(&node_id, table_name, partition_name)
                    .await?;
            }
        }

        // 3. 删除表元数据
        self.catalog.drop_table(table_name)?;

        // 4. 通过 Gossip 广播缓存失效消息
        self.cluster_manager
            .gossip_set(&format!("table_invalidate:{}", table_name), "1")
            .await;

        log::info!("✅ [CoordNode] Table '{}' dropped successfully", table_name);
        Ok(())
    }

    /// 转发到中央节点执行 create_table
    async fn forward_create_table_to_coordinator(
        &self,
        schema: Schema,
        partition_strategy: PartitionStrategy,
    ) -> CoreResult<()> {
        // 获取中央节点
        let coord_node = self
            .cluster_manager
            .node_manager
            .get_center_node()
            .await
            .ok_or_else(|| CoreError::Internal("No coordinator elected".to_string()))?;

        // 获取中央节点地址
        let coord_addr = self
            .cluster_manager
            .node_manager
            .node_internal_addr(&coord_node)
            .await?;

        log::info!(
            "📤 [Node] Forwarding create_table '{}' to coordinator {}",
            schema.name,
            coord_addr
        );

        // 连接中央节点的 tarpc 服务
        let transport = tarpc::serde_transport::tcp::connect(coord_addr, Bincode::default)
            .await
            .map_err(|e| CoreError::Network(format!("Failed to connect to coordinator: {}", e)))?;

        let client = DDLServiceClient::new(tarpc::client::Config::default(), transport).spawn();

        // 调用中央节点的 create_table
        client
            .create_table(tarpc::context::current(), schema, partition_strategy)
            .await
            .map_err(|e| CoreError::Network(format!("RPC call failed: {}", e)))??;

        Ok(())
    }

    /// 转发到中央节点执行 drop_table
    async fn forward_drop_table_to_coordinator(&self, table_name: String) -> CoreResult<()> {
        // 获取中央节点
        let coord_node = self
            .cluster_manager
            .node_manager
            .get_center_node()
            .await
            .ok_or_else(|| CoreError::Internal("No coordinator elected".to_string()))?;

        // 获取中央节点地址
        let coord_addr = self
            .cluster_manager
            .node_manager
            .node_internal_addr(&coord_node)
            .await?;

        log::info!(
            "📤 [Node] Forwarding drop_table '{}' to coordinator {}",
            table_name,
            coord_addr
        );

        // 连接中央节点的 tarpc 服务
        let transport = tarpc::serde_transport::tcp::connect(coord_addr, Bincode::default)
            .await
            .map_err(|e| CoreError::Network(format!("Failed to connect to coordinator: {}", e)))?;

        let client = DDLServiceClient::new(tarpc::client::Config::default(), transport).spawn();

        // 调用中央节点的 drop_table
        client
            .drop_table(tarpc::context::current(), table_name)
            .await
            .map_err(|e| CoreError::Network(format!("RPC call failed: {}", e)))??;

        Ok(())
    }
}

/// tarpc service 实现
impl DDLService for DDLServiceImpl {
    async fn create_table(
        self,
        _ctx: Context,
        schema: Schema,
        partition_strategy: PartitionStrategy,
    ) -> Result<(), CoreError> {
        if self.am_i_center_node().await {
            log::info!("📋 [CoordNode] Creating table '{}'", schema.name);
            self.create_table_as_coordinator(schema, partition_strategy)
                .await
        } else {
            log::info!(
                "📤 [Node] Forwarding create_table '{}' to coordinator",
                schema.name
            );
            self.forward_create_table_to_coordinator(schema, partition_strategy)
                .await
        }
    }

    async fn drop_table(self, _ctx: Context, table_name: String) -> Result<(), CoreError> {
        if self.am_i_center_node().await {
            log::info!("📋 [CoordNode] Dropping table '{}'", table_name);
            self.drop_table_as_coordinator(&table_name).await
        } else {
            log::info!(
                "📤 [Node] Forwarding drop_table '{}' to coordinator",
                table_name
            );
            self.forward_drop_table_to_coordinator(table_name).await
        }
    }

    async fn get_table_meta(
        self,
        _ctx: Context,
        table_name: String,
    ) -> Result<TableMeta, CoreError> {
        self.catalog
            .get_table(&table_name)
            .map(|arc| (*arc).clone())
    }

    async fn list_tables(self, _ctx: Context) -> Vec<String> {
        self.catalog.list_tables()
    }
}
