use std::sync::Arc;

use base64::engine;
use datafusion::arrow::json;
use ddl_macros::coordinator_route;
use rayon::vec;
use serde::{Deserialize, Serialize};
use tarpc::{client, context::Context};
use tokio_serde::formats::Bincode;

use crate::{
    catalog::{table_meta, Catalog, PartitionStrategy, TableMeta},
    cluster::{keys, ClusterManager},
    engine::Engine,
    schema::Schema,
    utils::error::{CoreError, CoreResult},
};

/// 分区详细信息（用于 RPC 传输）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionDetail {
    pub partition_id: String,
    pub segments: Vec<SegmentDetail>,
}

/// Segment 详细信息（用于 RPC 传输）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentDetail {
    pub segment_id: u64,
    pub doc_count: u32,
    pub deleted_count: u64,
    pub is_persisted: bool,
    pub base_path: Option<String>,
    pub is_external_reference: bool,
    pub external_data_path: Option<String>,
}

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
    async fn list_tables() -> Result<Vec<String>, CoreError>;

    /// 列出表的所有分区
    async fn list_partitions(table_name: String) -> Result<Vec<String>, CoreError>;

    /// 获取分区详细信息（包括 segments）
    async fn get_partition_detail(
        table_name: String,
        partition_id: String,
    ) -> Result<PartitionDetail, CoreError>;

    /// 本地删除分区数据（由所有者节点调用）
    async fn drop_partition_local(
        table_name: String,
        partition_id: String,
    ) -> Result<(), CoreError>;

    /// 持久化表（刷新所有分区到磁盘）
    async fn flush_table(table_name: String) -> Result<(), CoreError>;

    /// 本地持久化分区数据（由所有者节点调用）
    async fn flush_partition_local(
        table_name: String,
        partition_id: String,
    ) -> Result<(), CoreError>;

    /// 查询当前节点是否是中央节点
    async fn is_coordinator() -> bool;
}

/// DDL Service 实现
#[derive(Clone)]
pub struct DDLServiceImpl {
    engine: Arc<Engine>,
    catalog: Arc<Catalog>,
    cluster_manager: Option<Arc<ClusterManager>>,
}

impl DDLServiceImpl {
    pub fn new(
        engine: Arc<Engine>,
        catalog: Arc<Catalog>,
        cluster_manager: Option<Arc<ClusterManager>>,
    ) -> Self {
        Self {
            engine,
            catalog,
            cluster_manager,
        }
    }

    /// 判断当前节点是否是中央节点
    fn am_i_coord_node(&self) -> bool {
        self.cluster_manager
            .as_ref()
            .map(|cm| cm.am_i_coord_node())
            .unwrap_or(true)
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

        let partitions = self.catalog.create_table(table_meta).await?;

        match &self.cluster_manager {
            Some(cm) => {
                log::info!(
                    "📋 [CoordNode] Created table '{}' with {} partitions",
                    table_name,
                    partitions.len()
                );
                // publish table info to cluster
                cm.put_table(&table_name).await;

                let idle_nodes = cm.idle_nodes().await;

                if idle_nodes.is_empty() {
                    let msg = format!(
                        "⚠️  No idle nodes available to assign partitions for table '{}'",
                        table_name
                    );
                    log::error!("{}", msg);
                    return Err(CoreError::ClusterState(msg));
                }
            }
            None => {
                log::info!(
                    "📋 [Standalone] Created table '{}' with {} partitions",
                    table_name,
                    partitions.len()
                );
            }
        }

        if let Some(cm) = &self.cluster_manager {
            cm.set_key_value(format!("table:{}", table_name), table_meta_value)
                .await;
        }

        for partition_name in partitions {
            match self.cluster_manager.as_ref() {
                Some(cm) => {
                    let idle_nodes = cm.node_manager.idle_nodes().await;
                    if idle_nodes.is_empty() {
                        log::warn!(
                            "⚠️  No idle nodes available to assign partition '{}/{}'",
                            table_name,
                            partition_name
                        );
                        continue;
                    }
                    let selected_node = &idle_nodes[0];
                    self.catalog
                        .set_partition_owner(&table_name, &partition_name, &selected_node)
                        .map_err(|e| {
                            CoreError::Internal(format!(
                                "Failed to set partition owner for '{}/{}': {}",
                                table_name, partition_name, e
                            ))
                        })?;
                    log::info!(
                        "✅ Assigned partition '{}/{}' to node '{}'",
                        table_name,
                        partition_name,
                        selected_node
                    );
                }
                None => {
                    log::info!(
                        "ℹ️  Standalone mode, skipping partition assignment for '{}/{}'",
                        table_name,
                        partition_name
                    );
                }
            }
            // 选择 最空闲节点，创建partition
            self.cluster_manager
                .and_then(|cm| cm.node_manager.idle_nodes())
        }

        log::info!("✅ [CoordNode] Table '{}' created successfully", table_name);
        Ok(())
    }

    /// 作为协调者删除表
    async fn drop_table_as_coordinator(&self, table_name: &str) -> CoreResult<()> {
        log::info!("🗑️  [CoordNode] Starting drop table '{}'", table_name);

        // 1. 获取表的所有分区
        let partitions = match self.catalog.get_partition_names(table_name).await {
            Ok(parts) => parts,
            Err(e) => {
                log::warn!(
                    "⚠️  Table '{}' not found or has no partitions: {}",
                    table_name,
                    e
                );
                // 即使表不存在，也尝试删除（可能只是缓存问题）
                vec![]
            }
        };

        log::info!(
            "📋 [CoordNode] Table '{}' has {} partitions",
            table_name,
            partitions.len()
        );

        // 2. 收集每个节点需要删除的分区
        let mut node_partitions: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        for partition_id in &partitions {
            if let Some(owner) = self
                .catalog
                .get_partition_owner(table_name, partition_id)
                .await
            {
                node_partitions
                    .entry(owner)
                    .or_insert_with(Vec::new)
                    .push(partition_id.clone());
            }
        }

        log::info!(
            "📋 [CoordNode] Partitions distributed across {} nodes",
            node_partitions.len()
        );

        // 3. 并发通知所有节点删除本地分区
        let mut tasks = Vec::new();
        for (node_id, partition_ids) in node_partitions {
            let self_clone = self.clone();
            let table_name_clone = table_name.to_string();
            let node_id_clone = node_id.clone();

            tasks.push(tokio::spawn(async move {
                log::info!(
                    "📤 [CoordNode] Notifying node '{}' to drop {} partitions",
                    node_id_clone,
                    partition_ids.len()
                );

                for partition_id in partition_ids {
                    // 如果是本节点，直接删除
                    let is_local = self_clone
                        .cluster_manager
                        .as_ref()
                        .map(|cm| node_id_clone == cm.node_id())
                        .unwrap_or(true);

                    if is_local {
                        if let Err(e) = self_clone
                            .drop_partition_local_impl(&table_name_clone, &partition_id)
                            .await
                        {
                            log::error!(
                                "❌ Failed to drop local partition '{}/{}': {}",
                                table_name_clone,
                                partition_id,
                                e
                            );
                        }
                    } else {
                        // 否则通过 RPC 通知所有者节点删除
                        if let Err(e) = self_clone
                            .notify_node_drop_partition(
                                &node_id_clone,
                                table_name_clone.clone(),
                                partition_id.clone(),
                            )
                            .await
                        {
                            log::error!(
                                "❌ Failed to notify node '{}' to drop partition '{}/{}': {}",
                                node_id_clone,
                                table_name_clone,
                                partition_id,
                                e
                            );
                        }
                    }
                }
            }));
        }

        // 等待所有删除任务完成
        for task in tasks {
            let _ = task.await;
        }

        // 4. 删除表元数据（从 catalog 和共享存储）
        if let Err(e) = self.catalog.drop_table(table_name).await {
            log::error!("❌ Failed to drop table metadata: {}", e);
            return Err(e);
        }

        log::info!("✅ [CoordNode] Table '{}' dropped successfully", table_name);
        Ok(())
    }

    /// 本地删除分区数据（内部实现）
    async fn drop_partition_local_impl(
        &self,
        table_name: &str,
        partition_id: &str,
    ) -> CoreResult<()> {
        log::info!(
            "🗑️  [Local] Dropping partition '{}/{}'",
            table_name,
            partition_id
        );

        // 从 engine 中移除分区
        self.engine.remove_partition(table_name, partition_id).await;

        log::info!(
            "✅ [Local] Partition '{}/{}' dropped",
            table_name,
            partition_id
        );
        Ok(())
    }

    /// 通知指定节点删除分区
    async fn notify_node_drop_partition(
        &self,
        node_id: &str,
        table_name: String,
        partition_id: String,
    ) -> CoreResult<()> {
        let node_addr = self.internal_addr(node_id).await?;

        log::info!(
            "📤 [CoordNode] Notifying node '{}' ({}) to drop partition '{}/{}'",
            node_id,
            node_addr,
            table_name,
            partition_id
        );

        let transport = tarpc::serde_transport::tcp::connect(node_addr, || Bincode::default())
            .await
            .map_err(|e| CoreError::Network(format!("Failed to connect to node: {}", e)))?;

        let client = DDLServiceClient::new(tarpc::client::Config::default(), transport).spawn();

        client
            .drop_partition_local(tarpc::context::current(), table_name, partition_id)
            .await
            .map_err(|e| CoreError::Network(format!("RPC call failed: {}", e)))?
    }

    /// 中央节点执行：持久化表的所有分区
    async fn flush_table_as_coordinator(&self, table_name: &str) -> CoreResult<()> {
        log::info!("💾 [CoordNode] Starting flush table '{}'", table_name);

        // 1. 获取表的所有分区
        let partitions = self.catalog.get_partition_names(table_name).await?;

        log::info!(
            "📋 [CoordNode] Table '{}' has {} partitions to flush",
            table_name,
            partitions.len()
        );

        // 2. 收集每个节点需要 flush 的分区
        let mut node_partitions: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        for partition_id in &partitions {
            if let Some(owner) = self
                .catalog
                .get_partition_owner(table_name, partition_id)
                .await
            {
                node_partitions
                    .entry(owner)
                    .or_insert_with(Vec::new)
                    .push(partition_id.clone());
            }
        }

        log::info!(
            "📋 [CoordNode] Partitions distributed across {} nodes",
            node_partitions.len()
        );

        // 3. 并发通知所有节点 flush 本地分区
        let mut tasks = Vec::new();
        for (node_id, partition_ids) in node_partitions {
            let self_clone = self.clone();
            let table_name_clone = table_name.to_string();
            let node_id_clone = node_id.clone();

            tasks.push(tokio::spawn(async move {
                log::info!(
                    "📤 [CoordNode] Notifying node '{}' to flush {} partitions",
                    node_id_clone,
                    partition_ids.len()
                );

                for partition_id in partition_ids {
                    // 如果是本节点，直接 flush
                    let is_local = self_clone
                        .cluster_manager
                        .as_ref()
                        .map(|cm| node_id_clone == cm.node_id())
                        .unwrap_or(true);

                    if is_local {
                        if let Err(e) = self_clone
                            .flush_partition_local_impl(&table_name_clone, &partition_id)
                            .await
                        {
                            log::error!(
                                "❌ Failed to flush local partition '{}/{}': {}",
                                table_name_clone,
                                partition_id,
                                e
                            );
                        }
                    } else {
                        // 否则通过 RPC 通知所有者节点 flush
                        if let Err(e) = self_clone
                            .notify_node_flush_partition(
                                &node_id_clone,
                                table_name_clone.clone(),
                                partition_id.clone(),
                            )
                            .await
                        {
                            log::error!(
                                "❌ Failed to notify node '{}' to flush partition '{}/{}': {}",
                                node_id_clone,
                                table_name_clone,
                                partition_id,
                                e
                            );
                        }
                    }
                }
            }));
        }

        // 等待所有 flush 任务完成
        for task in tasks {
            let _ = task.await;
        }

        log::info!("✅ [CoordNode] Table '{}' flushed successfully", table_name);
        Ok(())
    }

    /// 本地持久化分区数据（内部实现）
    async fn flush_partition_local_impl(
        &self,
        table_name: &str,
        partition_id: &str,
    ) -> CoreResult<()> {
        log::info!(
            "💾 [Local] Flushing partition '{}/{}'",
            table_name,
            partition_id
        );

        // 获取分区并执行 flush
        if let Some(partition) = self.engine.get_partition(table_name, partition_id).await {
            partition
                .persist_all()
                .map_err(|e| CoreError::Internal(format!("Failed to flush partition: {}", e)))?;
            log::info!(
                "✅ [Local] Partition '{}/{}' flushed",
                table_name,
                partition_id
            );
        } else {
            log::warn!(
                "⚠️  Partition '{}/{}' not found on this node",
                table_name,
                partition_id
            );
        }

        Ok(())
    }

    /// 通知指定节点 flush 分区
    async fn notify_node_flush_partition(
        &self,
        node_id: &str,
        table_name: String,
        partition_id: String,
    ) -> CoreResult<()> {
        let (_, node_addr) = keys::parse_node_id(node_id)?;

        log::info!(
            "📤 [CoordNode] Notifying node '{}' ({}) to flush partition '{}/{}'",
            node_id,
            node_addr,
            table_name,
            partition_id
        );

        let transport = tarpc::serde_transport::tcp::connect(node_addr, || Bincode::default())
            .await
            .map_err(|e| CoreError::Network(format!("Failed to connect to node: {}", e)))?;

        let client = DDLServiceClient::new(tarpc::client::Config::default(), transport).spawn();

        client
            .flush_partition_local(tarpc::context::current(), table_name, partition_id)
            .await
            .map_err(|e| CoreError::Network(format!("RPC call failed: {}", e)))?
    }

    /// 转发到中央节点执行 create_table
    async fn forward_create_table_to_coordinator(
        &self,
        schema: Schema,
        partition_strategy: PartitionStrategy,
    ) -> CoreResult<()> {
        // 获取中央节点
        let coord_addr = self.coord_addr().await?;

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

    async fn new_client(&self, node: &str) -> CoreResult<DDLServiceClient> {
        let (_, addr) = keys::parse_node_id(node)?;

        log::info!("📤 [Node] New client to node {} at {}", node, addr);

        let transport = tarpc::serde_transport::tcp::connect(addr, Bincode::default)
            .await
            .map_err(|e| CoreError::Network(format!("Failed to connect to node: {}", e)))?;

        Ok(DDLServiceClient::new(tarpc::client::Config::default(), transport).spawn())
    }

    /// 转发到中央节点执行 drop_table
    async fn forward_drop_table_to_coordinator(&self, table_name: String) -> CoreResult<()> {
        // 获取中央节点地址
        let coord_addr = self.coord_addr().await?;
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

    async fn coord_client(&self) -> CoreResult<DDLServiceClient> {
        let coord_addr = self.coord_addr().await?;
        log::info!("📤 [Node] New client to coordinator {}", coord_addr);

        let transport = tarpc::serde_transport::tcp::connect(coord_addr, Bincode::default)
            .await
            .map_err(|e| CoreError::Network(format!("Failed to connect to coordinator: {}", e)))?;

        Ok(DDLServiceClient::new(tarpc::client::Config::default(), transport).spawn())
    }

    async fn coord_addr(&self) -> CoreResult<String> {
        let cm = self
            .cluster_manager
            .as_ref()
            .ok_or_else(|| CoreError::Internal("Not in cluster mode".to_string()))?;
        let node_manager = &cm.node_manager;

        if let Some(coord_addr) = node_manager.get_coord_node_addr().await {
            return Ok(coord_addr);
        }

        // 如果没有中央节点, 则进行一次简单选举寻找中央结点

        match node_manager.run_election().await {
            Ok(id) => node_manager.set_coord_node(&id).await?,
            Err(ids) => {
                // 去每个节点rpc询问它是不是中央节点
                for node_id in ids {
                    if let Ok(addr) = self.internal_addr(&node_id).await {
                        log::info!(
                            "🔍 Checking if node '{}' ({}) is coordinator",
                            node_id,
                            addr
                        );

                        match tarpc::serde_transport::tcp::connect(&addr, || Bincode::default())
                            .await
                        {
                            Ok(transport) => {
                                let client = DDLServiceClient::new(
                                    tarpc::client::Config::default(),
                                    transport,
                                )
                                .spawn();

                                match client.is_coordinator(tarpc::context::current()).await {
                                    Ok(true) => {
                                        log::info!("✅ Found coordinator: {}", node_id);
                                        node_manager.set_coord_node(&node_id).await?;
                                        break;
                                    }
                                    Ok(false) => {
                                        log::info!("❌ Node '{}' is not coordinator", node_id);
                                    }
                                    Err(e) => {
                                        log::warn!("⚠️ Failed to query node '{}': {}", node_id, e);
                                    }
                                }
                            }
                            Err(e) => {
                                log::warn!("⚠️ Failed to connect to node '{}': {}", node_id, e);
                            }
                        }
                    }
                }
            }
        }

        node_manager.get_coord_node_addr().await.ok_or_else(|| {
            CoreError::Internal(
                "Failed to determine coordinator address after election".to_string(),
            )
        })
    }

    /// 中央节点执行 list_tables
    async fn list_tables_as_coordinator(&self) -> CoreResult<Vec<String>> {
        log::info!("📋 [CoordNode] Listing tables");
        Ok(self.catalog.list_tables())
    }

    /// 中央节点执行 get_table_meta
    async fn get_table_meta_as_coordinator(&self, table_name: &str) -> CoreResult<TableMeta> {
        log::info!("📋 [CoordNode] Getting table meta for '{}'", table_name);
        self.catalog.get_table(table_name).map(|arc| (*arc).clone())
    }

    /// 中央节点执行 list_partitions
    async fn list_partitions_as_coordinator(&self, table_name: &str) -> CoreResult<Vec<String>> {
        log::info!(
            "📋 [CoordNode] Listing partitions for table '{}'",
            table_name
        );
        self.catalog.get_partition_names(table_name).await
    }

    /// 转发到中央节点执行 get_table_meta
    async fn forward_get_table_meta_to_coordinator(
        &self,
        table_name: String,
    ) -> CoreResult<TableMeta> {
        self.coord_client()
            .await?
            .get_table_meta(tarpc::context::current(), table_name)
            .await
            .map_err(|e| CoreError::Network(format!("RPC call failed: {}", e)))?
    }

    /// 本地节点执行：获取分区详细信息
    async fn get_partition_detail_local(
        &self,
        table_name: &str,
        partition_id: &str,
    ) -> CoreResult<PartitionDetail> {
        let partition = self
            .engine
            .get_partition(table_name, partition_id)
            .await
            .ok_or_else(|| {
                CoreError::NotExisted(format!(
                    "Partition '{}' in table '{}' not found",
                    partition_id, table_name
                ))
            })?;

        let mut segments = Vec::new();

        // 获取 frozen segments
        let frozen_segments = partition.get_frozen_segments();
        for (seg_id, segment) in frozen_segments.iter() {
            segments.push(SegmentDetail {
                segment_id: *seg_id,
                doc_count: segment.doc_count(),
                deleted_count: segment.deleted_count(),
                is_persisted: segment.is_persisted(),
                base_path: segment.base_path(),
                is_external_reference: segment.is_external_reference(),
                external_data_path: segment.get_external_data_path(),
            });
        }
        drop(frozen_segments);

        // 获取当前 segment
        let current_segment = partition.get_current_segment();
        segments.push(SegmentDetail {
            segment_id: 0,
            doc_count: current_segment.doc_count(),
            deleted_count: current_segment.deleted_count(),
            is_persisted: current_segment.is_persisted(),
            base_path: current_segment.base_path(),
            is_external_reference: false,
            external_data_path: None,
        });

        Ok(PartitionDetail {
            partition_id: partition_id.to_string(),
            segments,
        })
    }

    /// 中央节点执行：根据路由信息获取分区详情
    async fn get_partition_detail_as_coordinator(
        &self,
        table_name: &str,
        partition_id: &str,
    ) -> CoreResult<PartitionDetail> {
        // 获取分区所有者
        let owner_node_id = self
            .catalog
            .get_partition_owner(table_name, partition_id)
            .await
            .ok_or_else(|| {
                CoreError::Internal(format!(
                    "Partition '{}' in table '{}' has no owner",
                    partition_id, table_name
                ))
            })?;

        log::info!(
            "📋 [CoordNode] Partition '{}' is owned by node '{}'",
            partition_id,
            owner_node_id
        );

        // 如果是本节点或单机模式，直接获取
        let is_local = self
            .cluster_manager
            .as_ref()
            .map(|cm| owner_node_id == cm.node_id())
            .unwrap_or(true);

        if is_local {
            return self
                .get_partition_detail_local(table_name, partition_id)
                .await;
        }

        // 否则，转发到所有者节点
        let cm = self
            .cluster_manager
            .as_ref()
            .ok_or_else(|| CoreError::Internal("Not in cluster mode".to_string()))?;
        let owner_addr = cm
            .node_manager
            .get_node_internal_addr(&owner_node_id)
            .await
            .ok_or_else(|| {
                CoreError::Internal(format!("Node '{}' address not found", owner_node_id))
            })?;

        log::info!(
            "📤 [CoordNode] Forwarding get_partition_detail to owner node {}",
            owner_addr
        );

        let transport = tarpc::serde_transport::tcp::connect(owner_addr, || Bincode::default())
            .await
            .map_err(|e| CoreError::Network(format!("Failed to connect to owner node: {}", e)))?;

        let client = DDLServiceClient::new(tarpc::client::Config::default(), transport).spawn();

        client
            .get_partition_detail(
                tarpc::context::current(),
                table_name.to_string(),
                partition_id.to_string(),
            )
            .await
            .map_err(|e| CoreError::Network(format!("RPC call failed: {}", e)))?
    }

    /// 转发到中央节点
    async fn forward_get_partition_detail_to_coordinator(
        &self,
        table_name: String,
        partition_id: String,
    ) -> CoreResult<PartitionDetail> {
        log::info!("📤 [Node] Forwarding get_partition_detail to coordinator");

        self.coord_client()
            .await?
            .get_partition_detail(tarpc::context::current(), table_name, partition_id)
            .await
            .map_err(|e| CoreError::Network(format!("RPC call failed: {}", e)))?
    }
}

/// tarpc service 实现
impl DDLService for DDLServiceImpl {
    #[coordinator_route]
    async fn create_table(
        self,
        _ctx: Context,
        schema: Schema,
        partition_strategy: PartitionStrategy,
    ) -> Result<(), CoreError> {
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

        let table_info = self.catalog.create_table(table_meta).await?;

        let partitions = table_info
            .table
            .partition_strategy
            .generate_partitions()
            .unwrap_or_else(Vec::new);

        let idle_nodes = match self.cluster_manager.as_ref() {
            Some(cm) => cm.idle_nodes().await,
            None => vec![keys::SINGLE_NODE_CLUSTER_ID.to_string()],
        };

        let mut start = 0;

        for partition_name in &partitions {
            // 创建 partition 目录和元数据

            let node = idle_nodes.get(start % idle_nodes.len()).unwrap();

            self.new_client(node)
                .await?
                .create_partition(
                    tarpc::context::current(),
                    table_name.clone(),
                    partition_name.clone(),
                )
                .await?;
        }

        // // 添加到缓存
        // {
        //     let mut partitions_map: HashMap<String, PartitionMeta> = HashMap::new();
        //     for name in &partitions {
        //         let partition_meta = match self.load_partition_meta(&table_name, name).await {
        //             Ok(meta) => meta,
        //             Err(_) => PartitionMeta::new(name.clone()),
        //         };
        //         partitions_map.insert(name.clone(), partition_meta);
        //     }

        //     let table_info = TableInfo {
        //         table: meta,
        //         partitions: RwLock::new(partitions_map),
        //     };
        //     let mut tables = self.tables.write().await;
        //     tables.insert(table_name.clone(), Arc::new(table_info));
        // }

        match &self.cluster_manager {
            Some(cm) => {
                log::info!(
                    "📋 [CoordNode] Created table '{}' with {} partitions",
                    table_name,
                    partitions.len()
                );

                let idle_nodes = cm.idle_nodes().await;

                if idle_nodes.is_empty() {
                    let msg = format!(
                        "⚠️  No idle nodes available to assign partitions for table '{}'",
                        table_name
                    );
                    log::error!("{}", msg);
                    return Err(CoreError::ClusterState(msg));
                }
            }
            None => {
                log::info!(
                    "📋 [Standalone] Created table '{}' with {} partitions",
                    table_name,
                    partitions.len()
                );
            }
        }

        for partition_name in partitions {
            match self.cluster_manager.as_ref() {
                Some(cm) => {
                    let idle_nodes = cm.idle_nodes().await;
                    if idle_nodes.is_empty() {
                        log::warn!(
                            "⚠️  No idle nodes available to assign partition '{}/{}'",
                            table_name,
                            partition_name
                        );
                        continue;
                    }
                    let selected_node = &idle_nodes[0];
                    self.catalog
                        .set_partition_owner(&table_name, &partition_name, &selected_node)
                        .map_err(|e| {
                            CoreError::Internal(format!(
                                "Failed to set partition owner for '{}/{}': {}",
                                table_name, partition_name, e
                            ))
                        })?;
                    log::info!(
                        "✅ Assigned partition '{}/{}' to node '{}'",
                        table_name,
                        partition_name,
                        selected_node
                    );
                }
                None => {
                    log::info!(
                        "ℹ️  Standalone mode, skipping partition assignment for '{}/{}'",
                        table_name,
                        partition_name
                    );
                }
            }
        }

        log::info!("✅ Table '{}' created successfully", table_name);
        Ok(())
    }

    async fn drop_table(self, _ctx: Context, table_name: String) -> Result<(), CoreError> {
        if self.am_i_coord_node() {
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
        if self.am_i_coord_node() {
            log::info!("📋 [CoordNode] Getting table meta for '{}'", table_name);
            self.get_table_meta_as_coordinator(&table_name).await
        } else {
            log::info!(
                "📤 [Node] Forwarding get_table_meta '{}' to coordinator",
                table_name
            );
            self.forward_get_table_meta_to_coordinator(table_name).await
        }
    }

    async fn list_tables(self, _ctx: Context) -> CoreResult<Vec<String>> {
        if self.am_i_coord_node() {
            log::info!("📋 [CoordNode] Listing tables");
            self.list_tables_as_coordinator().await
        } else {
            log::info!("📤 [Node] Forwarding list_tables to coordinator");
            self.coord_client()
                .await?
                .list_tables(tarpc::context::current())
                .await
                .map_err(|e| CoreError::Network(format!("RPC call failed: {}", e)))?
        }
    }

    async fn list_partitions(
        self,
        _ctx: Context,
        table_name: String,
    ) -> Result<Vec<String>, CoreError> {
        if self.am_i_coord_node() {
            log::info!(
                "📋 [CoordNode] Listing partitions for table '{}'",
                table_name
            );
            self.list_partitions_as_coordinator(&table_name).await
        } else {
            log::info!(
                "📤 [Node] Forwarding list_partitions '{}' to coordinator",
                table_name
            );
            self.coord_client()
                .await?
                .list_partitions(tarpc::context::current(), table_name)
                .await?
        }
    }

    async fn get_partition_detail(
        self,
        _ctx: Context,
        table_name: String,
        partition_id: String,
    ) -> Result<PartitionDetail, CoreError> {
        if self.am_i_coord_node() {
            log::info!(
                "📋 [CoordNode] Getting partition detail for '{}/{}' (will route to owner node)",
                table_name,
                partition_id
            );
            self.get_partition_detail_as_coordinator(&table_name, &partition_id)
                .await
        } else {
            log::info!(
                "📤 [Node] Forwarding get_partition_detail '{}/{}' to coordinator",
                table_name,
                partition_id
            );
            self.forward_get_partition_detail_to_coordinator(table_name, partition_id)
                .await
        }
    }

    async fn drop_partition_local(
        self,
        _ctx: Context,
        table_name: String,
        partition_id: String,
    ) -> Result<(), CoreError> {
        log::info!(
            "🗑️  [Node] Dropping local partition '{}/{}'",
            table_name,
            partition_id
        );
        self.drop_partition_local_impl(&table_name, &partition_id)
            .await
    }

    async fn flush_table(self, _ctx: Context, table_name: String) -> Result<(), CoreError> {
        if self.am_i_coord_node() {
            log::info!("📋 [CoordNode] Flushing table '{}'", table_name);
            self.flush_table_as_coordinator(&table_name).await
        } else {
            log::info!(
                "📤 [Node] Forwarding flush_table '{}' to coordinator",
                table_name
            );
            self.coord_client()
                .await?
                .flush_table(tarpc::context::current(), table_name)
                .await?
        }
    }

    async fn flush_partition_local(
        self,
        _ctx: Context,
        table_name: String,
        partition_id: String,
    ) -> Result<(), CoreError> {
        log::info!(
            "💾 [Node] Flushing local partition '{}/{}'",
            table_name,
            partition_id
        );
        self.flush_partition_local_impl(&table_name, &partition_id)
            .await
    }

    async fn is_coordinator(self, _ctx: Context) -> bool {
        self.am_i_coord_node()
    }
}
