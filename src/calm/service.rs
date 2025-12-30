//! 统一的 RPC 服务
//!
//! 合并 DataService 和 MetaService 的所有功能到一个服务中

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use crate::{
    calm::{
        new_data_client, CalmService, ClusterManagerRef, PartitionDetail, SegmentDetail,
        TableDetail,
    },
    catalog::{table_meta::PartitionStrategy, Catalog, TableMeta},
    cluster::{keys, ClusterManager},
    engine::Engine,
    schema::Schema,
    utils::error::{CoreError, CoreResult},
};
use ddl_macros::coordinator_route;
use itertools::Itertools;
use tarpc::context::Context;
use tokio_serde::formats::Bincode;

/// 统一的 RPC 服务接口
#[tarpc::service]
pub trait CalmRpcService {
    // ==================== DataService 方法 ====================

    /// 列出当前节点的所有分区
    async fn list_all_partitions() -> CoreResult<Vec<(String, String)>>;

    /// 获取分区详细信息（包括 segments）
    async fn get_partition_detail(
        table_name: String,
        partition_name: String,
    ) -> CoreResult<PartitionDetail>;

    /// 创建分区（协调节点方法，选择节点并分配）
    async fn create_partition(
        table_name: String,
        partition_name: String,
    ) -> CoreResult<crate::catalog::PartitionMeta>;

    /// 本地创建分区（数据节点方法，真正执行创建）
    async fn create_partition_local(
        table_name: String,
        partition_name: String,
    ) -> CoreResult<crate::catalog::PartitionMeta>;

    /// 加载分区
    async fn load_partition(table_name: String, partition_name: String) -> CoreResult<()>;

    /// 本地删除分区数据
    async fn drop_partition(table_name: String, partition_name: String) -> CoreResult<()>;

    /// 本地持久化分区数据
    async fn flush_partition(table_name: String, partition_name: String) -> CoreResult<()>;

    /// 获取当前节点的状态信息（partition数量、CPU、内存、负载等）
    async fn node_info() -> CoreResult<crate::calm::NodeInfo>;

    /// 加载 segment 文件到指定分区（自动路由到分区所在节点）
    async fn load_segment(
        table_name: String,
        partition_name: String,
        file_path: String,
        handler_type: Option<crate::segment_loader::FileHandlerType>,
    ) -> CoreResult<()>;

    /// 本地加载 segment 文件（仅在分区所在节点执行）
    async fn load_segment_local(
        table_name: String,
        partition_name: String,
        file_path: String,
        handler_type: Option<crate::segment_loader::FileHandlerType>,
    ) -> CoreResult<usize>;

    // ==================== MetaService 方法 ======================================

    /// 获取所有存活节点的状态信息（仅协调节点）
    async fn list_node() -> CoreResult<Vec<crate::calm::NodeInfo>>;

    /// 创建表
    async fn create_table(schema: Schema, partition_strategy: PartitionStrategy) -> CoreResult<()>;

    /// 删除表（协调节点）
    async fn drop_table(table_name: String) -> CoreResult<()>;

    /// 本地删除表元数据（数据节点）
    async fn drop_table_local(table_name: String) -> CoreResult<()>;

    /// 获取表元数据
    async fn get_table_detail(table_name: String) -> CoreResult<TableDetail>;

    /// 列出所有表
    async fn list_tables() -> CoreResult<Vec<String>>;

    /// 列出表的所有分区
    async fn list_partitions(table_name: String) -> CoreResult<Vec<String>>;

    /// 持久化表（刷新所有分区到磁盘）
    async fn flush_table(table_name: String) -> CoreResult<()>;

    /// 查询当前节点是否是协调节点
    async fn is_coordinator() -> bool;
}

/// 统一服务的实现

impl CalmService {
    fn node_id(&self) -> &str {
        self.cluster_manager
            .as_ref()
            .map(|cm| cm.node_id())
            .unwrap_or("standalone")
    }

    fn am_i_coord_node(&self) -> bool {
        self.cluster_manager.am_i_coord_node()
    }

    /// 获取协调节点的 RPC 客户端（用于宏）
    async fn coord_client(&self) -> CoreResult<CalmRpcServiceClient> {
        let coord_id = self.cluster_manager.coord_id().await?;

        // 从 node_id 解析 tarpc 地址
        let tarpc_addr = keys::parse_tarpc_address_from_node_id(&coord_id)
            .ok_or_else(|| CoreError::Internal(format!("Invalid node_id format: {}", coord_id)))?;

        log::info!("📤 Creating client to coordinator: {}", tarpc_addr);

        let transport = tarpc::serde_transport::tcp::connect(tarpc_addr, Bincode::default)
            .await
            .map_err(|e| CoreError::Network(format!("Failed to connect to coordinator: {}", e)))?;

        Ok(CalmRpcServiceClient::new(tarpc::client::Config::default(), transport).spawn())
    }
}

impl CalmRpcService for CalmService {
    //=========================================================== local operator

    #[doc = " 列出当前结点的所有分区"]
    async fn list_all_partitions(
        self,
        _context: ::tarpc::context::Context,
    ) -> CoreResult<Vec<(String, String)>> {
        Ok(self.engine.list_all_spartitions().await)
    }

    #[coordinator_route]
    async fn create_partition(
        self,
        ctx: Context,
        table_name: String,
        partition_name: String,
    ) -> CoreResult<crate::catalog::PartitionMeta> {
        // === 以下是协调节点的实际执行逻辑 ===
        log::info!(
            "📦 [CoordNode] Coordinating partition '{}/{}' creation",
            table_name,
            partition_name
        );

        // 1. 检查分区是否已经存在（处理并发创建）
        match self
            .catalog
            .get_partition_meta(&table_name, &partition_name)
            .await
        {
            Ok(partition_meta) => {
                log::info!(
                    "✅ [CoordNode] Partition '{}/{}' already exists in catalog, returning existing meta",
                    table_name,
                    partition_name
                );
                return Ok(partition_meta);
            }
            Err(CoreError::NotExisted(_)) => {
                // 分区不存在，继续创建流程
                log::debug!(
                    "📦 [CoordNode] Partition '{}/{}' does not exist, proceeding with node selection",
                    table_name,
                    partition_name
                );
            }
            Err(e) => {
                // 其他错误，直接返回
                return Err(e);
            }
        }

        // 2. 选择合适的节点来创建分区（负载均衡）
        let best_node = self.idle_node().await?;

        let target_node = best_node.ok_or_else(|| {
            CoreError::ClusterState(
                "No connectable nodes available to create partition".to_string(),
            )
        })?;

        log::info!(
            "📍 [CoordNode] Selected node '{}' to create partition '{}/{}'",
            target_node,
            table_name,
            partition_name,
        );

        // 3. 通知选定的节点创建分区
        let client = new_data_client(&target_node).await?;

        let partition_meta = match client
            .create_partition_local(
                tarpc::context::current(),
                table_name.clone(),
                partition_name.clone(),
            )
            .await
        {
            Ok(Ok(partition_meta)) => {
                log::info!(
                    "✅ [CoordNode] Partition '{}/{}' created successfully on node '{}'",
                    table_name,
                    partition_name,
                    target_node
                );
                partition_meta
            }
            Ok(Err(e)) => {
                log::error!(
                    "❌ [CoordNode] Failed to create partition '{}/{}' on node '{}': {}",
                    table_name,
                    partition_name,
                    target_node,
                    e
                );
                return Err(e);
            }
            Err(e) => {
                log::error!(
                    "❌ [CoordNode] RPC error creating partition '{}/{}' on node '{}': {}",
                    table_name,
                    partition_name,
                    target_node,
                    e
                );
                return Err(CoreError::Network(format!("RPC failed: {}", e)));
            }
        };

        // 4. 🔥 协调节点立即同步到本地 catalog（关键！）
        log::info!(
            "📥 [CoordNode] Syncing partition '{}/{}' metadata to local catalog",
            table_name,
            partition_name
        );

        self.catalog
            .update_partition_meta(&table_name, partition_meta.clone())
            .await?;

        log::info!(
            "✅ [CoordNode] Partition '{}/{}' metadata synced to coordinator catalog",
            table_name,
            partition_name
        );

        Ok(partition_meta)
    }

    /// 本地创建分区（数据节点执行）
    #[ddl_macros::local_only]
    async fn create_partition_local(
        self,
        _ctx: Context,
        table_name: String,
        partition_name: String,
    ) -> CoreResult<crate::catalog::PartitionMeta> {
        log::info!(
            "🔧 [DataNode] Creating partition '{}/{}' locally",
            table_name,
            partition_name
        );

        // 1. 检查分区是否已经在本地存在（处理并发）
        if self
            .engine
            .get_partition(&table_name, &partition_name)
            .await
            .is_some()
        {
            log::info!(
                "✅ [DataNode] Partition '{}/{}' already loaded in engine, returning existing meta",
                table_name,
                partition_name
            );
            let partition_meta = self
                .catalog
                .get_partition_meta(&table_name, &partition_name)
                .await?;
            return Ok(partition_meta);
        }

        // 2. 加载表信息
        let table_info = self.catalog.load_table(&table_name).await?;

        // 3. 在 catalog 中创建分区元数据
        let owner = self.cluster_manager.node_id().unwrap_or("standalone");
        self.catalog
            .create_partition(&table_name, &partition_name, owner)
            .await?;

        // 4. 在 engine 中加载分区
        let partition_path = self.catalog.partition_dir(&table_name, &partition_name);
        self.engine
            .load_partition(
                &table_name,
                &partition_name,
                partition_path,
                table_info.table.schema,
            )
            .await?;

        // 5. 通过 gossip 发布分区信息
        if let Some(cm) = self.cluster_manager.as_ref() {
            let version = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            log::info!(
                "📡 [DataNode] Publishing partition '{}/{}' to gossip with version {}",
                table_name,
                partition_name,
                version
            );

            cm.put_partition(&table_name, &partition_name, version)
                .await;

            log::info!(
                "✅ [DataNode] Partition '{}/{}' published to gossip successfully",
                table_name,
                partition_name
            );
        } else {
            log::warn!(
                "⚠️  [DataNode] No cluster manager available, partition '{}/{}' NOT published to gossip",
                table_name,
                partition_name
            );
        }

        // 6. 返回创建的分区元数据
        let partition_meta = self
            .catalog
            .get_partition_meta(&table_name, &partition_name)
            .await?;

        log::info!(
            "✅ [DataNode] Partition '{}/{}' created and loaded successfully",
            table_name,
            partition_name
        );

        Ok(partition_meta)
    }

    #[ddl_macros::local_only]
    async fn load_partition(
        self,
        _context: Context,
        table_name: String,
        partition_name: String,
    ) -> CoreResult<()> {
        let table_info = self.catalog.load_table(&table_name).await?;

        let owner = self
            .catalog
            .get_partition_owner(&table_name, &partition_name)
            .await?;

        // 判断partition是否已经被分配给其他节点, 且还在工作中
        if owner != self.cluster_manager.node_id().unwrap_or("standalone") {
            match new_data_client(&owner)
                .await?
                .get_partition_detail(
                    tarpc::context::current(),
                    table_name.clone(),
                    partition_name.clone(),
                )
                .await
            {
                Ok(Ok(p)) => {
                    return Err(CoreError::Existed(format!(
                        "Partition is owned by node '{}', details: {:?}",
                        owner, p
                    )));
                }
                Ok(Err(e)) => {
                    if e.code() != CoreError::NotExisted("".to_string()).code() {
                        log::warn!(
                        "⚠️  Failed to get partition detail from owner node '{}': {}, proceeding to load locally",
                        owner,
                        e
                    );
                    }
                }
                Err(e) => {
                    log::warn!(
                        "⚠️  Failed to get partition detail from owner node '{}': {}, proceeding to load locally",
                        owner,
                        e
                    );
                }
            }
        }

        let owner = self.cluster_manager.node_id().unwrap_or("standalone");
        self.catalog
            .set_partition_owner(&table_name, &partition_name, owner)
            .await?;

        let partition_path = self.catalog.partition_dir(&table_name, &partition_name);

        self.engine
            .load_partition(
                &table_name,
                &partition_name,
                partition_path,
                table_info.table.schema,
            )
            .await?;

        Ok(())
    }

    /// 获取分区详细信息（包括 segments）
    async fn get_partition_detail(
        self,
        _context: Context,
        table_name: String,
        partition_name: String,
    ) -> CoreResult<PartitionDetail> {
        let partition = self
            .engine
            .get_partition(&table_name, &partition_name)
            .await
            .ok_or_else(|| {
                CoreError::NotExisted(format!(
                    "Partition '{}' in table '{}' not found",
                    partition_name, table_name
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
            partition_id: partition_name.to_string(),
            segments,
            owner_node: self.cluster_manager.node_id().map(|s| s.to_string()),
        })
    }

    /// 本地删除分区数据
    async fn drop_partition(
        self,
        _context: Context,
        table_name: String,
        partition_name: String,
    ) -> CoreResult<()> {
        log::info!(
            "🗑️  [Local] Dropping partition '{}/{}'",
            table_name,
            partition_name
        );

        // 从 engine 中移除分区
        self.engine
            .remove_partition(&table_name, &partition_name)
            .await;

        log::info!(
            "✅ [Local] Partition '{}/{}' dropped",
            table_name,
            partition_name
        );
        Ok(())
    }

    /// 本地持久化分区数据
    async fn flush_partition(
        self,
        _context: Context,
        table_name: String,
        partition_name: String,
    ) -> CoreResult<()> {
        log::info!(
            "💾 [Local] Flushing partition '{}/{}'",
            table_name,
            partition_name
        );

        // 获取分区并执行 flush
        if let Some(partition) = self
            .engine
            .get_partition(&table_name, &partition_name)
            .await
        {
            partition
                .persist_all()
                .map_err(|e| CoreError::Internal(format!("Failed to flush partition: {}", e)))?;
            log::info!(
                "✅ [Local] Partition '{}/{}' flushed",
                table_name,
                partition_name
            );
        } else {
            log::warn!(
                "⚠️  Partition '{}/{}' not found on this node",
                table_name,
                partition_name
            );
        }

        Ok(())
    }

    /// 获取当前节点的状态信息
    async fn node_info(self, _context: Context) -> CoreResult<crate::calm::NodeInfo> {
        use sysinfo::System;

        let node_id = self.node_id().to_string();

        // 获取当前节点的所有 partition
        let all_partitions = self.engine.list_all_spartitions().await;
        let partition_count = all_partitions.len();

        // 获取系统信息
        let mut sys = System::new_all();
        sys.refresh_all();

        // CPU 使用率 (所有核心的平均值)
        let cpu_usage =
            sys.cpus().iter().map(|cpu| cpu.cpu_usage()).sum::<f32>() / sys.cpus().len() as f32;

        // 内存信息
        let total_memory = sys.total_memory();
        let used_memory = sys.used_memory();
        let memory_usage = (used_memory as f64 / total_memory as f64 * 100.0) as f32;

        // 系统负载 (1分钟平均负载) - 使用关联函数
        let load_avg = System::load_average();
        let load_avg_1min = load_avg.one as f32;

        Ok(crate::calm::NodeInfo {
            node_id,
            partition_count,
            cpu_usage,
            memory_usage,
            total_memory,
            used_memory,
            load_avg_1min,
            all_partitions,
        })
    }

    // ========================================================== meta operator
    #[doc = " 创建表"]
    #[coordinator_route]
    async fn create_table(
        self,
        ctx: Context,
        schema: Schema,
        partition_strategy: PartitionStrategy,
    ) -> CoreResult<()> {
        // === 以下是协调节点的实际执行逻辑 ===
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

        log::info!(
            "📋 Table '{}' created, requesting {} partitions creation...",
            table_name,
            partitions.len()
        );

        // 1. 获取所有可用节点
        let available_nodes = self.cluster_manager.idle_nodes().await?;

        if available_nodes.is_empty() {
            return Err(CoreError::ClusterState(
                "No available nodes to create partitions".to_string(),
            ));
        }

        log::info!("📊 Found {} available nodes", available_nodes.len());

        // 2. 获取每个节点的状态（partition 数量、CPU、内存等），如果节点无法连接则跳过
        let mut node_states: Vec<(String, crate::calm::NodeInfo)> = Vec::new();

        for node_id in available_nodes {
            match new_data_client(&node_id).await {
                Ok(client) => match client.node_info(tarpc::context::current()).await {
                    Ok(Ok(node_info)) => {
                        log::debug!(
                            "📊 Node '{}': {} partitions, CPU: {:.1}%, Memory: {:.1}%, Load: {:.2}",
                            node_id,
                            node_info.partition_count,
                            node_info.cpu_usage,
                            node_info.memory_usage,
                            node_info.load_avg_1min
                        );
                        node_states.push((node_id.clone(), node_info));
                    }
                    Ok(Err(e)) => {
                        log::warn!(
                            "⚠️  Failed to get node info from '{}': {}, skipping",
                            node_id,
                            e
                        );
                    }
                    Err(e) => {
                        log::warn!("⚠️  RPC error from node '{}': {}, skipping", node_id, e);
                    }
                },
                Err(e) => {
                    log::warn!("⚠️  Cannot connect to node '{}': {}, skipping", node_id, e);
                }
            }
        }

        if node_states.is_empty() {
            return Err(CoreError::ClusterState(
                "No connectable nodes available to create partitions".to_string(),
            ));
        }

        // 3. 排序节点：优先选择负载低的节点（综合考虑 partition 数量、CPU、内存、系统负载）
        node_states.sort_by(|(_, a), (_, b)| {
            // 综合评分：partition 数量权重 40%，CPU 30%，内存 20%，系统负载 10%
            let score_a = a.partition_count as f32 * 0.4
                + a.cpu_usage * 0.3
                + a.memory_usage * 0.2
                + a.load_avg_1min * 10.0 * 0.1;
            let score_b = b.partition_count as f32 * 0.4
                + b.cpu_usage * 0.3
                + b.memory_usage * 0.2
                + b.load_avg_1min * 10.0 * 0.1;
            score_a
                .partial_cmp(&score_b)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        log::info!("📊 Node load distribution:");
        for (node_id, info) in &node_states {
            log::info!(
                "   - {}: {} partitions, CPU: {:.1}%, Memory: {:.1}% ({}/{}MB), Load: {:.2}",
                node_id,
                info.partition_count,
                info.cpu_usage,
                info.memory_usage,
                info.used_memory / 1024 / 1024,
                info.total_memory / 1024 / 1024,
                info.load_avg_1min
            );
        }

        // 4. 轮询分配 partition 到节点上
        let mut assignments: Vec<(String, String)> = Vec::new(); // (partition_name, node_id)
        let mut node_index = 0;

        for partition_name in &partitions {
            let (node_id, node_info) = &mut node_states[node_index];

            assignments.push((partition_name.clone(), node_id.clone()));

            // 更新该节点的 partition 计数（模拟负载）
            node_info.partition_count += 1;

            // 移动到下一个节点，并重新排序以保持负载均衡
            node_index = (node_index + 1) % node_states.len();
            node_states.sort_by(|(_, a), (_, b)| {
                let score_a = a.partition_count as f32 * 0.4
                    + a.cpu_usage * 0.3
                    + a.memory_usage * 0.2
                    + a.load_avg_1min * 10.0 * 0.1;
                let score_b = b.partition_count as f32 * 0.4
                    + b.cpu_usage * 0.3
                    + b.memory_usage * 0.2
                    + b.load_avg_1min * 10.0 * 0.1;
                score_a
                    .partial_cmp(&score_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        log::info!("📋 Partition assignment plan:");
        for (partition_name, node_id) in &assignments {
            log::info!("   - {}/{} -> {}", table_name, partition_name, node_id);
        }

        // 5. 通过 RPC 通知数据节点创建分区
        for (partition_name, node_id) in assignments {
            log::info!(
                "📤 Requesting partition '{}/{}' creation on node '{}'",
                table_name,
                partition_name,
                node_id
            );

            // 在远程节点创建分区（数据节点会通过 gossip 发布）
            match new_data_client(&node_id).await {
                Ok(client) => {
                    match client
                        .create_partition(
                            tarpc::context::current(),
                            table_name.clone(),
                            partition_name.clone(),
                        )
                        .await
                    {
                        Ok(Ok(_)) => {
                            log::info!(
                                "✅ Partition '{}/{}' created on node '{}'",
                                table_name,
                                partition_name,
                                node_id
                            );
                        }
                        Ok(Err(e)) => {
                            log::error!(
                                "❌ Failed to create partition '{}/{}' on node '{}': {}",
                                table_name,
                                partition_name,
                                node_id,
                                e
                            );
                            return Err(e);
                        }
                        Err(e) => {
                            log::error!(
                                "❌ RPC error creating partition '{}/{}' on node '{}': {}",
                                table_name,
                                partition_name,
                                node_id,
                                e
                            );
                            return Err(CoreError::Network(format!("RPC failed: {}", e)));
                        }
                    }
                }
                Err(e) => {
                    log::error!("❌ Cannot connect to node '{}': {}", node_id, e);
                    return Err(e);
                }
            }
        }

        // 2. 等待所有分区通过 gossip 发布并被 coord_job 更新到 table_info
        log::info!(
            "⏳ Waiting for all {} partitions to be synced via gossip...",
            partitions.len()
        );

        let max_wait_secs = 30;
        let start = std::time::Instant::now();

        loop {
            // 检查是否所有分区都已创建
            let table_info = self.catalog.get_or_load_table(&table_name).await?;
            let created_partitions: Vec<String> =
                table_info.partitions.read().await.keys().cloned().collect();

            log::debug!(
                "⏳ Partition sync check: created {}/{} - {:?}",
                created_partitions.len(),
                partitions.len(),
                created_partitions
            );

            let all_created = partitions.iter().all(|p| created_partitions.contains(p));

            if all_created {
                log::info!(
                    "✅ All {} partitions for table '{}' synced successfully",
                    partitions.len(),
                    table_name
                );
                break;
            }

            // 超时检查
            if start.elapsed().as_secs() > max_wait_secs {
                let missing: Vec<_> = partitions
                    .iter()
                    .filter(|p| !created_partitions.contains(p))
                    .collect();
                return Err(CoreError::Timeout(format!(
                    "Timeout waiting for partitions to be synced via gossip. Missing: {:?}",
                    missing
                )));
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        log::info!("✅ Table '{}' created successfully", table_name);
        Ok(())
    }

    #[doc = " 加载 segment 文件到指定分区"]
    #[coordinator_route]
    async fn load_segment(
        self,
        _: Context,
        table_name: String,
        partition_name: String,
        file_path: String,
        handler_type: Option<crate::segment_loader::FileHandlerType>,
    ) -> CoreResult<()> {
        log::info!(
            "📤 [CoordNode] Starting load_segment for '{}/{}' from file '{}'",
            table_name,
            partition_name,
            file_path
        );

        // 1. 获取 partition 信息
        let table_info = self.catalog.get_or_load_table(&table_name).await?;
        let partitions = table_info.partitions.read().await;
        let partition_info = partitions.get(&partition_name).ok_or_else(|| {
            CoreError::NotExisted(format!(
                "Partition '{}/{}' not found",
                table_name, partition_name
            ))
        })?;

        let owner_node = partition_info.owner.clone();
        drop(partitions);

        log::info!(
            "📍 Partition '{}/{}' is owned by node '{}'",
            table_name,
            partition_name,
            owner_node
        );

        // 2. 连接到 owner 节点并调用本地加载方法
        let client = new_data_client(&owner_node).await?;

        log::info!(
            "📤 Requesting node '{}' to load segment from '{}'",
            owner_node,
            file_path
        );

        match client
            .load_segment_local(
                tarpc::context::current(),
                table_name.clone(),
                partition_name.clone(),
                file_path.clone(),
                handler_type,
            )
            .await
        {
            Ok(Ok(rows_loaded)) => {
                log::info!(
                    "✅ Segment loaded successfully for '{}/{}' on node '{}': {} rows",
                    table_name,
                    partition_name,
                    owner_node,
                    rows_loaded
                );
                Ok(())
            }
            Ok(Err(e)) => {
                log::error!(
                    "❌ Failed to load segment for '{}/{}' on node '{}': {}",
                    table_name,
                    partition_name,
                    owner_node,
                    e
                );
                Err(e)
            }
            Err(e) => {
                log::error!(
                    "❌ RPC error loading segment for '{}/{}' on node '{}': {}",
                    table_name,
                    partition_name,
                    owner_node,
                    e
                );
                Err(CoreError::Network(format!("RPC failed: {}", e)))
            }
        }
    }

    #[doc = " 本地加载 segment 文件（仅在分区所在节点执行）"]
    #[ddl_macros::local_only]
    async fn load_segment_local(
        self,
        _: Context,
        table_name: String,
        partition_name: String,
        file_path: String,
        handler_type: Option<crate::segment_loader::FileHandlerType>,
    ) -> CoreResult<usize> {
        log::info!(
            "🔧 [Local] Loading segment for '{}/{}' from '{}' with handler_type: {:?}",
            table_name,
            partition_name,
            file_path,
            handler_type
        );

        // 获取根工作目录（SegmentLoader 会自己构建完整路径）
        let work_dir = self.catalog.work_dir().to_path_buf();

        log::debug!("📂 Work directory: {}", work_dir.display());

        // 调用 engine.load_segment
        let rows_loaded = self
            .engine
            .load_segment(
                &table_name,
                partition_name.clone(),
                work_dir,
                PathBuf::from(file_path.clone()),
                handler_type,
            )
            .await?;

        log::info!(
            "✅ [Local] Segment loaded successfully for '{}/{}': {} rows",
            table_name,
            partition_name,
            rows_loaded
        );

        Ok(rows_loaded)
    }

    #[doc = " 删除表"]
    #[coordinator_route]
    async fn drop_table(self, _: Context, table_name: String) -> CoreResult<()> {
        log::info!("🗑️  [CoordNode] Starting drop table '{}'", table_name);

        let table_info = self.catalog.get_table_info(&table_name).await?;

        let mut tasks = Vec::new();
        for (name, pm) in table_info.partitions.read().await.iter() {
            let cli = match new_data_client(&pm.owner).await {
                Ok(c) => c,
                Err(e) => {
                    log::error!(
                        "❌ Failed to create data client for node '{}': {}",
                        &pm.owner,
                        e
                    );
                    continue;
                }
            };
            let tn = table_name.clone();
            let pn = name.clone();
            tasks.push(async move {
                cli.drop_partition(tarpc::context::current(), tn.clone(), pn.clone())
                    .await
                    .map_err(|e| CoreError::Internal(format!("Failed to drop partition {}", e)))
            });
        }

        // 等待所有删除任务完成
        for task in tasks {
            let _ = task.await;
        }

        // 3. 通知所有 alive 节点删除本地的表元数据
        if let Some(cm) = self.cluster_manager.as_ref() {
            let alive_nodes = cm.idle_nodes().await;
            log::info!(
                "📤 [CoordNode] Notifying {} alive nodes to drop table metadata for '{}'",
                alive_nodes.len(),
                table_name
            );

            for node_id in &alive_nodes {
                match new_data_client(node_id).await {
                    Ok(client) => {
                        match client
                            .drop_table_local(tarpc::context::current(), table_name.clone())
                            .await
                        {
                            Ok(Ok(())) => {
                                log::info!(
                                    "✅ Node '{}' dropped table '{}' metadata locally",
                                    node_id,
                                    table_name
                                );
                            }
                            Ok(Err(e)) => {
                                log::warn!(
                                    "⚠️  Node '{}' failed to drop table '{}' metadata: {}",
                                    node_id,
                                    table_name,
                                    e
                                );
                            }
                            Err(e) => {
                                log::warn!(
                                    "⚠️  RPC error notifying node '{}' to drop table '{}': {}",
                                    node_id,
                                    table_name,
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!(
                            "⚠️  Cannot connect to node '{}' to drop table metadata: {}",
                            node_id,
                            e
                        );
                    }
                }
            }
        }

        // 4. 删除表元数据（从 catalog 和共享存储）
        if let Err(e) = self.catalog.drop_table(&table_name).await {
            log::error!("❌ Failed to drop table metadata: {}", e);
            return Err(e);
        }

        log::info!("✅ [CoordNode] Table '{}' dropped successfully", table_name);
        Ok(())
    }

    #[doc = " 本地删除表元数据"]
    #[ddl_macros::local_only]
    async fn drop_table_local(self, _: Context, table_name: String) -> CoreResult<()> {
        log::info!(
            "🗑️  [Local] Dropping table '{}' metadata from local catalog",
            table_name
        );

        // 1. 从本地 engine 中移除所有该表的分区
        let all_partitions = self.engine.list_all_spartitions().await;
        for (table, partition) in all_partitions {
            if table == table_name {
                self.engine.remove_partition(&table_name, &partition).await;
                log::debug!(
                    "🗑️  Removed partition '{}/{}' from engine",
                    table_name,
                    partition
                );
            }
        }

        // 2. 从本地 catalog 中删除表元数据
        if let Err(e) = self.catalog.drop_table(&table_name).await {
            // 如果表不存在，不报错（可能已经删除或从未加载）
            if matches!(e, CoreError::NotExisted(_)) {
                log::debug!(
                    "ℹ️  Table '{}' not found in local catalog (already dropped or never loaded)",
                    table_name
                );
            } else {
                log::error!(
                    "❌ Failed to drop table '{}' from local catalog: {}",
                    table_name,
                    e
                );
                return Err(e);
            }
        }

        log::info!(
            "✅ [Local] Table '{}' metadata dropped from local catalog",
            table_name
        );
        Ok(())
    }

    #[doc = " 获取表元数据"]
    #[coordinator_route]
    async fn get_table_detail(self, _: Context, table_name: String) -> CoreResult<TableDetail> {
        let table_info = self.catalog.get_or_load_table(&table_name).await?;

        let partition_list = table_info
            .partitions
            .read()
            .await
            .iter()
            .map(|(_, p)| (p.partition_name.clone(), p.owner.clone()))
            .collect_vec();

        let mut partitions = HashMap::new();

        for (partition_name, owner) in partition_list {
            log::info!(
                "📦 Partition: {}/{} owned by {:?}",
                table_name,
                partition_name,
                owner
            );

            let cli = new_data_client(&owner).await?;

            if let Ok(Ok(partition_detail)) = cli
                .get_partition_detail(
                    tarpc::context::current(),
                    table_name.clone(),
                    partition_name.clone(),
                )
                .await
            {
                partitions.insert(partition_name.clone(), partition_detail);
            } else {
                log::warn!(
                    "⚠️  Failed to get partition detail for '{}/{}' from node '{}'",
                    table_name,
                    partition_name,
                    owner
                );

                partitions.insert(
                    partition_name.clone(),
                    PartitionDetail {
                        partition_id: partition_name.clone(),
                        segments: Vec::new(),
                        owner_node: Some(owner),
                    },
                );
            }
        }

        Ok(TableDetail {
            table: table_info.table.clone(),
            partitions,
        })
    }

    #[doc = " 列出所有表"]
    #[coordinator_route]
    async fn list_tables(self, _: Context) -> CoreResult<Vec<String>> {
        Ok(self.catalog.list_tables().await)
    }

    #[doc = " 列出表的所有分区"]
    #[coordinator_route]
    async fn list_partitions(self, _: Context, table_name: String) -> CoreResult<Vec<String>> {
        self.catalog.get_partition_names(&table_name).await
    }

    #[doc = " 持久化表（刷新所有分区到磁盘）"]
    #[coordinator_route]
    async fn flush_table(self, _: Context, table_name: String) -> CoreResult<()> {
        let table_meta = self.catalog.get_or_load_table(&table_name).await?;

        log::info!(
            "💾 [MetaService] Flushing table '{}' with {} partitions",
            table_name,
            table_meta.partitions.read().await.len()
        );

        for (partition_name, pm) in table_meta.partitions.read().await.iter() {
            let _ = new_data_client(&pm.owner)
                .await?
                .flush_partition(
                    tarpc::context::current(),
                    table_name.clone(),
                    partition_name.clone(),
                )
                .await?;
        }

        log::info!("✅ Table '{}' flushed successfully", table_name);
        Ok(())
    }

    #[doc = " 查询当前节点是否是中央节点"]
    async fn is_coordinator(self, _context: ::tarpc::context::Context) -> bool {
        self.cluster_manager
            .as_ref()
            .map(|cm| cm.am_i_coord_node())
            .unwrap_or(true)
    }

    #[doc = " 获取所有存活节点的状态信息"]
    #[coordinator_route]
    async fn list_node(self, _: Context) -> CoreResult<Vec<crate::calm::NodeInfo>> {
        // 获取所有存活节点
        let alive_nodes = self.cluster_manager.idle_nodes().await?;

        if alive_nodes.is_empty() {
            return Ok(Vec::new());
        }

        log::debug!("📊 Querying {} alive nodes for status", alive_nodes.len());

        let mut node_infos = Vec::new();

        // 查询每个节点的状态
        for node_id in alive_nodes {
            match new_data_client(&node_id).await {
                Ok(client) => match client.node_info(tarpc::context::current()).await {
                    Ok(Ok(node_info)) => {
                        node_infos.push(node_info);
                    }
                    Ok(Err(e)) => {
                        log::warn!(
                            "⚠️  Failed to get node info from '{}': {}, skipping",
                            node_id,
                            e
                        );
                    }
                    Err(e) => {
                        log::warn!("⚠️  RPC error from node '{}': {}, skipping", node_id, e);
                    }
                },
                Err(e) => {
                    log::warn!("⚠️  Cannot connect to node '{}': {}, skipping", node_id, e);
                }
            }
        }

        Ok(node_infos)
    }

    #[doc = r" Returns a serving function to use with"]
    #[doc = r" [InFlightRequest::execute](::tarpc::server::InFlightRequest::execute)."]
    fn serve(self) -> ServeCalmRpcService<Self> {
        ServeCalmRpcService { service: self }
    }
}
