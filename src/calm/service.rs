//! 统一的 RPC 服务
//!
//! 合并 DataService 和 MetaService 的所有功能到一个服务中

use std::{collections::HashMap, sync::Arc};

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

    /// 创建分区
    async fn create_partition(table_name: String, partition_name: String) -> CoreResult<()>;

    /// 加载分区
    async fn load_partition(table_name: String, partition_name: String) -> CoreResult<()>;

    /// 本地删除分区数据
    async fn drop_partition(table_name: String, partition_name: String) -> CoreResult<()>;

    /// 本地持久化分区数据
    async fn flush_partition(table_name: String, partition_name: String) -> CoreResult<()>;

    /// 插入数据到指定分区（RPC 远程插入，分区必须指定）
    async fn insert_data_by_partition(
        table_name: String,
        partition_name: String,
        data: Vec<serde_json::Value>,
    ) -> CoreResult<usize>;

    // ==================== MetaService 方法 ======================================

    /// 创建表
    async fn create_table(schema: Schema, partition_strategy: PartitionStrategy) -> CoreResult<()>;

    /// 删除表
    async fn drop_table(table_name: String) -> CoreResult<()>;

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

    async fn create_partition(
        self,
        _context: Context,
        table_name: String,
        partition_name: String,
    ) -> CoreResult<()> {
        let table_info = self.catalog.load_table(&table_name).await?;

        let owner = self.cluster_manager.node_id().unwrap_or("standalone");
        self.catalog
            .create_partition(&table_name, &partition_name, owner)
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

        // 通过 gossip 发布分区信息
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

        Ok(())
    }

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

    async fn insert_data_by_partition(
        self,
        _context: Context,
        table_name: String,
        partition_name: String,
        data: Vec<serde_json::Value>,
    ) -> CoreResult<usize> {
        log::debug!(
            "📝 [RPC] insert_data_by_partition: table='{}', partition='{}', rows={}",
            table_name,
            partition_name,
            data.len()
        );

        // 直接插入到指定分区（远程调用已经路由好了）
        let table_info = self.catalog.get_or_load_table(&table_name).await?;
        let meta = &table_info.table;

        // 确保分区存在
        if self
            .engine
            .get_partition(&table_name, &partition_name)
            .await
            .is_none()
        {
            log::debug!(
                "🔧 [RPC] Creating partition '{}' for table '{}'",
                partition_name,
                table_name
            );
            let partition_dir = self.catalog.partition_dir(&table_name, &partition_name);
            self.engine
                .load_partition(
                    &table_name,
                    &partition_name,
                    partition_dir,
                    meta.schema.clone(),
                )
                .await?;
        }

        // 获取分区并插入
        let partition = self
            .engine
            .get_partition(&table_name, &partition_name)
            .await
            .ok_or_else(|| {
                CoreError::Internal(format!(
                    "Partition '{}' not found after creation",
                    partition_name
                ))
            })?;

        partition.upsert_json(&data)?;

        log::debug!(
            "✅ [RPC] Inserted {} rows to partition '{}'",
            data.len(),
            partition_name
        );

        Ok(data.len())
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

        // 1. 通过 RPC 通知数据节点创建分区
        for partition_name in &partitions {
            let node_id = self
                .cluster_manager
                .idle_nodes()
                .await?
                .get(0)
                .cloned()
                .ok_or_else(|| {
                    CoreError::ClusterState(format!(
                        "⚠️  No idle nodes available to create partition '{}/{}'",
                        table_name, partition_name
                    ))
                })?;

            log::info!(
                "📤 Requesting partition '{}/{}' creation on node '{}'",
                table_name,
                partition_name,
                node_id
            );

            // 在远程节点创建分区（数据节点会通过 gossip 发布）
            let _ = new_data_client(&node_id)
                .await?
                .create_partition(
                    tarpc::context::current(),
                    table_name.clone(),
                    partition_name.clone(),
                )
                .await?;

            log::info!(
                "✅ Partition '{}/{}' RPC call completed on node '{}'",
                table_name,
                partition_name,
                node_id
            );
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

        // 4. 删除表元数据（从 catalog 和共享存储）
        if let Err(e) = self.catalog.drop_table(&table_name).await {
            log::error!("❌ Failed to drop table metadata: {}", e);
            return Err(e);
        }

        log::info!("✅ [CoordNode] Table '{}' dropped successfully", table_name);
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

    #[doc = r" Returns a serving function to use with"]
    #[doc = r" [InFlightRequest::execute](::tarpc::server::InFlightRequest::execute)."]
    fn serve(self) -> ServeCalmRpcService<Self> {
        ServeCalmRpcService { service: self }
    }
}
