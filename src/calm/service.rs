//! 统一的 RPC 服务
//!
//! 合并 DataService 和 MetaService 的所有功能到一个服务中

use std::{collections::HashMap, path::PathBuf};

use crate::{
    calm::{
        new_data_client, CalmService, PartitionDetail, SegmentDetail,
        TableDetail,
    },
    catalog::{table_meta::PartitionStrategy, TableMeta},
    cluster::keys,
    schema::Schema,
    utils::error::{CoreError, CoreResult},
};
use ddl_macros::coordinator_route;
use itertools::Itertools;
use tarpc::context::Context;
use tokio_serde::formats::Bincode;

/// RPC 调用结果处理的扩展 trait
///
/// tarpc 调用返回 `Result<Result<T, CoreError>, RpcError>`，这个 trait 提供便捷方法来展平并添加上下文
trait RpcResultExt<T> {
    /// 展平双层 Result 并添加上下文信息
    fn flatten_rpc(self, context: &str) -> CoreResult<T>;
}

impl<T> RpcResultExt<T> for Result<CoreResult<T>, tarpc::client::RpcError> {
    fn flatten_rpc(self, context: &str) -> CoreResult<T> {
        match self {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => {
                log::error!("❌ [RPC] {}: {}", context, e);
                Err(e)
            }
            Err(e) => {
                log::error!("❌ [RPC] {} - Network error: {}", context, e);
                Err(CoreError::Network(format!(
                    "{}: RPC failed: {}",
                    context, e
                )))
            }
        }
    }
}

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

    /// 本地删除表元数据（数据节点）
    async fn drop_table_local(table_name: String) -> CoreResult<()>;

    /// 本地删除分区数据
    async fn drop_partition_local(table_name: String, partition_name: String) -> CoreResult<()>;

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

    /// 删除表（协调节点）
    async fn drop_partition(table_name: String, partition_name: String) -> CoreResult<()>;

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
    #[ddl_macros::local_only]
    async fn list_all_partitions(
        self,
        _context: ::tarpc::context::Context,
    ) -> CoreResult<Vec<(String, String)>> {
        Ok(self.engine.list_all_spartitions().await)
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

        let _lock = self.locker.partition_lock.lock().await;

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

        let partition_path = self.catalog.partition_dir(&table_name, &partition_name);

        // 验证parititon目录不存在
        if partition_path.exists() {
            return Err(CoreError::Existed(format!(
                "Partition '{}/{}' already exists on filesystem but not loaded in engine. Possible concurrent creation?",
                table_name,
                partition_name
            )));
        }

        // 2. 加载表信息
        let table_info = self.catalog.get_or_load_table(&table_name).await?;

        // 3. 在 catalog 中创建分区元数据
        let owner = self.cluster_manager.node_id().unwrap_or("standalone");
        self.catalog
            .create_partition(&table_name, &partition_name, owner)
            .await?;

        // 4. 在 engine 中加载分区
        self.engine
            .load_partition(
                &table_name,
                &partition_name,
                partition_path,
                table_info.table.schema.clone(),
            )
            .await?;

        // 5. 通过 gossip 发布分区信息
        self.cluster_manager
            .publish_partition(&table_name, &partition_name)
            .await;

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
        let _partition_lock = self.locker.partition_lock.lock().await;

        let table_info = self.catalog.get_or_load_table(&table_name).await?;

        let owner = self
            .catalog
            .get_or_load_partition_meta(&table_name, &partition_name)
            .await?
            .owner;

        // 判断partition是否已经被分配给其他节点, 且还在工作中
        if owner.as_str() != self.cluster_manager.node_id_without_none() {
            match self
                .check_partition_on_node(&table_name, &partition_name, &owner)
                .await
            {
                Ok(Some(true)) => {
                    return Err(CoreError::ClusterState(format!(
                        "Partition '{}/{}' is owned by node '{}' which already has the partition loaded",
                        table_name,
                        partition_name,
                        owner
                    )));
                }
                Ok(Some(false)) => {
                    log::info!(
                        "⚠️  Partition '{}/{}' owner node '{}' does not have the partition, proceeding to load locally",
                        table_name,
                        partition_name,
                        owner
                    );
                }
                _ => {
                    log::warn!(
                        "⚠️  Partition '{}/{}' owner node '{}' is unknown state, proceeding to load locally",
                        table_name,
                        partition_name,
                        owner
                    );
                }
            }
        };

        let owner = self.cluster_manager.node_id_without_none();

        let partition_path = self.catalog.partition_dir(&table_name, &partition_name);

        self.engine
            .load_partition(
                &table_name,
                &partition_name,
                partition_path,
                table_info.table.schema.clone(),
            )
            .await?;

        self.catalog
            .set_partition_owner(&table_name, &partition_name, owner)
            .await?;

        // 5. 通过 gossip 发布分区信息
        self.cluster_manager
            .publish_partition(&table_name, &partition_name)
            .await;

        Ok(())
    }

    /// 获取分区详细信息（包括 segments）
    #[ddl_macros::local_only]
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

        let partition_info = self
            .catalog
            .get_partition_meta(&table_name, &partition_name)
            .await?;

        // check partition is self
        if (partition_info.owner != self.node_id()) && (self.node_id() != "standalone") {
            return Err(CoreError::NotExisted(format!(
                "Partition '{}/{}' is owned by node '{}', cannot get detail from node '{}'",
                table_name,
                partition_name,
                partition_info.owner,
                self.node_id()
            )));
        }

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
            segments,
            partition_name,
            created_at: partition_info.created_at,
            updated_at: partition_info.updated_at,
            owner: partition_info.owner.clone(),
        })
    }

    /// 本地删除分区数据
    #[ddl_macros::local_only]
    async fn drop_partition_local(
        self,
        _context: Context,
        table_name: String,
        partition_name: String,
    ) -> CoreResult<()> {
        let _partition_lock = self.locker.partition_lock.lock().await;

        log::info!(
            "🗑️  [Local] Dropping partition '{}/{}'",
            table_name,
            partition_name
        );

        // 从 engine 中移除分区
        self.engine
            .remove_partition(&table_name, &partition_name)
            .await;

        // 从 catalog 中移除分区元数据，失败时仅记录日志
        if let Err(e) = self
            .catalog
            .remove_partition(&table_name, &partition_name)
            .await
        {
            log::warn!(
                "⚠️  Failed to remove partition '{}/{}' from catalog: {}",
                table_name,
                partition_name,
                e
            );
        }

        // 从 gossip 中移除分区
        self.cluster_manager
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
    #[ddl_macros::local_only]
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
    #[ddl_macros::local_only]
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

    #[doc = " 加载 segment 文件到指定分区 , （自动路由到分区所在节点）"]
    #[ddl_macros::local_only]
    async fn load_segment(
        self,
        _: Context,
        table_name: String,
        partition_name: String,
        file_path: String,
        handler_type: Option<crate::segment_loader::FileHandlerType>,
    ) -> CoreResult<()> {
        log::info!(
            "📤 Starting load_segment for '{}/{}' from file '{}'",
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

        let rows_loaded = client
            .load_segment_local(
                tarpc::context::current(),
                table_name.clone(),
                partition_name.clone(),
                file_path.clone(),
                handler_type,
            )
            .await
            .flatten_rpc(&format!(
                "Load segment for '{}/{}' on node '{}' from '{}'",
                table_name, partition_name, owner_node, file_path
            ))?;

        log::info!(
            "✅ Segment loaded successfully for '{}/{}' on node '{}': {} rows",
            table_name,
            partition_name,
            owner_node,
            rows_loaded
        );

        Ok(())
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

    // ========================================================== meta operator
    #[doc = " 创建表"]
    #[coordinator_route]
    async fn create_table(
        self,
        ctx: Context,
        schema: Schema,
        partition_strategy: PartitionStrategy,
    ) -> CoreResult<()> {
        let _lock = self.locker.coord_partition_lock.lock().await;
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

        // 4. 轮询分配 partition 到节点上
        let mut node_states: HashMap<String, usize> = self
            .idle_nodes()
            .await?
            .into_iter()
            .map(|n| (n.node_id, n.partition_count))
            .collect();

        let min_value = |m: &HashMap<String, usize>| {
            m.iter()
                .min_by_key(|entry| entry.1)
                .map(|(k, _)| k.clone())
                .unwrap()
        };
        let mut assignments = Vec::new();
        for partition_name in &partitions {
            let node_id = min_value(&node_states);
            assignments.push((partition_name.clone(), node_id.clone()));
            node_states.entry(node_id).and_modify(|count| *count += 1);
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
            let client = new_data_client(&node_id).await?;

            client
                .create_partition_local(
                    tarpc::context::current(),
                    table_name.clone(),
                    partition_name.clone(),
                )
                .await
                .flatten_rpc(&format!(
                    "Create partition '{}/{}' on node '{}'",
                    table_name, partition_name, node_id
                ))?;

            log::info!(
                "✅ Partition '{}/{}' created on node '{}'",
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
                cli.drop_partition_local(tarpc::context::current(), tn.clone(), pn.clone())
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
                let result = async {
                    let client = new_data_client(node_id).await?;
                    client
                        .drop_table_local(tarpc::context::current(), table_name.clone())
                        .await
                        .flatten_rpc(&format!(
                            "Drop table '{}' metadata on node '{}'",
                            table_name, node_id
                        ))
                }
                .await;

                match result {
                    Ok(()) => {
                        log::info!(
                            "✅ Node '{}' dropped table '{}' metadata locally",
                            node_id,
                            table_name
                        );
                    }
                    Err(e) => {
                        log::warn!(
                            "⚠️  Failed to drop table '{}' metadata on node '{}': {}",
                            table_name,
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

    #[coordinator_route]
    async fn drop_partition(
        self,
        _: Context,
        table_name: String,
        partition_name: String,
    ) -> CoreResult<()> {
        let _lock = self.locker.coord_partition_lock.lock().await;

        log::info!(
            "🗑️  [CoordNode] Starting drop partition '{}/{}'",
            table_name,
            partition_name
        );
        let table_info = self.catalog.get_or_load_table(&table_name).await?;
        let owner = table_info
            .partitions
            .read()
            .await
            .get(&partition_name)
            .ok_or_else(|| {
                CoreError::NotExisted(format!(
                    "Partition '{}/{}' not found",
                    table_name, partition_name
                ))
            })?
            .owner
            .clone();

        let cli = new_data_client(&owner).await?;

        cli.drop_partition_local(
            tarpc::context::current(),
            table_name.clone(),
            partition_name.clone(),
        )
        .await
        .flatten_rpc(&format!(
            "Drop partition '{}/{}' on node '{}'",
            table_name, partition_name, owner
        ))
    }

    #[doc = " 获取表元数据"]
    #[coordinator_route]
    async fn get_table_detail(self, _: Context, table_name: String) -> CoreResult<TableDetail> {
        let table_info = self.catalog.get_or_load_table(&table_name).await?;

        let partition_list = table_info
            .partitions
            .read()
            .await.values().map(|p| (p.partition_name.clone(), p.owner.clone()))
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

            match cli
                .get_partition_detail(
                    tarpc::context::current(),
                    table_name.clone(),
                    partition_name.clone(),
                )
                .await
                .flatten_rpc(&format!(
                    "Get partition detail for '{}/{}' from node '{}'",
                    table_name, partition_name, owner
                )) {
                Ok(partition_detail) => {
                    partitions.insert(partition_name.clone(), partition_detail);
                }
                Err(e) => {
                    log::warn!(
                        "⚠️  Failed to get partition detail for '{}/{}' from node '{}' err: {}. Returning empty detail.",
                        table_name,
                        partition_name,
                        owner,
                        e,
                    );

                    partitions.insert(
                        partition_name.clone(),
                        PartitionDetail {
                            partition_name: partition_name.clone(),
                            segments: Vec::new(),
                            created_at: 0,
                            updated_at: 0,
                            owner,
                        },
                    );
                }
            };
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
            let result = async {
                let client = new_data_client(&node_id).await?;
                client
                    .node_info(tarpc::context::current())
                    .await
                    .flatten_rpc(&format!("Get node info from '{}'", node_id))
            }
            .await;

            match result {
                Ok(node_info) => node_infos.push(node_info),
                Err(e) => {
                    log::warn!(
                        "⚠️  Failed to get info from node '{}': {}, skipping",
                        node_id,
                        e
                    );
                }
            }
        }

        Ok(node_infos)
    }

    #[coordinator_route]
    async fn create_partition(
        self,
        ctx: Context,
        table_name: String,
        partition_name: String,
    ) -> CoreResult<crate::catalog::PartitionMeta> {
        let _lock = self.locker.coord_partition_lock.lock().await;

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
        let target_node = self.idle_node().await?;

        log::info!(
            "📍 [CoordNode] Selected node '{}' to create partition '{}/{}'",
            target_node,
            table_name,
            partition_name,
        );

        // 3. 通知选定的节点创建分区
        let client = new_data_client(&target_node).await?;

        let partition_meta = client
            .create_partition_local(
                tarpc::context::current(),
                table_name.clone(),
                partition_name.clone(),
            )
            .await
            .flatten_rpc(&format!(
                "[CoordNode] Create partition '{}/{}' on node '{}'",
                table_name, partition_name, target_node
            ))?;

        log::info!(
            "✅ [CoordNode] Partition '{}/{}' created successfully on node '{}'",
            table_name,
            partition_name,
            target_node
        );

        // 4. 等待 gossip 同步到协调节点 catalog，超时后主动查询
        log::info!(
            "⏳ [CoordNode] Waiting for partition '{}/{}' to be synced via gossip...",
            table_name,
            partition_name
        );

        let max_wait_secs = 20;
        let start = std::time::Instant::now();

        loop {
            // 检查分区是否已同步到本地 catalog
            if let Ok(synced_meta) = self
                .catalog
                .get_partition_meta(&table_name, &partition_name)
                .await
            {
                log::info!(
                    "✅ [CoordNode] Partition '{}/{}' synced via gossip",
                    table_name,
                    partition_name
                );
                return Ok(synced_meta);
            }

            // 超时检查
            if start.elapsed().as_secs() > max_wait_secs {
                log::warn!(
                    "⏰ [CoordNode] Gossip sync timeout for partition '{}/{}', verifying with data node...",
                    table_name,
                    partition_name
                );

                // 超时后，主动查询数据节点验证分区是否真实存在
                let client = new_data_client(&target_node).await?;
                match client
                    .get_partition_detail(
                        tarpc::context::current(),
                        table_name.clone(),
                        partition_name.clone(),
                    )
                    .await
                    .flatten_rpc(&format!(
                        "Verify partition '{}/{}' on node '{}'",
                        table_name, partition_name, target_node
                    )) {
                    Ok(_) => {
                        // 分区在数据节点上确实存在，手动同步到本地 catalog
                        log::info!(
                            "📥 [CoordNode] Partition '{}/{}' verified on node '{}', manually syncing to catalog",
                            table_name,
                            partition_name,
                            target_node
                        );
                        self.catalog
                            .update_partition_meta(&table_name, partition_meta.clone())
                            .await?;
                        return Ok(partition_meta);
                    }
                    Err(e) => {
                        return Err(CoreError::Timeout(format!(
                            "Partition '{}/{}' gossip sync timeout and verification failed: {}",
                            table_name, partition_name, e
                        )));
                    }
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }
    }

    #[doc = r" Returns a serving function to use with"]
    #[doc = r" [InFlightRequest::execute](::tarpc::server::InFlightRequest::execute)."]
    fn serve(self) -> ServeCalmRpcService<Self> {
        ServeCalmRpcService { service: self }
    }
}
