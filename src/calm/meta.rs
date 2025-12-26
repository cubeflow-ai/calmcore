use std::{collections::HashMap, sync::Arc};

use ddl_macros::coordinator_route;
use itertools::Itertools;
use poem_openapi::types::Type;
use serde::{Deserialize, Serialize};
use tarpc::context::Context;
use tokio_serde::formats::Bincode;

use crate::{
    calm::{data::RpcDataServiceClient, new_data_client, PartitionDetail, TableDetail},
    catalog::{Catalog, PartitionStrategy, TableMeta},
    cluster::{keys, ClusterManager},
    engine::Engine,
    schema::Schema,
    utils::error::{CoreError, CoreResult},
};

#[derive(Clone)]
pub struct MetaService {
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) cluster_manager: Option<Arc<ClusterManager>>,
    pub(crate) engine: Arc<Engine>,
}

impl MetaService {
    pub(crate) fn new(
        engine: Arc<Engine>,
        catalog: Arc<Catalog>,
        cluster_manager: Option<Arc<ClusterManager>>,
    ) -> Self {
        Self {
            catalog,
            cluster_manager,
            engine,
        }
    }

    async fn select_idle_node(&self) -> CoreResult<String> {
        match &self.cluster_manager {
            None => Ok(keys::SINGLE_NODE_CLUSTER_ID.to_string()),
            Some(cm) => {
                let idle_nodes = cm.idle_nodes().await;
                if idle_nodes.is_empty() {
                    Err(CoreError::ClusterState(
                        "No idle nodes available".to_string(),
                    ))
                } else {
                    Ok(idle_nodes[0].clone())
                }
            }
        }
    }

    /// 检查当前节点是否是协调节点
    fn am_i_coord_node(&self) -> bool {
        let result = match &self.cluster_manager {
            Some(cm) => cm.am_i_coord_node(),
            None => true, // 单机模式认为是协调节点
        };

        log::debug!("🔍 [MetaService] am_i_coord_node check: {}", result);
        result
    }

    /// 获取协调节点的 RPC 客户端（用于宏）
    async fn coord_client(&self) -> CoreResult<RpcMetaServiceClient> {
        let cm = self
            .cluster_manager
            .as_ref()
            .ok_or_else(|| CoreError::ClusterState("Not in cluster mode".to_string()))?;

        let coord_id = cm
            .get_coord()
            .await
            .ok_or_else(|| CoreError::ClusterState("Coordinator not set".to_string()))?;

        let (_, coord_addr) = keys::parse_node_id(&coord_id)?;

        log::info!("📤 Creating client to coordinator: {}", coord_addr);

        let transport = tarpc::serde_transport::tcp::connect(coord_addr, Bincode::default)
            .await
            .map_err(|e| CoreError::Network(format!("Failed to connect to coordinator: {}", e)))?;

        Ok(RpcMetaServiceClient::new(tarpc::client::Config::default(), transport).spawn())
    }
}

#[tarpc::service]
pub trait RpcMetaService {
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

    /// 查询当前节点是否是中央节点
    async fn is_coordinator() -> bool;
}

impl RpcMetaService for MetaService {
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

        for partition_name in &partitions {
            let node_id = self.select_idle_node().await?;
            let _ = new_data_client(&node_id)
                .await?
                .create_partition(
                    tarpc::context::current(),
                    table_name.clone(),
                    partition_name.clone(),
                )
                .await?;
        }

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
                        .await
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
}
