use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tarpc::context::Context;

use crate::{
    calm::{new_data_client, ClusterManagerRef, PartitionDetail, SegmentDetail},
    catalog::{self, Catalog},
    cluster::ClusterManager,
    engine::Engine,
    utils::error::{CoreError, CoreResult},
};

#[derive(Clone)]
pub struct DataService {
    catalog: Arc<Catalog>,
    cluster_manager: ClusterManagerRef,
    engine: Arc<Engine>,
}
impl DataService {
    pub(crate) fn new(
        engine: Arc<Engine>,
        catalog: Arc<Catalog>,
        cluster_manager: Option<Arc<ClusterManager>>,
    ) -> Self {
        Self {
            catalog,
            cluster_manager: ClusterManagerRef(cluster_manager),
            engine,
        }
    }
}

pub struct MetaService {
    pub catalog: Arc<Catalog>,
    pub cluster_manager: Option<Arc<ClusterManager>>,
    pub engine: Arc<Engine>,
}

#[tarpc::service]
pub trait RpcDataService {
    /// 列出当前结点的所有分区
    async fn list_all_partitions() -> CoreResult<Vec<(String, String)>>;

    /// 获取分区详细信息（包括 segments）
    async fn get_partition_detail(
        table_name: String,
        partition_name: String,
    ) -> CoreResult<PartitionDetail>;

    async fn create_partition(table_name: String, partition_name: String) -> CoreResult<()>;

    async fn load_partition(table_name: String, partition_name: String) -> CoreResult<()>;

    /// 本地删除分区数据
    async fn drop_partition(table_name: String, partition_name: String) -> CoreResult<()>;

    /// 本地持久化分区数据
    async fn flush_partition(table_name: String, partition_name: String) -> CoreResult<()>;
}

impl RpcDataService for DataService {
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
}
