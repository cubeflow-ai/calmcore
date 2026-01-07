//! MixedTableProvider: 统一处理本地和远程分区的 TableProvider
//!
//! 设计理念：
//! 1. 单一的 TableProvider，返回统一的 schema
//! 2. scan() 内部区分本地和远程分区
//! 3. 本地分区 → PartitionExec
//! 4. 远程分区 → RemoteScanExec  
//! 5. 使用 UnionExec 合并所有 ExecutionPlan
//!
//! 优势：
//! - 避免多个 temp table 的 schema 不一致问题
//! - 统一的优化路径
//! - 更简洁的代码结构

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::{union::UnionExec, ExecutionPlan};

use crate::compute::table_provider::PartitionTableProvider;
use crate::engine::Engine;
use crate::partition::Partition;
use crate::utils::error::CoreResult;

use super::flight_executor::FlightExecutor;
use super::remote_scan_exec::RemoteScanExec;

/// 节点分区信息
pub struct NodePartitions {
    /// 节点 ID
    pub node_id: String,
    /// 该节点拥有的分区名称列表
    pub partition_names: Vec<String>,
    /// gRPC 地址（仅远程节点需要）
    pub grpc_addr: Option<String>,
}

/// MixedTableProvider: 统一处理本地和远程分区
pub struct MixedTableProvider {
    /// 表名
    table_name: String,
    /// 表的 schema
    schema: SchemaRef,
    /// 本地节点 ID
    local_node_id: String,
    /// 本地分区
    #[allow(dead_code)]
    local_partitions: Vec<Arc<Partition>>,
    /// 远程节点分区映射
    remote_nodes: Vec<NodePartitions>,
    /// Engine 引用
    engine: Arc<Engine>,
    /// 是否需要输出 _internal_id
    emit_internal_id: bool,
    /// 是否为 COUNT(*) 优化查询
    count_only: bool,
    /// 查询的 LIMIT（从 SQL 提取）
    query_limit: Option<usize>,
}

impl MixedTableProvider {
    pub fn new(
        table_name: String,
        schema: SchemaRef,
        local_node_id: String,
        local_partitions: Vec<Arc<Partition>>,
        remote_nodes: Vec<NodePartitions>,
        engine: Arc<Engine>,
        emit_internal_id: bool,
        count_only: bool,
        query_limit: Option<usize>,
    ) -> Self {
        Self {
            table_name,
            schema,
            local_node_id,
            local_partitions,
            remote_nodes,
            engine,
            emit_internal_id,
            count_only,
            query_limit,
        }
    }
}

impl std::fmt::Debug for MixedTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MixedTableProvider")
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .field("local_node_id", &self.local_node_id)
            .field("num_local_partitions", &self.local_partitions.len())
            .field("num_remote_nodes", &self.remote_nodes.len())
            .field("emit_internal_id", &self.emit_internal_id)
            .finish()
    }
}

#[async_trait]
impl TableProvider for MixedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // 🎯 COUNT(*) 优化: 如果是 count_only 查询，所有 filters 都已在索引层处理
        // 返回 Exact 告诉 DataFusion 不要再添加 FilterExec
        if self.count_only {
            return Ok(vec![TableProviderFilterPushDown::Exact; filters.len()]);
        }

        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // 🔧 LIMIT 优化: 如果 DataFusion 不传递 limit（多分区场景），使用查询的 limit
        // 这样可以减少远程节点的数据读取和网络传输
        let effective_limit = limit.or(self.query_limit);

        log::info!(
            "📊 [MixedTableProvider::scan] table={}, local_partitions={}, remote_nodes={}, projection={:?}, filters={}, datafusion_limit={:?}, query_limit={:?}, effective_limit={:?}",
            self.table_name,
            self.local_partitions.len(),
            self.remote_nodes.len(),
            projection,
            filters.len(),
            limit,
            self.query_limit,
            effective_limit
        );

        let mut all_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

        // 1. 扫描本地分区
        for partition in &self.local_partitions {
            let partition_provider = PartitionTableProvider::new(
                partition.clone(),
                self.emit_internal_id,
                self.count_only,
            );
            let plan = partition_provider
                .scan(state, projection, filters, effective_limit)
                .await?;
            all_plans.push(plan);
        }

        // 2. 扫描远程分区
        for node_info in &self.remote_nodes {
            if node_info.partition_names.is_empty() {
                continue;
            }

            let grpc_addr = node_info.grpc_addr.as_ref().ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Node {} has no gRPC address",
                    node_info.node_id
                ))
            })?;

            log::debug!(
                "🌐 [MixedTableProvider::scan] Creating remote scan for node={}, partitions={:?}, addr={}",
                node_info.node_id,
                node_info.partition_names,
                grpc_addr
            );

            let executor = Arc::new(FlightExecutor::new(
                node_info.node_id.clone(),
                grpc_addr.clone(),
            ));

            // 创建 RemoteScanExec，直接传递 projection/filters/effective_limit/count_only
            let remote_exec = RemoteScanExec::new(
                self.table_name.clone(),
                node_info.partition_names.clone(),
                self.schema.clone(),
                projection.cloned(),
                filters.to_vec(),
                effective_limit,
                self.count_only,
                executor,
            );

            all_plans.push(Arc::new(remote_exec));
        }

        // 3. 如果没有任何分区，返回空表
        if all_plans.is_empty() {
            log::debug!(
                "⚠️  [MixedTableProvider::scan] No partitions found, returning empty table"
            );
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                self.schema.clone(),
            )));
        }

        // 4. 如果只有一个 plan，直接返回
        if all_plans.len() == 1 {
            return Ok(all_plans.into_iter().next().unwrap());
        }

        // 5. 使用 UnionExec 合并所有 plan
        log::debug!(
            "🔗 [MixedTableProvider::scan] Unioning {} plans",
            all_plans.len()
        );
        let union_plan: Arc<dyn ExecutionPlan> = UnionExec::try_new(all_plans)?;

        // 6. 如果有 limit，在 UnionExec 之上包装 GlobalLimitExec
        // 这样可以确保 LIMIT 语义正确（跨多个分区/节点只返回总共 N 行）
        if let Some(limit_val) = effective_limit {
            log::debug!(
                "🔢 [MixedTableProvider::scan] Applying GlobalLimitExec with limit={}",
                limit_val
            );
            use datafusion::physical_plan::limit::GlobalLimitExec;
            Ok(Arc::new(GlobalLimitExec::new(
                union_plan,
                0,               // skip: 0（OFFSET 通过其他方式处理）
                Some(limit_val), // fetch: limit
            )))
        } else {
            Ok(union_plan)
        }
    }
}
