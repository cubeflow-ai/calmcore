//! UnionTable: 将多个 Partition 统一为单一的 TableProvider
//!
//! 核心功能:
//! 1. 实现 DataFusion 的 TableProvider trait
//! 2. 复用 PartitionTableProvider 的优化能力 (filter/projection/limit下推)
//! 3. 使用 UnionExec 合并多个 partition 的结果
//!
//! 优势:
//! - 完全利用 PartitionTableProvider 已有的索引优化
//! - 支持 WHERE/projection/LIMIT 下推
//! - 避免全表扫描
//! - 流式处理,避免内存爆炸

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;

use crate::engine::Engine;
use crate::partition::Partition;
use crate::utils::error::CoreResult;

/// UnionTableProvider: 统一多个 Partition 的 TableProvider
///
/// **设计思路**:
/// - 每个Partition已经有PartitionTableProvider,支持filter/projection/limit下推
/// - UnionTableProvider不重复实现这些优化,而是复用PartitionTableProvider
/// - 使用DataFusion内置的UnionExec来合并多个partition的ExecutionPlan
pub struct UnionTableProvider {
    /// 表的 schema
    schema: SchemaRef,

    /// 所有 partition
    partitions: Vec<Arc<Partition>>,

    /// 表名 (用于 LazyPartitionExec)
    table_name: String,

    /// Engine 引用 (用于 LazyPartitionExec)
    engine: Arc<Engine>,

    /// Partition owners 映射 (partition_name -> owner_node_id)
    partition_owners: std::collections::HashMap<String, String>,
}

impl UnionTableProvider {
    /// 创建新的 UnionTableProvider
    ///
    /// # 参数
    /// - `partitions`: 所有 partition
    /// - `table_name`: 表名
    /// - `engine`: Engine 引用
    ///
    /// # 错误
    /// 如果 partitions 为空或 schema 不一致,返回错误
    pub fn new(
        partitions: Vec<Arc<Partition>>,
        table_name: String,
        engine: Arc<Engine>,
        partition_owners: std::collections::HashMap<String, String>,
    ) -> CoreResult<Self> {
        if partitions.is_empty() {
            return Err(crate::utils::error::CoreError::Internal(
                "UnionTableProvider requires at least one partition".to_string(),
            ));
        }

        // 获取第一个 partition 的 schema 作为基准
        let schema = partitions[0].schema().to_arrow_schema();

        // 验证所有 partition 的 schema 一致
        for (idx, partition) in partitions.iter().enumerate().skip(1) {
            let part_schema = partition.schema().to_arrow_schema();
            if part_schema != schema {
                return Err(crate::utils::error::CoreError::Internal(format!(
                    "Schema mismatch: partition {} has different schema",
                    idx
                )));
            }
        }

        Ok(Self {
            schema,
            partitions,
            table_name,
            engine,
            partition_owners,
        })
    }
}

impl std::fmt::Debug for UnionTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnionTableProvider")
            .field("schema", &self.schema)
            .field("num_partitions", &self.partitions.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for UnionTableProvider {
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
        // 告诉DataFusion我们支持filter下推 (Inexact表示我们会尽力处理)
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        log::info!(
            "📊 [UnionTableProvider::scan] Scanning {} partitions, projection={:?}, filters={}, limit={:?}",
            self.partitions.len(),
            projection,
            filters.len(),
            limit
        );

        // 🎯 分布式方案：返回 LazyPartitionExec（只包含元数据）
        // LazyPartitionExec 可以被序列化并发送到远程节点
        // 远程节点执行时会从本地 Engine 加载数据

        use crate::compute::LazyPartitionExec;

        // 🔑 关键：在分布式模式下，partition_names 应该包含所有 partition（不仅仅是本地的）
        // 这样当 plan 被发送到其他节点时，每个节点可以根据 partition_owners 判断哪些是自己的
        let partition_names: Vec<String> = if self.partition_owners.is_empty() {
            // 单机模式或没有 owner 信息：只用本地 partitions
            self.partitions
                .iter()
                .map(|p| p.name().to_string())
                .collect()
        } else {
            // 分布式模式：使用 partition_owners 的所有 keys（包含所有节点的 partition）
            self.partition_owners.keys().cloned().collect()
        };

        log::info!(
            "✅ [UnionTableProvider] Creating LazyPartitionExec with {} partitions: {:?}",
            partition_names.len(),
            partition_names
        );

        log::info!(
            "🗺️  [UnionTableProvider] Partition owners map: {:?}",
            self.partition_owners
        );

        // 创建 LazyPartitionExec（类似 ParquetExec，只包含元数据）
        let lazy_exec = LazyPartitionExec::new(
            self.table_name.clone(),
            partition_names,
            self.partition_owners.clone(),
            self.schema.clone(),
            filters.to_vec(),
            projection.cloned(),
            limit,
            self.engine.clone(),
        );

        Ok(Arc::new(lazy_exec))
    }
}
