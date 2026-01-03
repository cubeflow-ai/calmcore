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
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::{union::UnionExec, ExecutionPlan};

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

    /// 表名
    table_name: String,

    /// Engine 引用
    engine: Arc<Engine>,

    /// 是否需要输出 `_internal_id` 列
    emit_internal_id: bool,
}

impl UnionTableProvider {
    /// 创建新的 UnionTableProvider
    ///
    /// # 参数
    /// - `partitions`: 本地 partition
    /// - `table_name`: 表名
    /// - `engine`: Engine 引用
    /// - `schema`: 表的schema
    ///
    /// # 错误
    /// 如果 schema 不一致,返回错误
    pub fn new(
        partitions: Vec<Arc<Partition>>,
        table_name: String,
        engine: Arc<Engine>,
        schema: SchemaRef,
        emit_internal_id: bool,
    ) -> CoreResult<Self> {
        let final_schema = if partitions.is_empty() {
            // 本地没有partition，使用传入的schema
            schema
        } else {
            // 本地有partition，从第一个partition获取schema
            let first_schema = partitions[0].schema().to_arrow_schema();

            // 验证所有本地 partition 的 schema 一致
            for (idx, partition) in partitions.iter().enumerate().skip(1) {
                let part_schema = partition.schema().to_arrow_schema();
                if part_schema != first_schema {
                    return Err(crate::utils::error::CoreError::Internal(format!(
                        "Schema mismatch: partition {} has different schema",
                        idx
                    )));
                }
            }

            first_schema
        };

        Ok(Self {
            schema: final_schema,
            partitions,
            table_name,
            engine,
            emit_internal_id,
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

        // 为每个 partition 创建 PartitionTableProvider 并调用 scan
        let mut partition_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

        for partition in &self.partitions {
            let partition_provider =
                super::PartitionTableProvider::new(partition.clone(), self.emit_internal_id);

            let plan = partition_provider
                .scan(_state, projection, filters, limit)
                .await?;
            partition_plans.push(plan);
        }

        // 使用 UnionExec 合并所有 partition 的 plan
        UnionExec::try_new(partition_plans)
    }
}
