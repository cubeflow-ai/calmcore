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
use datafusion::physical_plan::ExecutionPlan;

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
}

impl UnionTableProvider {
    /// 创建新的 UnionTableProvider
    ///
    /// # 参数
    /// - `partitions`: 所有 partition
    ///
    /// # 错误
    /// 如果 partitions 为空或 schema 不一致,返回错误
    pub fn new(partitions: Vec<Arc<Partition>>) -> CoreResult<Self> {
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

        Ok(Self { schema, partitions })
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

        // 🚀 新策略: 扁平化所有partition的所有segment
        // 让它们都成为DataFusion的独立partition,实现真正的全局并行!
        use crate::compute::table_provider::segment_scanner::SegmentScanner;

        let mut all_segment_scanners = Vec::new();

        for (part_idx, partition) in self.partitions.iter().enumerate() {
            log::info!("📍 [UnionTableProvider] Processing partition {}", part_idx);

            // Add current segment (only if non-empty)
            {
                let current_segment = partition.get_current_segment();
                if current_segment.doc_count() > 0 {
                    let scanner = SegmentScanner::new(
                        self.schema.clone(),
                        current_segment.get_row_data(),
                        current_segment.get_index_readers(),
                        current_segment.doc_count(),
                        current_segment.get_deleted(),
                    );
                    all_segment_scanners.push(scanner);
                }
            }

            // Add frozen segments
            {
                let frozen_segments = partition.get_frozen_segments();
                for (_seg_id, segment) in frozen_segments.iter() {
                    let scanner = SegmentScanner::new(
                        self.schema.clone(),
                        segment.get_row_data(),
                        segment.get_index_readers(),
                        segment.doc_count(),
                        segment.get_deleted(),
                    );
                    all_segment_scanners.push(scanner);
                }
            }
        }

        log::info!(
            "✅ [UnionTableProvider] Total {} segments across {} partitions, creating parallel MultiSegmentExec",
            all_segment_scanners.len(),
            self.partitions.len()
        );

        // 创建一个大的MultiSegmentExec,所有segment都是独立的DataFusion partition
        // DataFusion会自动并行调度它们!
        use crate::compute::table_provider::partition_table_provider::create_multi_segment_exec;

        let exec = create_multi_segment_exec(
            self.schema.clone(),
            all_segment_scanners,
            filters.to_vec(),
            projection.cloned(),
            limit,
        );

        Ok(Arc::new(exec))
    }
}
