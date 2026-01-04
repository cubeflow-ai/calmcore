//! Hash 路由器

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;

use crate::catalog::TableMeta;
use crate::utils::error::{CoreError, CoreResult};

use super::utils::{compute_hash_indices, split_batch_by_indices};
use super::RoutedBatches;

pub struct HashRouter;

impl HashRouter {
    /// PKHash 路由：使用主键字段
    pub fn route_by_primary_key(
        batch: RecordBatch,
        meta: &Arc<TableMeta>,
        num_partitions: usize,
    ) -> CoreResult<RoutedBatches> {
        // 获取主键字段名
        let pk_field = meta
            .schema
            .primary_key
            .as_ref()
            .ok_or_else(|| CoreError::Internal("PKHash requires primary key".into()))?;

        Self::route_by_field(batch, pk_field, num_partitions)
    }

    /// Hash 路由：使用指定字段
    pub fn route_by_field(
        batch: RecordBatch,
        field: &str,
        num_partitions: usize,
    ) -> CoreResult<RoutedBatches> {
        // 1. 提取字段列
        let column = batch.column_by_name(field).ok_or_else(|| {
            CoreError::Internal(format!("Hash field '{}' not found in batch", field))
        })?;

        // 2. 计算每行的 partition index
        let partition_indices = compute_hash_indices(column, num_partitions)?;

        // 3. 按 partition 分组
        split_batch_by_indices(batch, partition_indices, num_partitions)
    }
}
