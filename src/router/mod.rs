//! 数据路由模块
//!
//! 负责根据分区策略将 RecordBatch 路由到不同的 partition

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;

use crate::catalog::{PartitionStrategy, TableMeta};
use crate::utils::error::CoreResult;

mod hash_router;
mod range_router;
mod utils;

pub use hash_router::HashRouter;
pub use range_router::RangeRouter;
pub use utils::{compute_hash_indices, split_batch_by_indices, take_rows};

/// 路由结果：partition_name -> RecordBatch
pub type RoutedBatches = HashMap<String, RecordBatch>;

/// 数据路由器 trait
pub trait DataRouter: Send + Sync {
    /// 根据分区策略路由 RecordBatch
    fn route_batch(&self, batch: RecordBatch, meta: &Arc<TableMeta>) -> CoreResult<RoutedBatches>;
}

/// 路由器工厂
pub struct Router;

impl Router {
    /// 根据分区策略路由数据
    pub fn route_batch(batch: RecordBatch, meta: &Arc<TableMeta>) -> CoreResult<RoutedBatches> {
        match &meta.partition_strategy {
            PartitionStrategy::PKHash { num_partitions } => {
                HashRouter::route_by_primary_key(batch, meta, *num_partitions)
            }

            PartitionStrategy::Hash {
                field,
                num_partitions,
            } => HashRouter::route_by_field(batch, field, *num_partitions),

            PartitionStrategy::Range {
                field,
                start,
                step,
                parallelism,
            } => RangeRouter::route(batch, field, *start, *step, *parallelism),

            PartitionStrategy::Custom => Err(crate::utils::error::CoreError::Internal(
                "Custom partition requires explicit partition_name".into(),
            )),

            PartitionStrategy::None => {
                // 单分区：所有数据路由到 0000000000000000000
                Ok(HashMap::from([("0000000000000000000".to_string(), batch)]))
            }
        }
    }
}
