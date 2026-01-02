//! Range 路由器

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;

use crate::catalog::PartitionStrategy;
use crate::utils::error::{CoreError, CoreResult};

use super::utils::take_rows;
use super::RoutedBatches;

pub struct RangeRouter;

impl RangeRouter {
    /// Range 路由：根据字段值的范围分配分区
    pub fn route(
        batch: RecordBatch,
        field: &str,
        start: i64,
        step: i64,
        parallelism: Option<usize>,
    ) -> CoreResult<RoutedBatches> {
        if step <= 0 {
            return Err(CoreError::Internal(format!(
                "Range step must be positive, got {}",
                step
            )));
        }

        // 1. 提取 range 字段
        let column = batch.column_by_name(field).ok_or_else(|| {
            CoreError::Internal(format!("Range field '{}' not found in batch", field))
        })?;

        // 2. 计算每行的 partition_name
        let partition_names = Self::compute_partition_names(column, start, step, parallelism)?;

        // 3. 按 partition_name 分组
        Self::split_batch_by_names(batch, partition_names)
    }

    /// 计算每行应该路由到的 partition_name
    fn compute_partition_names(
        column: &Arc<dyn Array>,
        start: i64,
        step: i64,
        parallelism: Option<usize>,
    ) -> CoreResult<Vec<String>> {
        match column.data_type() {
            // 支持 Int64 类型（普通整数字段）
            DataType::Int64 => {
                let array = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        CoreError::Internal("Failed to downcast to Int64Array".into())
                    })?;

                let mut names = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if array.is_null(i) {
                        return Err(CoreError::Internal(
                            "Range field contains null values".into(),
                        ));
                    }

                    let value = array.value(i);
                    let partition_name =
                        Self::calculate_partition_name(value, start, step, parallelism);
                    names.push(partition_name);
                }

                Ok(names)
            }

            // 支持 Timestamp 类型（毫秒精度）
            DataType::Timestamp(time_unit, _) => {
                use datafusion::arrow::datatypes::TimeUnit;

                match time_unit {
                    TimeUnit::Millisecond => {
                        let array = column
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .ok_or_else(|| {
                                CoreError::Internal(
                                    "Failed to downcast to TimestampMillisecondArray".into(),
                                )
                            })?;

                        let mut names = Vec::with_capacity(array.len());
                        for i in 0..array.len() {
                            if array.is_null(i) {
                                return Err(CoreError::Internal(
                                    "Range field contains null values".into(),
                                ));
                            }

                            let value = array.value(i); // 毫秒时间戳
                            let partition_name =
                                Self::calculate_partition_name(value, start, step, parallelism);
                            names.push(partition_name);
                        }

                        Ok(names)
                    }

                    TimeUnit::Second => {
                        // 秒级时间戳：需要转换为毫秒
                        let array =
                            column
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .ok_or_else(|| {
                                    CoreError::Internal("Failed to downcast timestamp array".into())
                                })?;

                        let mut names = Vec::with_capacity(array.len());
                        for i in 0..array.len() {
                            if array.is_null(i) {
                                return Err(CoreError::Internal(
                                    "Range field contains null values".into(),
                                ));
                            }

                            let value_seconds = array.value(i);
                            let value_millis = value_seconds * 1000; // 转换为毫秒
                            let partition_name = Self::calculate_partition_name(
                                value_millis,
                                start,
                                step,
                                parallelism,
                            );
                            names.push(partition_name);
                        }

                        Ok(names)
                    }

                    TimeUnit::Microsecond => {
                        // 微秒级时间戳：需要转换为毫秒
                        let array =
                            column
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .ok_or_else(|| {
                                    CoreError::Internal("Failed to downcast timestamp array".into())
                                })?;

                        let mut names = Vec::with_capacity(array.len());
                        for i in 0..array.len() {
                            if array.is_null(i) {
                                return Err(CoreError::Internal(
                                    "Range field contains null values".into(),
                                ));
                            }

                            let value_micros = array.value(i);
                            let value_millis = value_micros / 1000; // 转换为毫秒
                            let partition_name = Self::calculate_partition_name(
                                value_millis,
                                start,
                                step,
                                parallelism,
                            );
                            names.push(partition_name);
                        }

                        Ok(names)
                    }

                    TimeUnit::Nanosecond => {
                        // 纳秒级时间戳：需要转换为毫秒
                        let array =
                            column
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .ok_or_else(|| {
                                    CoreError::Internal("Failed to downcast timestamp array".into())
                                })?;

                        let mut names = Vec::with_capacity(array.len());
                        for i in 0..array.len() {
                            if array.is_null(i) {
                                return Err(CoreError::Internal(
                                    "Range field contains null values".into(),
                                ));
                            }

                            let value_nanos = array.value(i);
                            let value_millis = value_nanos / 1_000_000; // 转换为毫秒
                            let partition_name = Self::calculate_partition_name(
                                value_millis,
                                start,
                                step,
                                parallelism,
                            );
                            names.push(partition_name);
                        }

                        Ok(names)
                    }
                }
            }

            _ => Err(CoreError::Internal(format!(
                "Range partition supports Int64 and Timestamp fields, got {:?}",
                column.data_type()
            ))),
        }
    }

    /// 计算单个值的 partition_name（支持并行度）
    fn calculate_partition_name(
        value: i64,
        start: i64,
        step: i64,
        parallelism: Option<usize>,
    ) -> String {
        let offset = value - start;
        let partition_index = offset / step;
        let partition_start = start + (partition_index * step);

        let base_name = PartitionStrategy::format_partition_id(partition_start);

        // 如果启用并行度，随机选择子分区
        if let Some(p) = parallelism {
            if p > 1 {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};

                let mut hasher = DefaultHasher::new();
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                now.hash(&mut hasher);
                value.hash(&mut hasher);

                let hash = hasher.finish();
                let parallel_index = (hash as usize) % p;
                return format!("{}_{}", base_name, parallel_index);
            }
        }

        base_name
    }

    /// 按 partition_name 分割 RecordBatch
    fn split_batch_by_names(
        batch: RecordBatch,
        partition_names: Vec<String>,
    ) -> CoreResult<RoutedBatches> {
        // 分组：partition_name -> row_indices
        let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
        for (row_idx, partition_name) in partition_names.iter().enumerate() {
            groups
                .entry(partition_name.clone())
                .or_default()
                .push(row_idx);
        }

        // 为每个分区创建 RecordBatch
        let mut result = HashMap::new();
        for (partition_name, row_indices) in groups {
            let partition_batch = take_rows(&batch, &row_indices)?;
            result.insert(partition_name, partition_batch);
        }

        Ok(result)
    }
}
