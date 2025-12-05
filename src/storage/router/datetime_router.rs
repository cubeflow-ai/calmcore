//! Datetime Range 路由器 - 专为时序数据优化的时间分区路由

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;

use crate::catalog::table_meta::TimeGranularity;
use crate::catalog::PartitionStrategy;
use crate::utils::error::{CoreError, CoreResult};

use super::utils::take_rows;
use super::RoutedBatches;

pub struct DatetimeRouter;

impl DatetimeRouter {
    /// DatetimeRange 路由：根据时间戳字段按时间粒度分配分区
    ///
    /// # 参数
    /// - `batch`: 数据批次
    /// - `field`: 时间戳字段名
    /// - `granularity`: 时间粒度
    /// - `timezone`: 时区（可选）
    /// - `parallelism`: 并行度（可选）
    pub fn route(
        batch: RecordBatch,
        field: &str,
        granularity: TimeGranularity,
        timezone: Option<String>,
        parallelism: Option<usize>,
    ) -> CoreResult<RoutedBatches> {
        // 1. 提取时间戳字段
        let column = batch.column_by_name(field).ok_or_else(|| {
            CoreError::Internal(format!("Datetime field '{}' not found in batch", field))
        })?;

        // 2. 计算每行的 partition_name
        let partition_names =
            Self::compute_partition_names(column, granularity, timezone, parallelism)?;

        // 3. 按 partition_name 分组
        Self::split_batch_by_names(batch, partition_names)
    }

    /// 计算每行应该路由到的 partition_name
    fn compute_partition_names(
        column: &Arc<dyn Array>,
        granularity: TimeGranularity,
        timezone: Option<String>,
        parallelism: Option<usize>,
    ) -> CoreResult<Vec<String>> {
        match column.data_type() {
            DataType::Timestamp(time_unit, _) => {
                use datafusion::arrow::datatypes::TimeUnit;

                // 根据不同的时间单位提取毫秒时间戳
                let timestamps_ms: Vec<i64> = match time_unit {
                    TimeUnit::Second => {
                        let array = column
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .ok_or_else(|| {
                                CoreError::Internal(
                                    "Failed to downcast to TimestampSecondArray".into(),
                                )
                            })?;

                        (0..array.len())
                            .map(|i| {
                                if array.is_null(i) {
                                    Err(CoreError::Internal(
                                        "Datetime field contains null values".into(),
                                    ))
                                } else {
                                    Ok(array.value(i) * 1000) // 秒转毫秒
                                }
                            })
                            .collect::<CoreResult<Vec<_>>>()?
                    }
                    TimeUnit::Millisecond => {
                        let array = column
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .ok_or_else(|| {
                                CoreError::Internal(
                                    "Failed to downcast to TimestampMillisecondArray".into(),
                                )
                            })?;

                        (0..array.len())
                            .map(|i| {
                                if array.is_null(i) {
                                    Err(CoreError::Internal(
                                        "Datetime field contains null values".into(),
                                    ))
                                } else {
                                    Ok(array.value(i))
                                }
                            })
                            .collect::<CoreResult<Vec<_>>>()?
                    }
                    TimeUnit::Microsecond => {
                        let array = column
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .ok_or_else(|| {
                                CoreError::Internal(
                                    "Failed to downcast to TimestampMicrosecondArray".into(),
                                )
                            })?;

                        (0..array.len())
                            .map(|i| {
                                if array.is_null(i) {
                                    Err(CoreError::Internal(
                                        "Datetime field contains null values".into(),
                                    ))
                                } else {
                                    Ok(array.value(i) / 1000) // 微秒转毫秒
                                }
                            })
                            .collect::<CoreResult<Vec<_>>>()?
                    }
                    TimeUnit::Nanosecond => {
                        let array = column
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .ok_or_else(|| {
                                CoreError::Internal(
                                    "Failed to downcast to TimestampNanosecondArray".into(),
                                )
                            })?;

                        (0..array.len())
                            .map(|i| {
                                if array.is_null(i) {
                                    Err(CoreError::Internal(
                                        "Datetime field contains null values".into(),
                                    ))
                                } else {
                                    Ok(array.value(i) / 1_000_000) // 纳秒转毫秒
                                }
                            })
                            .collect::<CoreResult<Vec<_>>>()?
                    }
                };

                // 计算分区名
                let mut names = Vec::with_capacity(timestamps_ms.len());
                for timestamp_ms in timestamps_ms {
                    let partition_name = PartitionStrategy::DatetimeRange {
                        field: String::new(), // 这里只需要计算函数，不需要完整结构
                        granularity,
                        timezone: timezone.clone(),
                        parallelism,
                    }
                    .calculate_datetime_partition(timestamp_ms)
                    .ok_or_else(|| {
                        CoreError::Internal(format!(
                            "Failed to calculate datetime partition for timestamp {}",
                            timestamp_ms
                        ))
                    })?;

                    names.push(partition_name);
                }

                Ok(names)
            }

            _ => Err(CoreError::Internal(format!(
                "Datetime field must be Timestamp type, got {:?}",
                column.data_type()
            ))),
        }
    }

    /// 按分区名分割 batch
    fn split_batch_by_names(
        batch: RecordBatch,
        partition_names: Vec<String>,
    ) -> CoreResult<RoutedBatches> {
        // 1. 按 partition_name 分组行索引
        let mut partition_indices: HashMap<String, Vec<usize>> = HashMap::new();

        for (row_idx, partition_name) in partition_names.iter().enumerate() {
            partition_indices
                .entry(partition_name.clone())
                .or_insert_with(Vec::new)
                .push(row_idx);
        }

        // 2. 为每个分区创建 RecordBatch
        let mut result = HashMap::new();

        for (partition_name, indices) in partition_indices {
            let sub_batch = take_rows(&batch, &indices)?;
            result.insert(partition_name, sub_batch);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, TimestampMillisecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_datetime_router_day() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));

        // 创建测试数据：2024-01-01 和 2024-01-02 的数据
        let ts_2024_01_01 = 1704067200000; // 2024-01-01 00:00:00 UTC (毫秒)
        let ts_2024_01_02 = 1704153600000; // 2024-01-02 00:00:00 UTC (毫秒)

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    ts_2024_01_01,
                    ts_2024_01_01 + 3600000, // +1 hour
                    ts_2024_01_02,
                    ts_2024_01_02 + 3600000,
                ])),
            ],
        )
        .unwrap();

        let routed = DatetimeRouter::route(
            batch,
            "created_at",
            TimeGranularity::Day,
            Some("UTC".to_string()),
            None,
        )
        .unwrap();

        // 应该路由到 2 个分区
        assert_eq!(routed.len(), 2);

        // 验证分区名
        assert!(routed.contains_key("20240101"));
        assert!(routed.contains_key("20240102"));

        // 验证每个分区的行数
        assert_eq!(routed.get("20240101").unwrap().num_rows(), 2);
        assert_eq!(routed.get("20240102").unwrap().num_rows(), 2);
    }

    #[test]
    fn test_datetime_router_hour_with_parallelism() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));

        let ts_2024_01_01_08 = 1704096000000; // 2024-01-01 08:00:00 UTC (毫秒)

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    ts_2024_01_01_08,
                    ts_2024_01_01_08 + 1000,
                    ts_2024_01_01_08 + 2000,
                    ts_2024_01_01_08 + 3000,
                ])),
            ],
        )
        .unwrap();

        let routed = DatetimeRouter::route(
            batch,
            "created_at",
            TimeGranularity::Hour,
            Some("UTC".to_string()),
            Some(2), // 2 个并行分区
        )
        .unwrap();

        // 应该路由到 2 个并行分区
        assert_eq!(routed.len(), 2);

        // 验证分区名格式（应该是 2024010108_0 和 2024010108_1）
        let keys: Vec<&String> = routed.keys().collect();
        assert!(keys.iter().any(|k| k.starts_with("2024010108_")));
    }
}
