/// TOP-K 合并器
///
/// 类似 Elasticsearch 的做法：
/// 1. 每个 partition 返回 TOP-K 结果
/// 2. 协调节点合并所有 partition 的结果
/// 3. 全局排序并应用最终 LIMIT
use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::compute::SortColumn;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DFResult;

/// TOP-K 合并器
pub struct TopKMerger {
    /// 排序字段 [(field_name, ascending)]
    sort_fields: Vec<(String, bool)>,

    /// 最终 LIMIT
    final_limit: usize,

    /// OFFSET（可选）
    final_offset: usize,
}

impl TopKMerger {
    /// 创建新的 TOP-K 合并器
    ///
    /// # Arguments
    /// * `sort_fields` - 排序字段列表 [(field_name, ascending)]
    /// * `limit` - 最终 LIMIT
    /// * `offset` - OFFSET（默认 0）
    pub fn new(sort_fields: Vec<(String, bool)>, limit: usize, offset: Option<usize>) -> Self {
        Self {
            sort_fields,
            final_limit: limit,
            final_offset: offset.unwrap_or(0),
        }
    }

    /// 计算每个 partition 应该返回多少条数据
    ///
    /// 策略：每个 partition 返回 limit + offset 条数据
    ///
    /// 原理：
    /// - 每个 partition 返回自己的 TOP-(limit+offset)
    /// - 协调节点合并所有 partition 的结果后，再做全局排序
    /// - 最终取全局的 TOP-(limit+offset)
    ///
    /// 这样可以保证无论数据如何分布，都能得到正确的全局 TOP-K
    pub fn calculate_per_partition_limit(&self, _num_partitions: usize) -> usize {
        self.final_limit + self.final_offset
    }

    /// 合并多个 partition 的结果
    ///
    /// # Arguments
    /// * `partition_results` - 每个 partition 的查询结果（已经是 TOP-K）
    ///
    /// # Returns
    /// 全局 TOP-K 结果（单个合并后的 RecordBatch）
    pub fn merge(&self, partition_results: Vec<Vec<RecordBatch>>) -> DFResult<RecordBatch> {
        // 1. 合并所有 partition 的 batches
        let mut all_batches = Vec::new();
        for batches in partition_results {
            all_batches.extend(batches);
        }

        if all_batches.is_empty() {
            // 返回空的 RecordBatch（如果有schema的话）
            // 这里我们需要一个schema,从第一个partition获取
            return Err(datafusion::error::DataFusionError::Internal(
                "No data to merge".to_string(),
            ));
        }

        // 2. 合并成单个 RecordBatch（如果有多个）
        let schema = all_batches[0].schema();
        let merged_batch = if all_batches.len() == 1 {
            all_batches.into_iter().next().unwrap()
        } else {
            concat_batches(&schema, &all_batches)?
        };

        // 3. 全局排序
        let sorted_batch = self.sort_batch(merged_batch)?;

        // 4. 应用 OFFSET 和 LIMIT
        let final_batch = self.apply_offset_limit(sorted_batch)?;

        Ok(final_batch)
    }

    /// 对 RecordBatch 进行排序
    fn sort_batch(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        use datafusion::arrow::compute::lexsort_to_indices;
        use datafusion::arrow::compute::take;

        if batch.num_rows() == 0 {
            return Ok(batch);
        }

        let schema = batch.schema();

        // 构建排序列
        let mut sort_columns = Vec::new();
        for (field_name, ascending) in &self.sort_fields {
            // 查找字段索引（不区分大小写）
            let field_name_lower = field_name.to_lowercase();
            let field_index = schema
                .fields()
                .iter()
                .position(|f| f.name().to_lowercase() == field_name_lower)
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Sort field '{}' not found in schema. Available fields: {:?}",
                        field_name,
                        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                    ))
                })?;

            let column = batch.column(field_index).clone();
            sort_columns.push(SortColumn {
                values: column,
                options: Some(datafusion::arrow::compute::SortOptions {
                    descending: !ascending,
                    nulls_first: false,
                }),
            });
        }

        // 计算排序索引
        let indices = lexsort_to_indices(&sort_columns, None)?;

        // 应用排序
        let sorted_columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

        let result = RecordBatch::try_new(schema, sorted_columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

        Ok(result)
    }

    /// 应用 OFFSET 和 LIMIT
    fn apply_offset_limit(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        let total_rows = batch.num_rows();

        // 计算实际的起始和结束位置
        let start = self.final_offset.min(total_rows);
        let end = (self.final_offset + self.final_limit).min(total_rows);

        if start >= end {
            // 没有数据 - 创建空的 RecordBatch，但保持正确的列数
            use datafusion::arrow::array::new_empty_array;
            let schema = batch.schema();
            let empty_columns: Vec<ArrayRef> = schema
                .fields()
                .iter()
                .map(|field| new_empty_array(field.data_type()))
                .collect();

            return RecordBatch::try_new(schema, empty_columns)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None));
        }

        // 切片
        let sliced = batch.slice(start, end - start);

        Ok(sliced)
    }
}

/// 合并多个 RecordBatch
fn concat_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> DFResult<RecordBatch> {
    use datafusion::arrow::compute::concat_batches as arrow_concat;

    arrow_concat(schema, batches)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// 比较两行数据（用于排序）
///
/// 返回值：
/// - Ordering::Less: row1 < row2
/// - Ordering::Equal: row1 == row2
/// - Ordering::Greater: row1 > row2
#[allow(dead_code)]
fn compare_rows(
    batch: &RecordBatch,
    row1: usize,
    row2: usize,
    sort_fields: &[(String, bool)],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    for (field_name, ascending) in sort_fields {
        // 查找字段
        let field_index = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == field_name);

        if field_index.is_none() {
            continue;
        }

        let field_index = field_index.unwrap();
        let column = batch.column(field_index);

        // 比较值
        let cmp = compare_values(column, row1, row2);

        if cmp != Ordering::Equal {
            return if *ascending { cmp } else { cmp.reverse() };
        }
    }

    Ordering::Equal
}

/// 比较两个值
fn compare_values(array: &ArrayRef, idx1: usize, idx2: usize) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    // 处理 NULL
    if array.is_null(idx1) && array.is_null(idx2) {
        return Ordering::Equal;
    }
    if array.is_null(idx1) {
        return Ordering::Less; // NULL 排在前面
    }
    if array.is_null(idx2) {
        return Ordering::Greater;
    }

    // 根据数据类型比较
    match array.data_type() {
        DataType::Int8 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Int8Type>();
            arr.value(idx1).cmp(&arr.value(idx2))
        }
        DataType::Int16 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Int16Type>();
            arr.value(idx1).cmp(&arr.value(idx2))
        }
        DataType::Int32 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Int32Type>();
            arr.value(idx1).cmp(&arr.value(idx2))
        }
        DataType::Int64 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
            arr.value(idx1).cmp(&arr.value(idx2))
        }
        DataType::UInt8 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::UInt8Type>();
            arr.value(idx1).cmp(&arr.value(idx2))
        }
        DataType::UInt16 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::UInt16Type>();
            arr.value(idx1).cmp(&arr.value(idx2))
        }
        DataType::UInt32 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::UInt32Type>();
            arr.value(idx1).cmp(&arr.value(idx2))
        }
        DataType::UInt64 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::UInt64Type>();
            arr.value(idx1).cmp(&arr.value(idx2))
        }
        DataType::Float32 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Float32Type>();
            let v1 = arr.value(idx1);
            let v2 = arr.value(idx2);
            v1.partial_cmp(&v2).unwrap_or(Ordering::Equal)
        }
        DataType::Float64 => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
            let v1 = arr.value(idx1);
            let v2 = arr.value(idx2);
            v1.partial_cmp(&v2).unwrap_or(Ordering::Equal)
        }
        DataType::Utf8 => {
            let arr = array.as_string::<i32>();
            arr.value(idx1).cmp(arr.value(idx2))
        }
        DataType::Boolean => {
            let arr = array.as_boolean();
            arr.value(idx1).cmp(&arr.value(idx2))
        }
        _ => Ordering::Equal, // 不支持的类型
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_calculate_per_partition_limit() {
        // LIMIT 10, OFFSET 0 -> 每个 partition 返回 10
        let merger = TopKMerger::new(vec![("age".to_string(), true)], 10, None);
        assert_eq!(merger.calculate_per_partition_limit(4), 10);

        // LIMIT 10, OFFSET 5 -> 每个 partition 返回 15
        let merger = TopKMerger::new(vec![("age".to_string(), true)], 10, Some(5));
        assert_eq!(merger.calculate_per_partition_limit(4), 15);

        // LIMIT 200, OFFSET 0 -> 每个 partition 返回 200
        let merger = TopKMerger::new(vec![("age".to_string(), true)], 200, None);
        assert_eq!(merger.calculate_per_partition_limit(4), 200);
    }

    #[test]
    fn test_merge_and_sort() {
        // 创建测试数据
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Partition 1: age = [25, 30, 35]
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![25, 30, 35])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();

        // Partition 2: age = [20, 28, 40]
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![4, 5, 6])),
                Arc::new(Int64Array::from(vec![20, 28, 40])),
                Arc::new(StringArray::from(vec!["David", "Eve", "Frank"])),
            ],
        )
        .unwrap();

        // 合并并排序（按 age ASC）
        let merger = TopKMerger::new(vec![("age".to_string(), true)], 3, None);
        let result = merger.merge(vec![vec![batch1], vec![batch2]]).unwrap();

        assert_eq!(result.num_rows(), 3);

        // 验证排序结果：应该是 age = [20, 25, 28]
        let age_col = result
            .column(1)
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
        assert_eq!(age_col.value(0), 20);
        assert_eq!(age_col.value(1), 25);
        assert_eq!(age_col.value(2), 28);
    }

    #[test]
    fn test_offset_limit() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int64Array::from(vec![20, 25, 30, 35, 40])),
            ],
        )
        .unwrap();

        // LIMIT 2 OFFSET 1 -> 应该返回 [2, 3]
        let merger = TopKMerger::new(vec![("age".to_string(), true)], 2, Some(1));
        let result = merger.apply_offset_limit(batch).unwrap();

        assert_eq!(result.num_rows(), 2);
        let id_col = result
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
        assert_eq!(id_col.value(0), 2);
        assert_eq!(id_col.value(1), 3);
    }
}
