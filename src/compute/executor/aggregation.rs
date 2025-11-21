use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;

use crate::utils::error::CoreResult;

/// 聚合结果合并器
///
/// 负责将多个分区的聚合结果合并成最终结果
/// 支持: COUNT, SUM, AVG, MAX, MIN, GROUP BY
pub struct AggregationMerger;

impl AggregationMerger {
    pub fn new() -> Self {
        Self
    }

    /// 合并各分区的聚合结果
    ///
    /// # 支持的场景
    /// 1. 简单聚合: SELECT COUNT(*), SUM(x), AVG(y) FROM table
    /// 2. GROUP BY: SELECT group_col, COUNT(*), SUM(x) FROM table GROUP BY group_col
    pub fn merge_aggregation_results(
        &self,
        partition_results: &[Vec<RecordBatch>],
        sql: &str,
    ) -> CoreResult<RecordBatch> {
        let query_lower = sql.to_lowercase();

        // 检查是否有 GROUP BY
        if query_lower.contains("group by") {
            self.merge_grouped_aggregation(partition_results, sql)
        } else {
            self.merge_simple_aggregation(partition_results, sql)
        }
    }

    /// 合并简单聚合（无 GROUP BY）
    fn merge_simple_aggregation(
        &self,
        partition_results: &[Vec<RecordBatch>],
        sql: &str,
    ) -> CoreResult<RecordBatch> {
        let query_lower = sql.to_lowercase();

        // 从第一个分区获取 schema
        let first_batch = partition_results
            .first()
            .and_then(|batches| batches.first())
            .ok_or_else(|| {
                crate::utils::error::CoreError::Internal("No results from partitions".to_string())
            })?;

        let schema = first_batch.schema();
        let num_columns = first_batch.num_columns();

        eprintln!(
            "🔍 [AggregationMerger] Merging {} columns from {} partitions",
            num_columns,
            partition_results.len()
        );

        // 为每一列准备合并后的数组
        let mut merged_columns: Vec<ArrayRef> = Vec::new();

        for col_idx in 0..num_columns {
            let field = schema.field(col_idx);

            eprintln!(
                "🔍 [AggregationMerger] Processing column {}: {} (type: {:?})",
                col_idx,
                field.name(),
                field.data_type()
            );

            // 判断聚合类型
            let agg_value = if query_lower.contains("count(") {
                self.merge_count_column(partition_results, col_idx)?
            } else if query_lower.contains("sum(") {
                self.merge_sum_column(partition_results, col_idx, field.data_type())?
            } else if query_lower.contains("avg(") {
                self.merge_avg_column(partition_results, col_idx, field.data_type())?
            } else if query_lower.contains("max(") {
                self.merge_max_column(partition_results, col_idx, field.data_type())?
            } else if query_lower.contains("min(") {
                self.merge_min_column(partition_results, col_idx, field.data_type())?
            } else {
                // 默认：取第一个分区的值
                partition_results[0][0].column(col_idx).clone()
            };

            merged_columns.push(agg_value);
        }

        // 构建合并后的 RecordBatch
        let merged_batch = RecordBatch::try_new(schema.clone(), merged_columns).map_err(|e| {
            crate::utils::error::CoreError::Internal(format!(
                "Failed to create merged batch: {}",
                e
            ))
        })?;

        eprintln!(
            "🔍 [AggregationMerger] Merged result: {} rows, {} columns",
            merged_batch.num_rows(),
            merged_batch.num_columns()
        );

        Ok(merged_batch)
    }

    /// 合并分组聚合（有 GROUP BY）
    ///
    /// 算法：
    /// 1. 收集所有分区的 (group_key -> aggregation_values) 映射
    /// 2. 对相同 group_key 的值进行二次聚合
    /// 3. 构造最终的 RecordBatch
    fn merge_grouped_aggregation(
        &self,
        partition_results: &[Vec<RecordBatch>],
        sql: &str,
    ) -> CoreResult<RecordBatch> {
        let query_lower = sql.to_lowercase();

        // 从第一个分区获取 schema
        let first_batch = partition_results
            .first()
            .and_then(|batches| batches.first())
            .ok_or_else(|| {
                crate::utils::error::CoreError::Internal("No results from partitions".to_string())
            })?;

        let schema = first_batch.schema();
        let num_columns = first_batch.num_columns();

        eprintln!(
            "🔍 [AggregationMerger] Merging GROUP BY results: {} columns from {} partitions",
            num_columns,
            partition_results.len()
        );

        // 推断哪些列是 GROUP BY 列，哪些是聚合列
        // 简化假设：第一列是 GROUP BY 列，其余是聚合列
        // TODO: 更复杂的 SQL 解析
        if num_columns < 2 {
            return Err(crate::utils::error::CoreError::InvalidParam(
                "GROUP BY query must have at least 2 columns (group + aggregation)".to_string(),
            ));
        }

        let group_col_idx = 0;
        let group_field = schema.field(group_col_idx);

        // 收集所有分区的数据到 HashMap
        // key: group_key, value: Vec<aggregation_values>
        let mut grouped_data: HashMap<String, Vec<Vec<Option<f64>>>> = HashMap::new();

        for batches in partition_results {
            for batch in batches {
                let num_rows = batch.num_rows();

                for row_idx in 0..num_rows {
                    // 提取 group key
                    let group_key = self.extract_group_key(batch, group_col_idx, row_idx)?;

                    // 提取聚合值
                    let mut agg_values = Vec::new();
                    for col_idx in 1..num_columns {
                        let value = self.extract_numeric_value(batch, col_idx, row_idx);
                        agg_values.push(value);
                    }

                    grouped_data
                        .entry(group_key)
                        .or_insert_with(Vec::new)
                        .push(agg_values);
                }
            }
        }

        eprintln!(
            "🔍 [AggregationMerger] Found {} unique groups",
            grouped_data.len()
        );

        // 构造最终结果
        let mut final_group_keys = Vec::new();
        let mut final_agg_columns: Vec<Vec<f64>> = vec![Vec::new(); num_columns - 1];

        for (group_key, rows) in grouped_data.iter() {
            final_group_keys.push(group_key.clone());

            // 对每个聚合列进行合并
            for agg_idx in 0..(num_columns - 1) {
                let col_idx = agg_idx + 1;
                let _field = schema.field(col_idx);

                // 收集该组在该聚合列的所有值
                let values: Vec<f64> = rows.iter().filter_map(|row| row[agg_idx]).collect();

                // 根据聚合类型合并
                let merged_value = if query_lower.contains("count(") {
                    values.iter().sum()
                } else if query_lower.contains("sum(") {
                    values.iter().sum()
                } else if query_lower.contains("avg(") {
                    if values.is_empty() {
                        0.0
                    } else {
                        values.iter().sum::<f64>() / values.len() as f64
                    }
                } else if query_lower.contains("max(") {
                    values.iter().cloned().fold(f64::MIN, f64::max)
                } else if query_lower.contains("min(") {
                    values.iter().cloned().fold(f64::MAX, f64::min)
                } else {
                    values.first().cloned().unwrap_or(0.0)
                };

                final_agg_columns[agg_idx].push(merged_value);
            }
        }

        // 构造 RecordBatch
        let mut columns: Vec<ArrayRef> = Vec::new();

        // Group column (假设是字符串)
        match group_field.data_type() {
            DataType::Utf8 => {
                columns.push(Arc::new(StringArray::from(final_group_keys)));
            }
            DataType::Int64 => {
                let int_keys: Vec<i64> = final_group_keys
                    .iter()
                    .map(|s| s.parse().unwrap_or(0))
                    .collect();
                columns.push(Arc::new(Int64Array::from(int_keys)));
            }
            _ => {
                return Err(crate::utils::error::CoreError::InvalidParam(format!(
                    "Unsupported GROUP BY column type: {:?}",
                    group_field.data_type()
                )));
            }
        }

        // Aggregation columns
        for agg_idx in 0..(num_columns - 1) {
            let col_idx = agg_idx + 1;
            let field = schema.field(col_idx);

            let values = &final_agg_columns[agg_idx];

            match field.data_type() {
                DataType::Int64 => {
                    let int_values: Vec<i64> = values.iter().map(|&v| v as i64).collect();
                    columns.push(Arc::new(Int64Array::from(int_values)));
                }
                DataType::Float64 => {
                    columns.push(Arc::new(Float64Array::from(values.clone())));
                }
                _ => {
                    return Err(crate::utils::error::CoreError::InvalidParam(format!(
                        "Unsupported aggregation column type: {:?}",
                        field.data_type()
                    )));
                }
            }
        }

        let result = RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
            crate::utils::error::CoreError::Internal(format!(
                "Failed to create grouped result batch: {}",
                e
            ))
        })?;

        eprintln!(
            "🔍 [AggregationMerger] GROUP BY result: {} groups",
            result.num_rows()
        );

        Ok(result)
    }

    /// 提取分组键
    fn extract_group_key(
        &self,
        batch: &RecordBatch,
        col_idx: usize,
        row_idx: usize,
    ) -> CoreResult<String> {
        let column = batch.column(col_idx);

        if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
            if string_array.len() > row_idx && !string_array.is_null(row_idx) {
                return Ok(string_array.value(row_idx).to_string());
            }
        }

        if let Some(int64_array) = column.as_any().downcast_ref::<Int64Array>() {
            if int64_array.len() > row_idx && !int64_array.is_null(row_idx) {
                return Ok(int64_array.value(row_idx).to_string());
            }
        }

        Err(crate::utils::error::CoreError::Internal(
            "Failed to extract group key".to_string(),
        ))
    }

    /// 提取数值（用于聚合）
    fn extract_numeric_value(
        &self,
        batch: &RecordBatch,
        col_idx: usize,
        row_idx: usize,
    ) -> Option<f64> {
        let column = batch.column(col_idx);

        if let Some(int64_array) = column.as_any().downcast_ref::<Int64Array>() {
            if int64_array.len() > row_idx && !int64_array.is_null(row_idx) {
                return Some(int64_array.value(row_idx) as f64);
            }
        }

        if let Some(float64_array) = column.as_any().downcast_ref::<Float64Array>() {
            if float64_array.len() > row_idx && !float64_array.is_null(row_idx) {
                return Some(float64_array.value(row_idx));
            }
        }

        None
    }

    // ========== 简单聚合合并方法 ==========

    /// 合并 COUNT 列
    fn merge_count_column(
        &self,
        partition_results: &[Vec<RecordBatch>],
        col_idx: usize,
    ) -> CoreResult<ArrayRef> {
        let mut total_count: i64 = 0;

        for batches in partition_results {
            if let Some(batch) = batches.first() {
                let column = batch.column(col_idx);

                if let Some(int64_array) = column.as_any().downcast_ref::<Int64Array>() {
                    if int64_array.len() > 0 && !int64_array.is_null(0) {
                        total_count += int64_array.value(0);
                    }
                } else if let Some(uint64_array) = column.as_any().downcast_ref::<UInt64Array>() {
                    if uint64_array.len() > 0 && !uint64_array.is_null(0) {
                        total_count += uint64_array.value(0) as i64;
                    }
                }
            }
        }

        Ok(Arc::new(Int64Array::from(vec![total_count])))
    }

    /// 合并 SUM 列
    fn merge_sum_column(
        &self,
        partition_results: &[Vec<RecordBatch>],
        col_idx: usize,
        data_type: &DataType,
    ) -> CoreResult<ArrayRef> {
        match data_type {
            DataType::Int64 => {
                let mut total: i64 = 0;
                for batches in partition_results {
                    if let Some(batch) = batches.first() {
                        if let Some(array) =
                            batch.column(col_idx).as_any().downcast_ref::<Int64Array>()
                        {
                            if array.len() > 0 && !array.is_null(0) {
                                total += array.value(0);
                            }
                        }
                    }
                }
                Ok(Arc::new(Int64Array::from(vec![total])))
            }
            DataType::Float64 => {
                let mut total: f64 = 0.0;
                for batches in partition_results {
                    if let Some(batch) = batches.first() {
                        if let Some(array) = batch
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                        {
                            if array.len() > 0 && !array.is_null(0) {
                                total += array.value(0);
                            }
                        }
                    }
                }
                Ok(Arc::new(Float64Array::from(vec![total])))
            }
            _ => Ok(partition_results[0][0].column(col_idx).clone()),
        }
    }

    /// 合并 AVG 列（简化版：直接对各分区的 AVG 取平均）
    fn merge_avg_column(
        &self,
        partition_results: &[Vec<RecordBatch>],
        col_idx: usize,
        data_type: &DataType,
    ) -> CoreResult<ArrayRef> {
        match data_type {
            DataType::Float64 => {
                let mut sum: f64 = 0.0;
                let mut count = 0;
                for batches in partition_results {
                    if let Some(batch) = batches.first() {
                        if let Some(array) = batch
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                        {
                            if array.len() > 0 && !array.is_null(0) {
                                sum += array.value(0);
                                count += 1;
                            }
                        }
                    }
                }
                let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                Ok(Arc::new(Float64Array::from(vec![avg])))
            }
            _ => Ok(partition_results[0][0].column(col_idx).clone()),
        }
    }

    /// 合并 MAX 列
    fn merge_max_column(
        &self,
        partition_results: &[Vec<RecordBatch>],
        col_idx: usize,
        data_type: &DataType,
    ) -> CoreResult<ArrayRef> {
        match data_type {
            DataType::Int64 => {
                let mut max_val: Option<i64> = None;
                for batches in partition_results {
                    if let Some(batch) = batches.first() {
                        if let Some(array) =
                            batch.column(col_idx).as_any().downcast_ref::<Int64Array>()
                        {
                            if array.len() > 0 && !array.is_null(0) {
                                let val = array.value(0);
                                max_val = Some(max_val.map_or(val, |m| m.max(val)));
                            }
                        }
                    }
                }
                Ok(Arc::new(Int64Array::from(vec![max_val.unwrap_or(0)])))
            }
            DataType::Float64 => {
                let mut max_val: Option<f64> = None;
                for batches in partition_results {
                    if let Some(batch) = batches.first() {
                        if let Some(array) = batch
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                        {
                            if array.len() > 0 && !array.is_null(0) {
                                let val = array.value(0);
                                max_val = Some(max_val.map_or(val, |m| m.max(val)));
                            }
                        }
                    }
                }
                Ok(Arc::new(Float64Array::from(vec![max_val.unwrap_or(0.0)])))
            }
            _ => Ok(partition_results[0][0].column(col_idx).clone()),
        }
    }

    /// 合并 MIN 列
    fn merge_min_column(
        &self,
        partition_results: &[Vec<RecordBatch>],
        col_idx: usize,
        data_type: &DataType,
    ) -> CoreResult<ArrayRef> {
        match data_type {
            DataType::Int64 => {
                let mut min_val: Option<i64> = None;
                for batches in partition_results {
                    if let Some(batch) = batches.first() {
                        if let Some(array) =
                            batch.column(col_idx).as_any().downcast_ref::<Int64Array>()
                        {
                            if array.len() > 0 && !array.is_null(0) {
                                let val = array.value(0);
                                min_val = Some(min_val.map_or(val, |m| m.min(val)));
                            }
                        }
                    }
                }
                Ok(Arc::new(Int64Array::from(vec![min_val.unwrap_or(0)])))
            }
            DataType::Float64 => {
                let mut min_val: Option<f64> = None;
                for batches in partition_results {
                    if let Some(batch) = batches.first() {
                        if let Some(array) = batch
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                        {
                            if array.len() > 0 && !array.is_null(0) {
                                let val = array.value(0);
                                min_val = Some(min_val.map_or(val, |m| m.min(val)));
                            }
                        }
                    }
                }
                Ok(Arc::new(Float64Array::from(vec![min_val.unwrap_or(0.0)])))
            }
            _ => Ok(partition_results[0][0].column(col_idx).clone()),
        }
    }
}
