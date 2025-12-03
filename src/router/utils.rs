//! 路由工具函数

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray, UInt64Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;

use crate::utils::error::{CoreError, CoreResult};

/// 计算 hash 字段每行的分区索引
pub fn compute_hash_indices(
    column: &Arc<dyn Array>,
    num_partitions: usize,
) -> CoreResult<Vec<usize>> {
    match column.data_type() {
        DataType::Utf8 => {
            let array = column.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CoreError::Internal("Failed to downcast to StringArray".into()))?;
            
            let mut indices = Vec::with_capacity(array.len());
            for i in 0..array.len() {
                if array.is_null(i) {
                    return Err(CoreError::Internal("Hash field contains null values".into()));
                }
                
                let value = array.value(i);
                let hash_value = hash_string(value);
                let partition_idx = (hash_value % num_partitions as u64) as usize;
                indices.push(partition_idx);
            }
            
            Ok(indices)
        }
        
        DataType::UInt64 => {
            let array = column.as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| CoreError::Internal("Failed to downcast to UInt64Array".into()))?;
            
            let mut indices = Vec::with_capacity(array.len());
            for i in 0..array.len() {
                if array.is_null(i) {
                    return Err(CoreError::Internal("Hash field contains null values".into()));
                }
                
                let value = array.value(i);
                let partition_idx = (value % num_partitions as u64) as usize;
                indices.push(partition_idx);
            }
            
            Ok(indices)
        }
        
        _ => Err(CoreError::Internal(
            format!("Hash partition does not support field type {:?}", column.data_type())
        )),
    }
}

/// 按分区索引分割 RecordBatch
pub fn split_batch_by_indices(
    batch: RecordBatch,
    indices: Vec<usize>,
    num_partitions: usize,
) -> CoreResult<HashMap<String, RecordBatch>> {
    // 分组：partition_idx -> row_indices
    let mut groups: Vec<Vec<usize>> = vec![Vec::new(); num_partitions];
    for (row_idx, &partition_idx) in indices.iter().enumerate() {
        groups[partition_idx].push(row_idx);
    }
    
    // 为每个分区创建 RecordBatch
    let mut result = HashMap::new();
    for (partition_idx, row_indices) in groups.into_iter().enumerate() {
        if row_indices.is_empty() {
            continue;
        }
        
        let partition_name = format!("partition_{:019}", partition_idx);
        let partition_batch = take_rows(&batch, &row_indices)?;
        result.insert(partition_name, partition_batch);
    }
    
    Ok(result)
}

/// 从 RecordBatch 中提取指定行
pub fn take_rows(
    batch: &RecordBatch,
    row_indices: &[usize],
) -> CoreResult<RecordBatch> {
    let indices = UInt64Array::from(
        row_indices.iter().map(|&i| i as u64).collect::<Vec<_>>()
    );
    let indices_ref = Arc::new(indices) as ArrayRef;
    
    let new_columns: Vec<ArrayRef> = batch.columns()
        .iter()
        .map(|col| {
            take(col.as_ref(), &indices_ref, None)
                .map_err(|e| CoreError::Internal(format!("Failed to take rows: {}", e)))
        })
        .collect::<CoreResult<Vec<_>>>()?;
    
    RecordBatch::try_new(batch.schema(), new_columns)
        .map_err(|e| CoreError::Internal(format!("Failed to create RecordBatch: {}", e)))
}

/// 计算字符串的 hash 值
fn hash_string(s: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}
