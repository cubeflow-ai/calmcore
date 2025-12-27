use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use ahash::AHasher;
use datafusion::arrow::{
    self as arrow,
    array::{Array, ArrayRef, RecordBatch, StringArray, UInt32Array},
    datatypes::Schema,
};

use crate::{
    arrow_downcast,
    utils::error::{CoreError, CoreResult},
};

pub fn array_to_hash(arr: &ArrayRef) -> Vec<u32> {
    match arr.data_type() {
        arrow::datatypes::DataType::Utf8 => {
            let string_array = arrow_downcast!(arr, StringArray);
            let mut result = Vec::with_capacity(string_array.len());
            for v in string_array.iter() {
                let mut hasher = AHasher::default();
                v.hash(&mut hasher);
                result.push(hasher.finish() as u32);
            }
            result
        }
        arrow::datatypes::DataType::UInt32 => arrow_downcast!(arr, UInt32Array)
            .iter()
            .map(|v| v.unwrap_or(0))
            .collect(),
        _ => panic!("unsupported pk type"),
    }
}

/// 将 JSON 数组转换为 RecordBatch (优化：减少内存分配和拷贝)
pub fn json_to_record_batch(
    data: &[serde_json::Value],
    schema: Arc<Schema>,
) -> CoreResult<RecordBatch> {
    json_to_record_arrow(data, schema)
}

pub fn json_to_record_arrow(
    data: &[serde_json::Value],
    arrow_schema: Arc<arrow::datatypes::Schema>,
) -> CoreResult<RecordBatch> {
    use datafusion::arrow::json::ReaderBuilder;
    use std::io::Cursor;

    if data.is_empty() {
        return Err(CoreError::InvalidParam("Empty data array".to_string()));
    }

    // 从 Schema 获取 Arrow Schema
    // 使用 saturating_mul 防止溢出
    let estimated_size = data.len().saturating_mul(100).min(100 * 1024 * 1024); // 最大 100MB
    let mut buffer = Vec::with_capacity(estimated_size);

    for (i, value) in data.iter().enumerate() {
        if i > 0 {
            buffer.push(b'\n');
        }
        serde_json::to_writer(&mut buffer, value)
            .map_err(|e| CoreError::Internal(format!("Failed to serialize JSON: {}", e)))?;
    }

    // 使用 Arrow JSON Reader 解析
    let cursor = Cursor::new(buffer);
    let mut reader = ReaderBuilder::new(arrow_schema.clone())
        .build(cursor)
        .map_err(|e| CoreError::Internal(format!("Failed to create JSON reader: {}", e)))?;

    // 读取所有 RecordBatch 并合并
    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| CoreError::Internal(format!("Failed to read RecordBatch: {}", e)))?;
        batches.push(batch);
    }

    if batches.is_empty() {
        return Err(CoreError::Internal("No data in JSON reader".to_string()));
    }

    // 如果只有一个 batch，直接返回
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }

    // 合并多个 batch
    let merged = arrow::compute::concat_batches(&arrow_schema, &batches)
        .map_err(|e| CoreError::Internal(format!("Failed to merge batches: {}", e)))?;

    Ok(merged)
}

/// 将 RecordBatch 转换为 JSON 数组
pub fn record_batch_to_json(batch: &RecordBatch) -> CoreResult<Vec<serde_json::Value>> {
    use datafusion::arrow::json::ArrayWriter;
    use std::io::Cursor;

    let mut buffer = Vec::new();
    {
        let mut writer = ArrayWriter::new(Cursor::new(&mut buffer));
        writer
            .write(batch)
            .map_err(|e| CoreError::Internal(format!("Failed to write JSON: {}", e)))?;
        writer
            .finish()
            .map_err(|e| CoreError::Internal(format!("Failed to finish JSON: {}", e)))?;
    }

    // 解析 JSON 数组 (ArrayWriter 生成的是 JSON 数组格式，不是 NDJSON)
    let json_str = String::from_utf8(buffer)
        .map_err(|e| CoreError::Internal(format!("Invalid UTF-8: {}", e)))?;

    // ArrayWriter 输出的是 JSON 数组格式: [{"col1": val1}, {"col2": val2}, ...]
    let results: Vec<serde_json::Value> = serde_json::from_str(&json_str)
        .map_err(|e| CoreError::Internal(format!("Failed to parse JSON array: {}", e)))?;

    Ok(results)
}
