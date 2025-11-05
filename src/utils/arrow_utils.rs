use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use ahash::AHasher;
use datafusion::arrow::{
    self as arrow,
    array::{Array, ArrayRef, RecordBatch, StringArray, UInt32Array},
};

use crate::{
    arrow_downcast,
    schema::Schema,
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
    schema: &Schema,
) -> CoreResult<RecordBatch> {
    json_to_record_arrow(data, schema.to_arrow_schema())
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
    let estimated_size = data.len() * 100; // 估算每条记录约 100 字节
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
    let mut reader = ReaderBuilder::new(arrow_schema)
        .build(cursor)
        .map_err(|e| CoreError::Internal(format!("Failed to create JSON reader: {}", e)))?;

    // 读取 RecordBatch
    reader
        .next()
        .ok_or_else(|| CoreError::Internal("No data in JSON reader".to_string()))?
        .map_err(|e| CoreError::Internal(format!("Failed to read RecordBatch: {}", e)))
}
