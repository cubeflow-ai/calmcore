use std::fs;
use std::path::Path;

use crate::utils::error::{CoreError, CoreResult};

/// 通用方法：从 JSON 文件加载并反序列化对象
pub fn load_json_from_file<T: serde::de::DeserializeOwned>(path: &Path) -> CoreResult<T> {
    let content = fs::read_to_string(path)
        .map_err(|e| CoreError::IOError(format!("Failed to read file {:?}: {}", path, e)))?;

    serde_json::from_str(&content)
        .map_err(|e| CoreError::IOError(format!("Failed to parse JSON from {:?}: {}", path, e)))
}
