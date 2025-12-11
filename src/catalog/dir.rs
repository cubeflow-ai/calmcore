use std::path::{Path, PathBuf};

use crate::catalog::PartitionStrategy;

/// 获取表的目录路径
pub fn table_dir(work_dir: &Path, table_name: &str) -> PathBuf {
    work_dir.join("tables").join(table_name)
}

/// 获取 partition 目录路径（使用 partition_name 字符串）
pub fn partition_dir(work_dir: &Path, table_name: &str, partition_name: &str) -> PathBuf {
    let table_path = table_dir(work_dir, table_name);
    let dir_name = PartitionStrategy::generate_partition_dir_name(partition_name);
    table_path.join("partitions").join(dir_name)
}
