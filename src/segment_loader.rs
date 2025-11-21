/// Segment 加载器模块
///
/// 用于 Custom 分区策略，支持从外部文件加载数据到 segment
use crate::partition::Partition;
use crate::schema::Schema;
use crate::utils::error::{CoreError, CoreResult};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// 文件处理类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileHandlerType {
    /// 引用文件路径（不移动原文件，只创建链接或引用）
    Reference,
    /// 移动文件（将文件移动到引擎管理的目录）
    Move,
    /// 拷贝文件（复制文件到引擎管理的目录）
    Copy,
}

/// Segment 加载器
pub struct SegmentLoader {
    /// 工作目录
    work_dir: PathBuf,
}

impl SegmentLoader {
    /// 创建新的 SegmentLoader
    pub fn new(work_dir: PathBuf) -> Self {
        Self { work_dir }
    }

    /// 加载外部文件到 segment
    ///
    /// # 参数
    /// - `table_name`: 表名
    /// - `partition_id`: 分区ID
    /// - `file_path`: 外部文件路径
    /// - `handler_type`: 文件处理类型
    /// - `schema`: 表的 schema
    ///
    /// # 返回
    /// 返回加载的文档数量
    pub async fn load_segment(
        &self,
        table_name: &str,
        partition_id: &str,
        file_path: &Path,
        handler_type: FileHandlerType,
        schema: &Schema,
    ) -> CoreResult<usize> {
        // 检查文件是否存在
        if !file_path.exists() {
            return Err(CoreError::NotExisted(format!(
                "File does not exist: {:?}",
                file_path
            )));
        }

        // 检查文件是否可读
        if !file_path.is_file() {
            return Err(CoreError::InvalidParam(format!(
                "Path is not a file: {:?}",
                file_path
            )));
        }

        // 创建临时目录用于处理
        let temp_dir = self
            .work_dir
            .join("tables")
            .join(table_name)
            .join(format!("partition-{}", partition_id))
            .join("temp_load");

        std::fs::create_dir_all(&temp_dir)
            .map_err(|e| CoreError::IOError(format!("Failed to create temp directory: {}", e)))?;

        // 根据文件类型处理文件
        let target_file =
            match handler_type {
                FileHandlerType::Reference => {
                    // 引用模式：创建符号链接或硬链接
                    let link_path =
                        temp_dir.join(file_path.file_name().ok_or_else(|| {
                            CoreError::InvalidParam("Invalid file name".to_string())
                        })?);

                    #[cfg(unix)]
                    {
                        std::os::unix::fs::symlink(file_path, &link_path).map_err(|e| {
                            CoreError::IOError(format!("Failed to create symlink: {}", e))
                        })?;
                    }

                    #[cfg(windows)]
                    {
                        std::os::windows::fs::symlink_file(file_path, &link_path).map_err(|e| {
                            CoreError::IOError(format!("Failed to create symlink: {}", e))
                        })?;
                    }

                    link_path
                }
                FileHandlerType::Move => {
                    // 移动模式：将文件移动到临时目录
                    let target_path =
                        temp_dir.join(file_path.file_name().ok_or_else(|| {
                            CoreError::InvalidParam("Invalid file name".to_string())
                        })?);

                    std::fs::rename(file_path, &target_path)
                        .map_err(|e| CoreError::IOError(format!("Failed to move file: {}", e)))?;

                    target_path
                }
                FileHandlerType::Copy => {
                    // 拷贝模式：复制文件到临时目录
                    let target_path =
                        temp_dir.join(file_path.file_name().ok_or_else(|| {
                            CoreError::InvalidParam("Invalid file name".to_string())
                        })?);

                    std::fs::copy(file_path, &target_path)
                        .map_err(|e| CoreError::IOError(format!("Failed to copy file: {}", e)))?;

                    target_path
                }
            };

        // 读取文件内容并解析为 JSON
        let file_content = std::fs::read_to_string(&target_file)
            .map_err(|e| CoreError::IOError(format!("Failed to read file: {}", e)))?;

        // 解析 JSON 数据（假设是 JSONL 格式，每行一个 JSON 对象）
        let mut documents = Vec::new();
        for line in file_content.lines() {
            if line.trim().is_empty() {
                continue;
            }

            let doc: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| CoreError::InvalidParam(format!("Failed to parse JSON: {}", e)))?;

            documents.push(doc);
        }

        let doc_count = documents.len();

        log::info!(
            "Loaded {} documents from file {:?} for partition {}",
            doc_count,
            file_path,
            partition_id
        );

        // 清理临时目录
        if let Err(e) = std::fs::remove_dir_all(&temp_dir) {
            log::warn!("Failed to clean up temp directory: {}", e);
        }

        Ok(doc_count)
    }

    /// 从文件创建 segment
    ///
    /// 这个方法将文件内容读取后，创建一个新的 segment 并写入数据
    pub async fn create_segment_from_file(
        &self,
        partition: &Arc<Partition>,
        file_path: &Path,
        handler_type: FileHandlerType,
    ) -> CoreResult<usize> {
        // 处理文件（根据 handler_type）
        let temp_dir = partition.base_dir().join("temp_load");
        std::fs::create_dir_all(&temp_dir)
            .map_err(|e| CoreError::IOError(format!("Failed to create temp directory: {}", e)))?;

        let target_file =
            match handler_type {
                FileHandlerType::Reference => {
                    let link_path =
                        temp_dir.join(file_path.file_name().ok_or_else(|| {
                            CoreError::InvalidParam("Invalid file name".to_string())
                        })?);

                    #[cfg(unix)]
                    {
                        std::os::unix::fs::symlink(file_path, &link_path).map_err(|e| {
                            CoreError::IOError(format!("Failed to create symlink: {}", e))
                        })?;
                    }

                    #[cfg(windows)]
                    {
                        std::os::windows::fs::symlink_file(file_path, &link_path).map_err(|e| {
                            CoreError::IOError(format!("Failed to create symlink: {}", e))
                        })?;
                    }

                    link_path
                }
                FileHandlerType::Move => {
                    let target_path =
                        temp_dir.join(file_path.file_name().ok_or_else(|| {
                            CoreError::InvalidParam("Invalid file name".to_string())
                        })?);
                    std::fs::rename(file_path, &target_path)
                        .map_err(|e| CoreError::IOError(format!("Failed to move file: {}", e)))?;
                    target_path
                }
                FileHandlerType::Copy => {
                    let target_path =
                        temp_dir.join(file_path.file_name().ok_or_else(|| {
                            CoreError::InvalidParam("Invalid file name".to_string())
                        })?);
                    std::fs::copy(file_path, &target_path)
                        .map_err(|e| CoreError::IOError(format!("Failed to copy file: {}", e)))?;
                    target_path
                }
            };

        // 读取并解析文件内容
        let file_content = std::fs::read_to_string(&target_file)
            .map_err(|e| CoreError::IOError(format!("Failed to read file: {}", e)))?;

        let mut documents = Vec::new();
        for line in file_content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let doc: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| CoreError::InvalidParam(format!("Failed to parse JSON: {}", e)))?;
            documents.push(doc);
        }

        // 写入到 partition
        let doc_count = documents.len();
        partition.upsert_json(&documents)?;

        // 清理临时目录
        if let Err(e) = std::fs::remove_dir_all(&temp_dir) {
            log::warn!("Failed to clean up temp directory: {}", e);
        }

        log::info!(
            "Created segment with {} documents from file {:?}",
            doc_count,
            file_path
        );

        Ok(doc_count)
    }
}
