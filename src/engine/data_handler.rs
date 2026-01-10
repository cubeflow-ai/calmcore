//! 数据操作模块 - 处理数据的插入、查询和加载

use std::path::PathBuf;
use std::sync::Arc;


use crate::utils::error::{CoreError, CoreResult};

use super::Engine;

impl Engine {
    /// 插入批量数据（统一路由接口）
    ///
    /// 插入 RecordBatch 到指定 partition
    ///
    /// # 参数
    /// - `table_name`: 表名
    /// - `partition_name`: 目标 partition 名称（必须指定）
    /// - `batch`: 要插入的 RecordBatch
    ///
    /// # 说明
    /// 直接插入到指定的 partition，不做路由。
    /// Partition 必须已经在 Engine 中存在。
    pub async fn insert_batch(
        self: &Arc<Self>,
        table_name: &str,
        partition_name: &str,
        batch: datafusion::arrow::record_batch::RecordBatch,
    ) -> CoreResult<()> {
        log::debug!(
            "📝 [Engine] Inserting {} rows to partition '{}' of table '{}'",
            batch.num_rows(),
            partition_name,
            table_name
        );

        // 获取 partition
        let partition = self
            .get_partition(table_name, partition_name)
            .await
            .ok_or_else(|| {
                CoreError::Internal(format!(
                    "Partition '{}' not found for table '{}'. Ensure partition is created before inserting.",
                    partition_name, table_name
                ))
            })?;

        // 直接插入 RecordBatch 到 partition
        partition
            .upsert(batch.clone())
            .map_err(|e| CoreError::Internal(format!("Failed to insert to partition: {}", e)))?;

        log::debug!(
            "✅ [Engine] Successfully inserted {} rows to partition '{}'",
            batch.num_rows(),
            partition_name
        );

        Ok(())
    }





    /// 加载外部文件到 segment
    ///
    /// # 参数
    /// - `table_name`: 表名
    /// - `partition_name`: 分区名称（必须提供且符合目录名称规范）
    /// - `file_path`: 外部文件路径
    /// - `handler_type`: 文件处理类型
    ///
    /// # 返回
    /// 返回加载的文档数量
    pub async fn load_segment(
        &self,
        table_name: &str,
        partition_name: String,
        work_dir: PathBuf,
        file_path: PathBuf,
        handler_type: Option<crate::segment_loader::FileHandlerType>,
    ) -> CoreResult<usize> {
        // 1. 判断 partition 是否存在：先检查内存引用，再检查目录
        let partition = if let Some(existing_partition) =
            self.get_partition(table_name, &partition_name).await
        {
            log::info!(
                "Partition {} already exists in memory for table {}",
                partition_name,
                table_name
            );
            existing_partition
        } else {
            // 不存在，创建新的 partition
            log::error!(
                "Partition {} not found in memory for table {}, loading from disk",
                partition_name,
                table_name
            );
            return Err(CoreError::NotExisted(format!(
                "Partition {} not found in memory for table {}, loading from disk",
                partition_name, table_name
            )));
        };

        // 2. 检查文件是否已经被加载过
        // 获取文件的规范路径用于比较
        let file_canonical_path = file_path.canonicalize().map_err(|e| {
            crate::utils::error::CoreError::IOError(format!(
                "Failed to canonicalize file path {:?}: {}",
                file_path, e
            ))
        })?;

        // 检查所有 frozen segments 是否已经引用了这个文件
        {
            let frozen_segments = partition.get_frozen_segments();
            for (seg_id, segment) in frozen_segments.iter() {
                if let Some(segment_parquet_path) = segment.get_parquet_path() {
                    // 尝试规范化 segment 的路径进行比较
                    if let Ok(segment_canonical_path) =
                        std::path::Path::new(segment_parquet_path).canonicalize()
                    {
                        if segment_canonical_path == file_canonical_path {
                            return Err(crate::utils::error::CoreError::InvalidParam(format!(
                                "File {:?} has already been loaded into partition {} as segment {}",
                                file_path, partition_name, seg_id
                            )));
                        }
                    }
                }
            }
            // frozen_segments 在这里自动释放
        }

        log::info!(
            "File {:?} not yet loaded, proceeding to load into partition {}",
            file_path,
            partition_name
        );

        // 3. 使用 SegmentLoader 加载文件
        use crate::segment_loader::SegmentLoader;
        let loader = SegmentLoader::new(work_dir);

        let doc_count = loader
            .create_segment_from_file(&partition, &file_path, handler_type)
            .await?;

        log::info!(
            "Loaded {} documents from {:?} into partition {} of table {}",
            doc_count,
            file_path,
            partition_name,
            table_name
        );

        // 加载完成后，触发持久化
        self.trigger_persist(table_name, &partition_name);

        Ok(doc_count)
    }
}
