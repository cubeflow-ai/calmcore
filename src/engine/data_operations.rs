//! 数据操作模块 - 处理数据的插入、查询和加载

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;

use crate::catalog::{PartitionStrategy, TableMeta};
use crate::partition::Partition;
use crate::utils::error::{CoreError, CoreResult};

use super::config::InsertStats;
use super::Engine;

impl Engine {
    /// 插入批量数据（统一路由接口）
    ///
    /// # 参数
    /// - `table_name`: 表名
    /// - `batch`: 要插入的 RecordBatch
    ///
    /// # 返回
    /// 返回插入统计信息
    ///
    /// # 说明
    /// 这是新的统一插入接口，会自动根据分区策略路由数据到对应分区
    /// - 使用 Router 进行数据路由
    /// - 按需创建分区（如果分区不存在）
    /// - 并发插入到多个分区
    pub async fn insert_batch(
        self: &Arc<Self>,
        table_name: &str,
        batch: datafusion::arrow::record_batch::RecordBatch,
        partition_name: Option<String>,
    ) -> CoreResult<InsertStats> {
        todo!()
        // use crate::router::Router;

        // // 1. 获取表元数据
        // let meta = self.catalog.get_table(table_name)?;

        // // 2. 如果指定了 partition_name,直接插入;否则使用 Router 路由
        // let routed_batches: HashMap<String, RecordBatch> = if let Some(partition) = partition_name {
        //     // 手动指定分区,不需要路由
        //     let mut map = HashMap::new();
        //     map.insert(partition, batch);
        //     map
        // } else {
        //     // 使用 Router 路由数据
        //     Router::route_batch(batch, &meta)?
        // };

        // if routed_batches.is_empty() {
        //     return Ok(InsertStats {
        //         rows_inserted: 0,
        //         partitions_affected: 0,
        //     });
        // }

        // // 3. 并发插入到各个分区
        // let mut tasks = Vec::new();

        // for (partition_name, partition_batch) in routed_batches {
        //     let table_name = table_name.to_string();
        //     let partition_name_clone = partition_name.clone();
        //     let self_clone = Arc::clone(self);
        //     let meta_clone = meta.clone();

        //     let task = tokio::spawn(async move {
        //         // 确保分区存在
        //         self_clone
        //             .ensure_partition_exists(&table_name, &partition_name_clone, &meta_clone)
        //             .await?;

        //         // 插入数据
        //         let rows = partition_batch.num_rows();
        //         self_clone
        //             .insert_to_partition(&table_name, &partition_name_clone, partition_batch)
        //             .await?;

        //         Ok::<_, CoreError>((partition_name_clone, rows))
        //     });

        //     tasks.push(task);
        // }

        // // 4. 等待所有插入完成
        // let mut total_rows = 0;
        // let mut partitions_affected = 0;

        // for task in tasks {
        //     match task.await {
        //         Ok(Ok((partition_name, rows))) => {
        //             total_rows += rows;
        //             partitions_affected += 1;
        //             log::debug!(
        //                 "Inserted {} rows to partition {} of table '{}'",
        //                 rows,
        //                 partition_name,
        //                 table_name
        //             );
        //         }
        //         Ok(Err(e)) => {
        //             return Err(CoreError::Internal(format!(
        //                 "Failed to insert to partition: {}",
        //                 e
        //             )));
        //         }
        //         Err(e) => {
        //             return Err(CoreError::Internal(format!("Task join error: {}", e)));
        //         }
        //     }
        // }

        // Ok(InsertStats {
        //     rows_inserted: total_rows,
        //     partitions_affected,
        // })
    }

    /// 确保分区存在（按需创建）
    async fn ensure_partition_exists(
        &self,
        table_name: &str,
        partition_name: &str,
        meta: &Arc<TableMeta>,
    ) -> CoreResult<()> {
        todo!()
        // // 检查分区是否已存在
        // if self
        //     .get_partition(table_name, partition_name)
        //     .await
        //     .is_some()
        // {
        //     return Ok(());
        // }

        // // 分区不存在，创建新分区
        // let partition_dir = self
        //     .config
        //     .data_dir
        //     .join("tables")
        //     .join(table_name)
        //     .join("partitions")
        //     .join(crate::catalog::PartitionStrategy::generate_partition_dir_name(partition_name));

        // log::info!(
        //     "Creating partition {} for table '{}' at {:?}",
        //     partition_name,
        //     table_name,
        //     partition_dir
        // );

        // let partition = Partition::new(
        //     partition_name.to_string(),
        //     table_name.to_string(),
        //     partition_dir,
        //     meta.schema.clone(),
        //     (*self.partition_notify_tx).clone(),
        // );

        // self.add_partition_with_table(table_name, Arc::new(partition))
        //     .await;

        // Ok(())
    }

    /// 插入数据到指定分区
    async fn insert_to_partition(
        &self,
        table_name: &str,
        partition_name: &str,
        batch: datafusion::arrow::record_batch::RecordBatch,
    ) -> CoreResult<()> {
        // let partition = self
        //     .get_partition(table_name, partition_name)
        //     .await
        //     .ok_or_else(|| {
        //         CoreError::Internal(format!(
        //             "Partition {} not found for table '{}'",
        //             partition_name, table_name
        //         ))
        //     })?;

        // partition.upsert(batch)?;
        Ok(())
    }

    /// 执行 SQL 查询（流式版本）
    ///
    /// 返回 DataFusion 的原生 Stream，避免全部加载到内存
    ///
    /// # 优势
    /// - 内存占用可控（不会一次性 collect 所有结果）
    /// - 适合大数据量查询
    /// - 支持 Ballista 分布式执行
    pub async fn execute_sql_stream(
        self: &Arc<Self>,
        sql: &str,
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        use crate::compute::Executor;

        let executor = Executor::new(self.clone());
        executor.execute_sql_stream(sql).await
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
        file_path: PathBuf,
        handler_type: Option<crate::segment_loader::FileHandlerType>,
    ) -> CoreResult<usize> {
        // 获取表的元数据
        // let meta = self.catalog.get_table(table_name)?;

        // // 验证 partition_name 格式 (partition_* 或 Custom 策略允许任意名称)
        // if !partition_name.starts_with("partition_")
        //     && !matches!(meta.partition_strategy, PartitionStrategy::Custom)
        // {
        //     return Err(crate::utils::error::CoreError::InvalidParam(format!(
        //         "Invalid partition_name '{}'. Must start with 'partition_' or use Custom partition strategy",
        //         partition_name
        //     )));
        // }

        // let partition_dir = self
        //     .config
        //     .data_dir
        //     .join("tables")
        //     .join(table_name)
        //     .join("partitions")
        //     .join(crate::catalog::PartitionStrategy::generate_partition_dir_name(&partition_name));

        // // 1. 判断 partition 是否存在：先检查内存引用，再检查目录
        // let partition = if let Some(existing_partition) =
        //     self.get_partition(table_name, &partition_name).await
        // {
        //     log::info!(
        //         "Partition {} already exists in memory for table {}",
        //         partition_name,
        //         table_name
        //     );
        //     existing_partition
        // } else if partition_dir.exists() {
        //     // 目录存在但内存中没有，从磁盘加载
        //     log::info!(
        //         "Loading existing partition {} from disk for table {}",
        //         partition_name,
        //         table_name
        //     );
        //     let loaded_partition = Partition::load(
        //         partition_name.clone(),
        //         table_name.to_string(),
        //         partition_dir.clone(),
        //         meta.schema.clone(),
        //         (*self.partition_notify_tx).clone(),
        //     )?;
        //     let partition = Arc::new(loaded_partition);
        //     self.add_partition_with_table(table_name, partition.clone())
        //         .await;
        //     partition
        // } else {
        //     // 不存在，创建新的 partition
        //     log::info!(
        //         "Creating new partition {} for table {}",
        //         partition_name,
        //         table_name
        //     );
        //     let new_partition = Partition::new(
        //         partition_name.clone(),
        //         table_name.to_string(),
        //         partition_dir,
        //         meta.schema.clone(),
        //         (*self.partition_notify_tx).clone(),
        //     );
        //     let partition = Arc::new(new_partition);
        //     self.add_partition_with_table(table_name, partition.clone())
        //         .await;
        //     partition
        // };

        // // 2. 检查文件是否已经被加载过
        // // 获取文件的规范路径用于比较
        // let file_canonical_path = file_path.canonicalize().map_err(|e| {
        //     crate::utils::error::CoreError::IOError(format!(
        //         "Failed to canonicalize file path {:?}: {}",
        //         file_path, e
        //     ))
        // })?;

        // // 检查所有 frozen segments 是否已经引用了这个文件
        // {
        //     let frozen_segments = partition.get_frozen_segments();
        //     for (seg_id, segment) in frozen_segments.iter() {
        //         if let Some(segment_parquet_path) = segment.get_parquet_path() {
        //             // 尝试规范化 segment 的路径进行比较
        //             if let Ok(segment_canonical_path) =
        //                 std::path::Path::new(segment_parquet_path).canonicalize()
        //             {
        //                 if segment_canonical_path == file_canonical_path {
        //                     return Err(crate::utils::error::CoreError::InvalidParam(format!(
        //                         "File {:?} has already been loaded into partition {} as segment {}",
        //                         file_path, partition_name, seg_id
        //                     )));
        //                 }
        //             }
        //         }
        //     }
        //     // frozen_segments 在这里自动释放
        // }

        // log::info!(
        //     "File {:?} not yet loaded, proceeding to load into partition {}",
        //     file_path,
        //     partition_name
        // );

        // // 3. 使用 SegmentLoader 加载文件
        // use crate::segment_loader::SegmentLoader;
        // let loader = SegmentLoader::new(self.config.data_dir.clone());

        // let doc_count = loader
        //     .create_segment_from_file(&partition, &file_path, handler_type)
        //     .await?;

        // log::info!(
        //     "Loaded {} documents from {:?} into partition {} of table {}",
        //     doc_count,
        //     file_path,
        //     partition_name,
        //     table_name
        // );

        // // 加载完成后，触发持久化
        // self.trigger_persist(table_name, &partition_name);

        // Ok(doc_count)
        todo!()
    }
}
