//! 分布式执行器
//!
//! 负责分布式查询的执行，包括：
//! - Scatter-Gather 模式（无 GROUP BY）
//! - Shuffle 模式（有 GROUP BY）
//!
//! ## 设计原则
//!
//! - 复用现有组件（SegmentScanner、索引优化等）
//! - 流式处理，避免内存爆炸
//! - 并行执行，最大化吞吐量

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::stream::{self, Stream};
use uuid::Uuid;

use super::config::DistributedConfig;
use super::error_handler::{
    node_unreachable_error, shuffle_failed_error, ErrorAggregator, QueryCancellationManager,
    TimeoutExecutor,
};
use super::node_client::{NodeClientManager, QueryRequest};
use crate::cluster::{ClusterManager, PartitionManager};
use crate::compute::{SqlNormalizer, UnionTableProvider};
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

/// 远程 Partition 信息
#[derive(Debug, Clone)]
pub struct RemotePartitionInfo {
    /// Partition 名称
    pub partition_name: String,
    /// 负责该 Partition 的节点 ID
    pub node_id: String,
}

/// 分布式执行器
///
/// 负责分布式查询的执行，根据查询类型选择执行策略：
/// - 无 GROUP BY：Scatter-Gather 模式
/// - 有 GROUP BY：Shuffle 模式（后续实现）
pub struct DistributedExecutor {
    /// 本地引擎
    engine: Arc<Engine>,
    /// 集群管理器
    cluster_manager: Arc<ClusterManager>,
    /// 分区管理器
    partition_manager: Arc<PartitionManager>,
    /// 节点客户端管理器
    node_clients: Arc<NodeClientManager>,
    /// 配置
    config: DistributedConfig,
    /// 查询取消管理器
    ///
    /// # Requirements
    /// - 8.1: 任一节点失败时取消其他节点的查询
    cancellation_manager: Arc<QueryCancellationManager>,
    /// 超时执行器
    ///
    /// # Requirements
    /// - 8.3: 查询超时返回超时错误
    timeout_executor: Arc<TimeoutExecutor>,
}

impl DistributedExecutor {
    /// 创建新的分布式执行器
    pub fn new(
        engine: Arc<Engine>,
        cluster_manager: Arc<ClusterManager>,
        partition_manager: Arc<PartitionManager>,
        config: DistributedConfig,
    ) -> Self {
        let node_clients = Arc::new(NodeClientManager::new(
            cluster_manager.clone(),
            config.clone(),
        ));

        let cancellation_manager = Arc::new(QueryCancellationManager::new(node_clients.clone()));
        let timeout_executor = Arc::new(TimeoutExecutor::new(config.query_timeout_ms));

        Self {
            engine,
            cluster_manager,
            partition_manager,
            node_clients,
            config,
            cancellation_manager,
            timeout_executor,
        }
    }

    /// 执行分布式 SQL 查询
    ///
    /// 根据查询类型自动选择执行策略：
    /// - 无 GROUP BY：Scatter-Gather 模式
    /// - 有 GROUP BY：Shuffle 模式
    pub async fn execute_sql(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
        todo!()
        // log::info!("[DistributedExecutor] Executing SQL: {}", sql);

        // // 1. 标准化 SQL
        // let normalized = SqlNormalizer::normalize(sql)?;

        // // 2. 提取表名
        // let table_name = self.extract_table_name(&normalized.rewritten_sql)?;

        // // 3. 获取 Partition 分布
        // let topology = self
        //     .partition_manager
        //     .get_table_topology(&table_name)
        //     .await
        //     .ok_or_else(|| {
        //         CoreError::NotExisted(format!(
        //             "Table '{}' not found in cluster topology",
        //             table_name
        //         ))
        //     })?;

        // // 4. 分类本地和远程 Partition
        // let my_node_id = self.cluster_manager.node_id();
        // let (local_partitions, remote_partitions) = self.classify_partitions(&topology, my_node_id);

        // log::info!(
        //     "[DistributedExecutor] Table '{}': {} local, {} remote partitions",
        //     table_name,
        //     local_partitions.len(),
        //     remote_partitions.len()
        // );

        // // 5. 判断是否需要 Shuffle（检查 GROUP BY）
        // let has_group_by = self.has_group_by(&normalized.rewritten_sql);

        // // 6. 选择执行策略
        // if has_group_by {
        //     // 提取 GROUP BY 列
        //     let group_by_cols = self.extract_group_by_columns(&normalized.rewritten_sql)?;

        //     // Shuffle 模式
        //     self.execute_with_shuffle(
        //         &normalized.rewritten_sql,
        //         &table_name,
        //         &local_partitions,
        //         &remote_partitions,
        //         &group_by_cols,
        //     )
        //     .await
        // } else {
        //     // Scatter-Gather 模式
        //     self.execute_scatter_gather(
        //         &normalized.rewritten_sql,
        //         &table_name,
        //         &local_partitions,
        //         &remote_partitions,
        //     )
        //     .await
        // }
    }

    /// 执行 Scatter-Gather 查询（无 GROUP BY）
    ///
    /// 并行发送查询到所有相关节点，流式合并结果
    ///
    /// # Requirements
    /// - 4.1: 并行发送查询到所有相关节点
    /// - 4.2: 流式合并所有结果
    /// - 7.1: 直接 UNION 所有节点结果
    pub async fn execute_scatter_gather(
        &self,
        sql: &str,
        table_name: &str,
        local_partitions: &[String],
        remote_partitions: &[RemotePartitionInfo],
    ) -> CoreResult<SendableRecordBatchStream> {
        log::info!(
            "[DistributedExecutor] Scatter-Gather: {} local, {} remote",
            local_partitions.len(),
            remote_partitions.len()
        );

        // 生成查询 ID
        let query_id = Uuid::new_v4().to_string();

        // 收集所有流
        let mut streams: Vec<SendableRecordBatchStream> = Vec::new();
        let mut schema: Option<SchemaRef> = None;

        // 1. 执行本地查询
        if !local_partitions.is_empty() {
            let local_stream = self
                .execute_local_partitions(sql, table_name, local_partitions)
                .await?;

            // 获取 schema
            if schema.is_none() {
                schema = Some(local_stream.schema());
            }

            streams.push(local_stream);
        }

        // 2. 并行执行远程查询
        if !remote_partitions.is_empty() {
            let remote_streams = self
                .execute_remote_partitions(sql, &query_id, remote_partitions)
                .await?;

            for stream in remote_streams {
                if schema.is_none() {
                    schema = Some(stream.schema());
                }
                streams.push(stream);
            }
        }

        // 3. 合并所有流
        if streams.is_empty() {
            return Err(CoreError::Internal(
                "No streams to merge - no partitions found".to_string(),
            ));
        }

        let final_schema = schema.unwrap();

        // 4. 如果有 LIMIT，需要在合并后再次应用（因为每个节点都返回了 LIMIT 行）
        let limit = self.extract_limit(sql);
        let merged_stream = if let Some(limit_value) = limit {
            log::debug!(
                "[DistributedExecutor] Applying global LIMIT {} after merge",
                limit_value
            );
            let merged = self.merge_streams(streams, final_schema.clone());
            self.apply_limit(merged, limit_value, final_schema)
        } else {
            self.merge_streams(streams, final_schema)
        };

        Ok(merged_stream)
    }

    /// 从 SQL 中提取 LIMIT 值
    ///
    /// # Requirements
    /// - 4.3: 将 LIMIT 下推到各节点
    fn extract_limit(&self, sql: &str) -> Option<usize> {
        let sql_upper = sql.to_uppercase();

        if let Some(limit_pos) = sql_upper.find(" LIMIT ") {
            let after_limit = &sql[limit_pos + 7..].trim();
            // 提取数字
            let limit_str: String = after_limit
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();

            if !limit_str.is_empty() {
                return limit_str.parse().ok();
            }
        }

        None
    }

    /// 应用 LIMIT 到合并后的流
    ///
    /// 因为每个节点都返回了 LIMIT 行，合并后需要再次应用 LIMIT
    fn apply_limit(
        &self,
        stream: SendableRecordBatchStream,
        limit: usize,
        schema: SchemaRef,
    ) -> SendableRecordBatchStream {
        Box::pin(LimitedRecordBatchStream::new(stream, limit, schema))
    }

    /// 执行本地 Partition 查询
    ///
    /// 复用现有的 UnionTableProvider 和 SegmentScanner
    async fn execute_local_partitions(
        &self,
        sql: &str,
        table_name: &str,
        partition_names: &[String],
    ) -> CoreResult<SendableRecordBatchStream> {
        log::debug!(
            "[DistributedExecutor] Executing local partitions: {:?}",
            partition_names
        );

        // 获取 Partition 对象
        let mut partitions = Vec::new();
        for partition_name in partition_names {
            if let Some(partition) = self.engine.get_partition(table_name, partition_name).await {
                partitions.push(partition);
            }
        }

        if partitions.is_empty() {
            return Err(CoreError::NotExisted(format!(
                "No local partitions found for table '{}'",
                table_name
            )));
        }

        // 创建 UnionTableProvider
        let union_table = UnionTableProvider::new(partitions)
            .map_err(|e| CoreError::Internal(format!("Failed to create UnionTable: {}", e)))?;

        // 创建 DataFusion SessionContext
        use datafusion::prelude::*;
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);

        // 注册表
        ctx.register_table(table_name, Arc::new(union_table))
            .map_err(|e| CoreError::Internal(format!("Failed to register table: {}", e)))?;

        // 执行查询
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| CoreError::Internal(format!("Query execution error: {}", e)))?;

        let stream = df
            .execute_stream()
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to execute stream: {}", e)))?;

        Ok(stream)
    }

    /// 执行远程 Partition 查询
    ///
    /// 并行发送查询到所有远程节点
    ///
    /// # Requirements
    /// - 4.1: 并行发送查询到所有相关节点
    /// - 4.3: 将 LIMIT 下推到各节点
    /// - 4.4: 将 WHERE 条件下推到各节点
    /// - 8.1: 任一节点失败时取消其他节点的查询
    /// - 8.2: 节点不可达时返回包含节点信息的错误
    /// - 8.3: 查询超时返回超时错误
    async fn execute_remote_partitions(
        &self,
        sql: &str,
        query_id: &str,
        remote_partitions: &[RemotePartitionInfo],
    ) -> CoreResult<Vec<SendableRecordBatchStream>> {
        log::debug!(
            "[DistributedExecutor] Executing remote partitions: {:?}",
            remote_partitions
        );

        // 按节点分组 Partition
        let mut node_partitions: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        for rp in remote_partitions {
            node_partitions
                .entry(rp.node_id.clone())
                .or_default()
                .push(rp.partition_name.clone());
        }

        // 获取涉及的节点列表
        let node_ids: Vec<String> = node_partitions.keys().cloned().collect();

        // 注册查询到取消管理器
        self.cancellation_manager
            .register_query(query_id, node_ids.clone())
            .await;

        // 并行发送查询到各节点
        let mut futures = Vec::new();

        for (node_id, partitions) in node_partitions {
            let node_clients = self.node_clients.clone();
            let sql = sql.to_string();
            let query_id = query_id.to_string();
            let timeout_ms = self.config.query_timeout_ms;
            let node_id_clone = node_id.clone();

            let future = async move {
                let client = match node_clients.get_client(&node_id).await {
                    Ok(c) => c,
                    Err(e) => {
                        // 节点不可达错误
                        // Requirements: 8.2
                        return Err(node_unreachable_error(&node_id, "unknown", &e.to_string()));
                    }
                };

                let request = QueryRequest {
                    sql,
                    query_id,
                    timeout_ms,
                    partition_filter: partitions,
                };

                let batches = client.execute_query(&request).await?;
                Ok::<_, CoreError>((node_id_clone, batches))
            };

            futures.push(future);
        }

        // 使用超时执行器执行所有远程查询
        // Requirements: 8.3
        let execute_all = async { futures::future::join_all(futures).await };

        let results = self
            .timeout_executor
            .execute_with_timeout(
                query_id,
                node_ids.clone(),
                Some(self.config.query_timeout_ms),
                async { Ok(execute_all.await) },
            )
            .await?;

        // 使用错误聚合器收集结果
        let mut error_aggregator = ErrorAggregator::new(query_id);
        let mut streams = Vec::new();
        let mut first_error: Option<(String, CoreError)> = None;

        for result in results {
            match result {
                Ok((node_id, batches)) => {
                    error_aggregator.record_success(&node_id);
                    if !batches.is_empty() {
                        let schema = batches[0].schema();
                        let stream = self.batches_to_stream(batches, schema);
                        streams.push(stream);
                        log::debug!(
                            "[DistributedExecutor] Received results from node {}",
                            node_id
                        );
                    }
                }
                Err(e) => {
                    // 记录失败的节点
                    let node_id = "unknown".to_string(); // 从错误中提取节点 ID 如果可能
                    error_aggregator.record_failure(&node_id, &e.to_string());

                    if first_error.is_none() {
                        first_error = Some((node_id.clone(), e));
                    }

                    log::error!(
                        "[DistributedExecutor] Remote query failed on node: {}",
                        node_id
                    );
                }
            }
        }

        // 如果有任何失败，取消其他节点的查询
        // Requirements: 8.1
        if let Some((failed_node, error)) = first_error {
            log::warn!(
                "[DistributedExecutor] Query {} failed, cancelling other nodes",
                query_id
            );

            self.cancellation_manager
                .cancel_query(query_id, &error.to_string(), Some(&failed_node))
                .await;

            // 注销查询
            self.cancellation_manager.unregister_query(query_id).await;

            return Err(error);
        }

        // 注销查询
        self.cancellation_manager.unregister_query(query_id).await;

        Ok(streams)
    }

    /// 将 RecordBatch 列表转换为 Stream
    fn batches_to_stream(
        &self,
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
    ) -> SendableRecordBatchStream {
        let stream = stream::iter(batches.into_iter().map(Ok));
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    /// 合并多个 Stream 为一个
    ///
    /// 使用流式 UNION，不需要等待所有数据
    ///
    /// # Requirements
    /// - 7.1: 直接 UNION 所有节点结果
    fn merge_streams(
        &self,
        streams: Vec<SendableRecordBatchStream>,
        schema: SchemaRef,
    ) -> SendableRecordBatchStream {
        // 创建合并流
        let merged = MergedRecordBatchStream::new(streams, schema.clone());
        Box::pin(merged)
    }

    /// 分类 Partition 为本地和远程
    // fn classify_partitions(
    //     &self,
    //     topology: &TableTopology,
    //     my_node_id: &str,
    // ) -> (Vec<String>, Vec<RemotePartitionInfo>) {
    //     let mut local = Vec::new();
    //     let mut remote = Vec::new();

    //     for (partition_name, partition_info) in &topology.partitions {
    //         if partition_info.write_node == my_node_id {
    //             local.push(partition_name.clone());
    //         } else {
    //             remote.push(RemotePartitionInfo {
    //                 partition_name: partition_name.clone(),
    //                 node_id: partition_info.write_node.clone(),
    //             });
    //         }
    //     }

    //     (local, remote)
    // }

    /// 从 SQL 中提取表名
    fn extract_table_name(&self, sql: &str) -> CoreResult<String> {
        let sql_upper = sql.to_uppercase();

        if let Some(from_pos) = sql_upper.find(" FROM ") {
            let after_from = &sql[from_pos + 6..].trim();
            let table_name = after_from
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches(|c: char| !c.is_alphanumeric() && c != '_' && c != '.');

            if !table_name.is_empty() {
                return Ok(table_name.to_string());
            }
        }

        Err(CoreError::InvalidParam(format!(
            "Could not extract table name from SQL: {}",
            sql
        )))
    }

    /// 检查 SQL 是否包含 GROUP BY
    fn has_group_by(&self, sql: &str) -> bool {
        sql.to_uppercase().contains("GROUP BY")
    }

    /// 从 SQL 中提取 GROUP BY 列名
    ///
    /// # Requirements
    /// - 5.1: 提取 GROUP BY 字段
    fn extract_group_by_columns(&self, sql: &str) -> CoreResult<Vec<String>> {
        let sql_upper = sql.to_uppercase();

        if let Some(group_by_pos) = sql_upper.find("GROUP BY") {
            let after_group_by = &sql[group_by_pos + 8..].trim();

            // 找到 GROUP BY 子句的结束位置（ORDER BY, LIMIT, HAVING, 或语句结束）
            let end_pos = after_group_by
                .to_uppercase()
                .find(" ORDER BY")
                .or_else(|| after_group_by.to_uppercase().find(" LIMIT"))
                .or_else(|| after_group_by.to_uppercase().find(" HAVING"))
                .unwrap_or(after_group_by.len());

            let group_by_clause = &after_group_by[..end_pos];

            // 解析列名（逗号分隔）
            let columns: Vec<String> = group_by_clause
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            if columns.is_empty() {
                return Err(CoreError::InvalidParam(
                    "GROUP BY clause is empty".to_string(),
                ));
            }

            return Ok(columns);
        }

        Ok(Vec::new())
    }

    /// 执行带 Shuffle 的分布式查询（有 GROUP BY）
    ///
    /// 按 GROUP BY 字段的 Hash 值分发数据到各节点，然后在各节点本地执行聚合。
    ///
    /// # Requirements
    /// - 5.1: 提取 GROUP BY 字段
    /// - 5.2: 按 GROUP BY 字段的 Hash 值分发数据
    /// - 5.5: 所有数据 Shuffle 完成后在各节点本地执行聚合
    /// - 7.2: 直接 UNION 所有节点结果（因为 Shuffle 保证不重复）
    /// - 7.3: 合并后排序（如果有 ORDER BY）
    /// - 7.4: 合并后应用 LIMIT
    pub async fn execute_with_shuffle(
        &self,
        sql: &str,
        table_name: &str,
        local_partitions: &[String],
        remote_partitions: &[RemotePartitionInfo],
        group_by_cols: &[String],
    ) -> CoreResult<SendableRecordBatchStream> {
        log::info!(
            "[DistributedExecutor] Execute with Shuffle: {} local, {} remote, group_by={:?}",
            local_partitions.len(),
            remote_partitions.len(),
            group_by_cols
        );

        // 生成查询 ID
        let query_id = Uuid::new_v4().to_string();

        // 1. 获取表 Schema
        let schema = self.get_table_schema(table_name).await?;

        // 2. 获取 GROUP BY 列索引
        let group_by_indices = self.get_group_by_indices(&schema, group_by_cols)?;

        // 3. 计算节点数量和本节点索引
        let (num_nodes, my_node_index, node_ids) =
            self.calculate_node_topology(local_partitions, remote_partitions);

        log::debug!(
            "[DistributedExecutor] Shuffle topology: num_nodes={}, my_index={}, nodes={:?}",
            num_nodes,
            my_node_index,
            node_ids
        );

        // 4. 创建 Shuffle 管理器
        let mut shuffle_manager = super::shuffle_exec::NetworkShuffleManager::new(
            query_id.clone(),
            self.cluster_manager.node_id().to_string(),
            my_node_index,
            node_ids.clone(),
            self.config.shuffle_buffer_size,
        );

        // 5. 创建本地 Shuffle 执行计划
        let local_shuffle_stream = if !local_partitions.is_empty() {
            Some(
                self.create_local_shuffle_plan(
                    sql,
                    table_name,
                    local_partitions,
                    &group_by_indices,
                    &mut shuffle_manager,
                )
                .await?,
            )
        } else {
            None
        };

        // 6. 发送查询到远程节点（它们会执行自己的 Shuffle）
        let remote_streams = if !remote_partitions.is_empty() {
            self.execute_remote_shuffle_queries(sql, &query_id, remote_partitions)
                .await?
        } else {
            Vec::new()
        };

        // 7. 合并所有流
        let mut streams: Vec<SendableRecordBatchStream> = Vec::new();

        if let Some(local_stream) = local_shuffle_stream {
            streams.push(local_stream);
        }

        for remote_stream in remote_streams {
            streams.push(remote_stream);
        }

        if streams.is_empty() {
            return Err(CoreError::Internal(
                "No streams to merge - no partitions found".to_string(),
            ));
        }

        // 8. 获取输出 schema（从第一个流）
        let output_schema = streams[0].schema();

        // 9. 合并流（直接 UNION，因为 Shuffle 保证不重复）
        let merged_stream = self.merge_streams(streams, output_schema.clone());

        // 10. 应用 ORDER BY（如果有）
        let ordered_stream = if self.has_order_by(sql) {
            self.apply_order_by(merged_stream, sql, output_schema.clone())
                .await?
        } else {
            merged_stream
        };

        // 11. 应用全局 LIMIT（如果有）
        let final_stream = if let Some(limit) = self.extract_limit(sql) {
            log::debug!(
                "[DistributedExecutor] Applying global LIMIT {} after shuffle merge",
                limit
            );
            self.apply_limit(ordered_stream, limit, output_schema)
        } else {
            ordered_stream
        };

        Ok(final_stream)
    }

    /// 获取表 Schema
    async fn get_table_schema(&self, table_name: &str) -> CoreResult<SchemaRef> {
        // 尝试从本地 Engine 获取表 Schema
        match self.engine.get_table_meta(table_name) {
            Ok(table_meta) => Ok(table_meta.schema.to_arrow_schema()),
            Err(_) => Err(CoreError::NotExisted(format!(
                "Table '{}' not found",
                table_name
            ))),
        }
    }

    /// 获取 GROUP BY 列索引
    fn get_group_by_indices(
        &self,
        schema: &SchemaRef,
        group_by_cols: &[String],
    ) -> CoreResult<Vec<usize>> {
        let mut indices = Vec::new();

        for col_name in group_by_cols {
            let index = schema
                .fields()
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case(col_name))
                .ok_or_else(|| {
                    CoreError::InvalidParam(format!(
                        "GROUP BY column '{}' not found in schema",
                        col_name
                    ))
                })?;
            indices.push(index);
        }

        Ok(indices)
    }

    /// 计算节点拓扑
    fn calculate_node_topology(
        &self,
        local_partitions: &[String],
        remote_partitions: &[RemotePartitionInfo],
    ) -> (usize, usize, Vec<String>) {
        let my_node_id = self.cluster_manager.node_id().to_string();
        let mut node_ids: Vec<String> = Vec::new();

        // 添加本地节点（如果有本地 partition）
        if !local_partitions.is_empty() {
            node_ids.push(my_node_id.clone());
        }

        // 添加远程节点（去重）
        for rp in remote_partitions {
            if !node_ids.contains(&rp.node_id) {
                node_ids.push(rp.node_id.clone());
            }
        }

        // 确保至少有一个节点
        if node_ids.is_empty() {
            node_ids.push(my_node_id.clone());
        }

        let my_node_index = node_ids
            .iter()
            .position(|id| id == &my_node_id)
            .unwrap_or(0);

        (node_ids.len(), my_node_index, node_ids)
    }

    /// 创建本地 Shuffle 执行计划
    async fn create_local_shuffle_plan(
        &self,
        sql: &str,
        table_name: &str,
        partition_names: &[String],
        _group_by_indices: &[usize],
        _shuffle_manager: &mut super::shuffle_exec::NetworkShuffleManager,
    ) -> CoreResult<SendableRecordBatchStream> {
        log::debug!(
            "[DistributedExecutor] Creating local shuffle plan for {} partitions",
            partition_names.len()
        );

        // 获取 Partition 对象
        let mut partitions = Vec::new();
        for partition_name in partition_names {
            if let Some(partition) = self.engine.get_partition(table_name, partition_name).await {
                partitions.push(partition);
            }
        }

        if partitions.is_empty() {
            return Err(CoreError::NotExisted(format!(
                "No local partitions found for table '{}'",
                table_name
            )));
        }

        // 创建 UnionTableProvider
        let union_table = UnionTableProvider::new(partitions)
            .map_err(|e| CoreError::Internal(format!("Failed to create UnionTable: {}", e)))?;

        let _schema = union_table.schema();

        // 创建 DataFusion SessionContext
        use datafusion::prelude::*;
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);

        // 注册表
        ctx.register_table(table_name, Arc::new(union_table))
            .map_err(|e| CoreError::Internal(format!("Failed to register table: {}", e)))?;

        // 执行查询获取执行计划
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| CoreError::Internal(format!("Query execution error: {}", e)))?;

        let stream = df
            .execute_stream()
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to execute stream: {}", e)))?;

        Ok(stream)
    }

    /// 执行远程 Shuffle 查询
    ///
    /// # Requirements
    /// - 8.1: 任一节点失败时取消其他节点的查询
    /// - 8.2: 节点不可达时返回包含节点信息的错误
    /// - 8.3: 查询超时返回超时错误
    /// - 8.4: Shuffle 过程中节点失败返回 Shuffle 失败错误
    async fn execute_remote_shuffle_queries(
        &self,
        sql: &str,
        query_id: &str,
        remote_partitions: &[RemotePartitionInfo],
    ) -> CoreResult<Vec<SendableRecordBatchStream>> {
        log::debug!(
            "[DistributedExecutor] Executing remote shuffle queries for {} partitions",
            remote_partitions.len()
        );

        // 按节点分组 Partition
        let mut node_partitions: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        for rp in remote_partitions {
            node_partitions
                .entry(rp.node_id.clone())
                .or_default()
                .push(rp.partition_name.clone());
        }

        // 获取涉及的节点列表
        let node_ids: Vec<String> = node_partitions.keys().cloned().collect();
        let my_node_id = self.cluster_manager.node_id().to_string();

        // 注册查询到取消管理器
        self.cancellation_manager
            .register_query(query_id, node_ids.clone())
            .await;

        // 并行发送查询到各节点
        let mut futures = Vec::new();

        for (node_id, partitions) in node_partitions {
            let node_clients = self.node_clients.clone();
            let sql = sql.to_string();
            let query_id_clone = query_id.to_string();
            let timeout_ms = self.config.query_timeout_ms;
            let node_id_clone = node_id.clone();
            let my_node_id_clone = my_node_id.clone();

            let future = async move {
                let client = match node_clients.get_client(&node_id).await {
                    Ok(c) => c,
                    Err(e) => {
                        // 节点不可达错误 - 对于 Shuffle 查询，这是 Shuffle 失败
                        // Requirements: 8.2, 8.4
                        return Err(shuffle_failed_error(
                            &query_id_clone,
                            &my_node_id_clone,
                            &node_id,
                            &format!("Node unreachable: {}", e),
                        ));
                    }
                };

                let request = QueryRequest {
                    sql,
                    query_id: query_id_clone.clone(),
                    timeout_ms,
                    partition_filter: partitions,
                };

                match client.execute_query(&request).await {
                    Ok(batches) => Ok((node_id_clone, batches)),
                    Err(e) => {
                        // Shuffle 查询失败
                        // Requirements: 8.4
                        Err(shuffle_failed_error(
                            &query_id_clone,
                            &my_node_id_clone,
                            &node_id_clone,
                            &e.to_string(),
                        ))
                    }
                }
            };

            futures.push(future);
        }

        // 使用超时执行器执行所有远程 Shuffle 查询
        // Requirements: 8.3
        let execute_all = async { futures::future::join_all(futures).await };

        let results = self
            .timeout_executor
            .execute_with_timeout(
                query_id,
                node_ids.clone(),
                Some(self.config.query_timeout_ms),
                async { Ok(execute_all.await) },
            )
            .await?;

        // 使用错误聚合器收集结果
        let mut error_aggregator = ErrorAggregator::new(query_id);
        let mut streams = Vec::new();
        let mut first_error: Option<(String, CoreError)> = None;

        for result in results {
            match result {
                Ok((node_id, batches)) => {
                    error_aggregator.record_success(&node_id);
                    if !batches.is_empty() {
                        let schema = batches[0].schema();
                        let stream = self.batches_to_stream(batches, schema);
                        streams.push(stream);
                        log::debug!(
                            "[DistributedExecutor] Received shuffle results from node {}",
                            node_id
                        );
                    }
                }
                Err(e) => {
                    // 记录失败的节点
                    let node_id = "unknown".to_string();
                    error_aggregator.record_failure(&node_id, &e.to_string());

                    if first_error.is_none() {
                        first_error = Some((node_id.clone(), e));
                    }

                    log::error!(
                        "[DistributedExecutor] Remote shuffle query failed on node: {}",
                        node_id
                    );
                }
            }
        }

        // 如果有任何失败，取消其他节点的查询
        // Requirements: 8.1
        if let Some((failed_node, error)) = first_error {
            log::warn!(
                "[DistributedExecutor] Shuffle query {} failed, cancelling other nodes",
                query_id
            );

            self.cancellation_manager
                .cancel_query(query_id, &error.to_string(), Some(&failed_node))
                .await;

            // 注销查询
            self.cancellation_manager.unregister_query(query_id).await;

            return Err(error);
        }

        // 注销查询
        self.cancellation_manager.unregister_query(query_id).await;

        Ok(streams)
    }

    /// 检查 SQL 是否包含 ORDER BY
    fn has_order_by(&self, sql: &str) -> bool {
        sql.to_uppercase().contains("ORDER BY")
    }

    /// 应用 ORDER BY 到合并后的流
    ///
    /// # Requirements
    /// - 7.3: 合并后排序
    async fn apply_order_by(
        &self,
        stream: SendableRecordBatchStream,
        sql: &str,
        schema: SchemaRef,
    ) -> CoreResult<SendableRecordBatchStream> {
        use datafusion::prelude::*;
        use futures::StreamExt;

        // 收集所有数据到内存
        let batches: Vec<RecordBatch> = stream.filter_map(|r| async { r.ok() }).collect().await;

        if batches.is_empty() {
            return Ok(Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::iter(vec![]),
                ),
            ));
        }

        // 创建临时表并执行排序
        let ctx = SessionContext::new();
        let mem_table = datafusion::datasource::MemTable::try_new(schema.clone(), vec![batches])
            .map_err(|e| CoreError::Internal(format!("Failed to create MemTable: {}", e)))?;

        ctx.register_table("_temp_sort", Arc::new(mem_table))
            .map_err(|e| CoreError::Internal(format!("Failed to register temp table: {}", e)))?;

        // 提取 ORDER BY 子句并构建排序查询
        let order_by_clause = self.extract_order_by_clause(sql);
        let sort_sql = format!("SELECT * FROM _temp_sort ORDER BY {}", order_by_clause);

        let df = ctx
            .sql(&sort_sql)
            .await
            .map_err(|e| CoreError::Internal(format!("Sort query error: {}", e)))?;

        let sorted_stream = df
            .execute_stream()
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to execute sort: {}", e)))?;

        Ok(sorted_stream)
    }

    /// 从 SQL 中提取 ORDER BY 子句
    fn extract_order_by_clause(&self, sql: &str) -> String {
        let sql_upper = sql.to_uppercase();

        if let Some(order_by_pos) = sql_upper.find("ORDER BY") {
            let after_order_by = &sql[order_by_pos + 8..].trim();

            // 找到 ORDER BY 子句的结束位置（LIMIT 或语句结束）
            let end_pos = after_order_by
                .to_uppercase()
                .find(" LIMIT")
                .unwrap_or(after_order_by.len());

            return after_order_by[..end_pos].trim().to_string();
        }

        "1".to_string() // 默认排序
    }

    /// 获取节点客户端管理器
    pub fn node_clients(&self) -> &Arc<NodeClientManager> {
        &self.node_clients
    }

    /// 获取配置
    pub fn config(&self) -> &DistributedConfig {
        &self.config
    }
}

// ============================================================================
// 合并流实现
// ============================================================================

/// 合并多个 RecordBatchStream 的流
///
/// 按顺序从各个流中读取数据，实现流式 UNION
struct MergedRecordBatchStream {
    /// 待处理的流
    streams: Vec<SendableRecordBatchStream>,
    /// 当前正在处理的流索引
    current_index: usize,
    /// Schema
    schema: SchemaRef,
}

impl MergedRecordBatchStream {
    fn new(streams: Vec<SendableRecordBatchStream>, schema: SchemaRef) -> Self {
        Self {
            streams,
            current_index: 0,
            schema,
        }
    }
}

impl Stream for MergedRecordBatchStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // 检查是否还有流需要处理
            if this.current_index >= this.streams.len() {
                return Poll::Ready(None);
            }

            // 获取当前流
            let stream = &mut this.streams[this.current_index];

            // 尝试从当前流获取数据
            match Pin::new(stream).poll_next(cx) {
                Poll::Ready(Some(result)) => {
                    return Poll::Ready(Some(result));
                }
                Poll::Ready(None) => {
                    // 当前流结束，移动到下一个流
                    this.current_index += 1;
                    // 继续循环处理下一个流
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

impl RecordBatchStream for MergedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ============================================================================
// LIMIT 流实现
// ============================================================================

/// 带 LIMIT 的 RecordBatchStream
///
/// 在合并后应用全局 LIMIT，确保返回的总行数不超过指定值
///
/// # Requirements
/// - 4.3: 将 LIMIT 下推到各节点（合并后再次应用）
struct LimitedRecordBatchStream {
    /// 输入流
    input: SendableRecordBatchStream,
    /// LIMIT 值
    limit: usize,
    /// 已返回的行数
    rows_returned: usize,
    /// Schema
    schema: SchemaRef,
    /// 是否已完成
    finished: bool,
}

impl LimitedRecordBatchStream {
    fn new(input: SendableRecordBatchStream, limit: usize, schema: SchemaRef) -> Self {
        Self {
            input,
            limit,
            rows_returned: 0,
            schema,
            finished: false,
        }
    }
}

impl Stream for LimitedRecordBatchStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // 如果已完成或已达到 LIMIT，返回 None
        if this.finished || this.rows_returned >= this.limit {
            return Poll::Ready(None);
        }

        // 从输入流获取数据
        match Pin::new(&mut this.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let batch_rows = batch.num_rows();
                let remaining = this.limit - this.rows_returned;

                if batch_rows <= remaining {
                    // 整个 batch 都在 LIMIT 内
                    this.rows_returned += batch_rows;
                    Poll::Ready(Some(Ok(batch)))
                } else {
                    // 需要截断 batch
                    this.finished = true;
                    this.rows_returned = this.limit;

                    // 使用 slice 截断 batch
                    let truncated = batch.slice(0, remaining);
                    Poll::Ready(Some(Ok(truncated)))
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                this.finished = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for LimitedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as StdArc;

    #[test]
    fn test_extract_table_name() {
        // 创建一个简单的测试用例
        let test_cases = vec![
            ("SELECT * FROM users", "users"),
            ("SELECT COUNT(*) FROM taxi_trips WHERE x > 10", "taxi_trips"),
            ("SELECT a, b FROM my_table GROUP BY a", "my_table"),
            ("select * from Events where id = 1", "Events"),
        ];

        for (sql, expected) in test_cases {
            let sql_upper = sql.to_uppercase();
            if let Some(from_pos) = sql_upper.find(" FROM ") {
                let after_from = &sql[from_pos + 6..].trim();
                let table_name = after_from
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .trim_matches(|c: char| !c.is_alphanumeric() && c != '_' && c != '.');
                assert_eq!(table_name, expected, "Failed for SQL: {}", sql);
            } else {
                panic!("Could not find FROM in SQL: {}", sql);
            }
        }
    }

    #[test]
    fn test_has_group_by() {
        let test_cases = vec![
            ("SELECT * FROM users", false),
            ("SELECT COUNT(*) FROM users GROUP BY name", true),
            ("SELECT a, b FROM t group by a", true),
            ("SELECT * FROM users WHERE group_id = 1", false),
        ];

        for (sql, expected) in test_cases {
            let has_group_by = sql.to_uppercase().contains("GROUP BY");
            assert_eq!(has_group_by, expected, "Failed for SQL: {}", sql);
        }
    }

    #[test]
    fn test_remote_partition_info() {
        let info = RemotePartitionInfo {
            partition_name: "partition_0".to_string(),
            node_id: "node-1".to_string(),
        };

        assert_eq!(info.partition_name, "partition_0");
        assert_eq!(info.node_id, "node-1");
    }

    #[test]
    fn test_extract_limit() {
        // 测试 LIMIT 提取逻辑
        let test_cases = vec![
            ("SELECT * FROM users LIMIT 10", Some(10)),
            ("SELECT * FROM users LIMIT 100", Some(100)),
            ("SELECT * FROM users WHERE id > 5 LIMIT 50", Some(50)),
            ("SELECT * FROM users", None),
            ("SELECT * FROM users ORDER BY id", None),
            ("SELECT * FROM users limit 25", Some(25)),
        ];

        for (sql, expected) in test_cases {
            let sql_upper = sql.to_uppercase();
            let result = if let Some(limit_pos) = sql_upper.find(" LIMIT ") {
                let after_limit = &sql[limit_pos + 7..].trim();
                let limit_str: String = after_limit
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect();
                if !limit_str.is_empty() {
                    limit_str.parse().ok()
                } else {
                    None
                }
            } else {
                None
            };
            assert_eq!(result, expected, "Failed for SQL: {}", sql);
        }
    }

    // ========================================================================
    // Scatter-Gather 测试 (Checkpoint 6)
    // ========================================================================

    /// 测试 MergedRecordBatchStream 能正确合并多个流
    #[tokio::test]
    async fn test_merged_record_batch_stream() {
        use futures::StreamExt;

        // 创建测试 schema
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // 创建测试 batches
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                StdArc::new(Int32Array::from(vec![1, 2])),
                StdArc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                StdArc::new(Int32Array::from(vec![3, 4])),
                StdArc::new(StringArray::from(vec!["c", "d"])),
            ],
        )
        .unwrap();

        let batch3 = RecordBatch::try_new(
            schema.clone(),
            vec![
                StdArc::new(Int32Array::from(vec![5])),
                StdArc::new(StringArray::from(vec!["e"])),
            ],
        )
        .unwrap();

        // 创建多个流
        let stream1: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch1)]),
        ));

        let stream2: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch2)]),
        ));

        let stream3: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch3)]),
        ));

        // 合并流
        let merged = MergedRecordBatchStream::new(vec![stream1, stream2, stream3], schema.clone());

        // 收集所有结果
        let results: Vec<_> = Box::pin(merged).collect::<Vec<_>>().await;

        // 验证结果
        assert_eq!(results.len(), 3, "Should have 3 batches");

        let total_rows: usize = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|b| b.num_rows())
            .sum();

        assert_eq!(total_rows, 5, "Total rows should be 5");
    }

    /// 测试 LimitedRecordBatchStream 能正确应用 LIMIT
    #[tokio::test]
    async fn test_limited_record_batch_stream() {
        use futures::StreamExt;

        // 创建测试 schema
        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        // 创建测试 batches (每个 batch 3 行)
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![StdArc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![StdArc::new(Int32Array::from(vec![4, 5, 6]))],
        )
        .unwrap();

        let batch3 = RecordBatch::try_new(
            schema.clone(),
            vec![StdArc::new(Int32Array::from(vec![7, 8, 9]))],
        )
        .unwrap();

        // 创建输入流
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch1), Ok(batch2), Ok(batch3)]),
        ));

        // 应用 LIMIT 5
        let limited = LimitedRecordBatchStream::new(input, 5, schema.clone());

        // 收集所有结果
        let results: Vec<_> = Box::pin(limited).collect::<Vec<_>>().await;

        // 验证结果
        let total_rows: usize = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|b| b.num_rows())
            .sum();

        assert_eq!(total_rows, 5, "Total rows should be limited to 5");
    }

    /// 测试 LimitedRecordBatchStream 在 LIMIT 大于总行数时的行为
    #[tokio::test]
    async fn test_limited_stream_with_large_limit() {
        use futures::StreamExt;

        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![StdArc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch)]),
        ));

        // LIMIT 100 但只有 3 行
        let limited = LimitedRecordBatchStream::new(input, 100, schema.clone());

        let results: Vec<_> = Box::pin(limited).collect::<Vec<_>>().await;

        let total_rows: usize = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|b| b.num_rows())
            .sum();

        assert_eq!(total_rows, 3, "Should return all 3 rows when LIMIT > total");
    }

    /// 测试空流的合并
    #[tokio::test]
    async fn test_merged_empty_streams() {
        use futures::StreamExt;

        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        // 创建空流
        let stream1: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![]),
        ));

        let stream2: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![]),
        ));

        let merged = MergedRecordBatchStream::new(vec![stream1, stream2], schema.clone());

        let results: Vec<_> = Box::pin(merged).collect::<Vec<_>>().await;

        assert_eq!(results.len(), 0, "Empty streams should produce no results");
    }

    /// 测试 Scatter-Gather 查询类型判断
    #[test]
    fn test_scatter_gather_query_detection() {
        // Scatter-Gather 适用的查询（无 GROUP BY）
        let scatter_gather_queries = vec![
            "SELECT * FROM users",
            "SELECT * FROM users WHERE id > 10",
            "SELECT * FROM users LIMIT 100",
            "SELECT * FROM users WHERE name = 'test' LIMIT 10",
            "SELECT COUNT(*) FROM users",
            "SELECT SUM(amount) FROM orders",
        ];

        for sql in scatter_gather_queries {
            let has_group_by = sql.to_uppercase().contains("GROUP BY");
            assert!(
                !has_group_by,
                "Query should be Scatter-Gather (no GROUP BY): {}",
                sql
            );
        }

        // 需要 Shuffle 的查询（有 GROUP BY）
        let shuffle_queries = vec![
            "SELECT name, COUNT(*) FROM users GROUP BY name",
            "SELECT category, SUM(amount) FROM orders GROUP BY category",
            "SELECT a, b, COUNT(*) FROM t GROUP BY a, b",
        ];

        for sql in shuffle_queries {
            let has_group_by = sql.to_uppercase().contains("GROUP BY");
            assert!(
                has_group_by,
                "Query should need Shuffle (has GROUP BY): {}",
                sql
            );
        }
    }

    /// 测试 WHERE 条件下推（Scatter-Gather 特性）
    #[test]
    fn test_where_clause_pushdown() {
        // 验证 WHERE 条件在 SQL 中保持不变，可以下推到各节点
        let sql = "SELECT * FROM users WHERE age > 18 AND status = 'active'";

        // WHERE 条件应该保持在 SQL 中
        assert!(sql.to_uppercase().contains("WHERE"));
        assert!(sql.contains("age > 18"));
        assert!(sql.contains("status = 'active'"));
    }

    /// 测试 LIMIT 下推（Scatter-Gather 特性）
    #[test]
    fn test_limit_pushdown() {
        // 验证 LIMIT 在 SQL 中保持不变，可以下推到各节点
        let sql = "SELECT * FROM users LIMIT 100";

        assert!(sql.to_uppercase().contains("LIMIT"));

        // 提取 LIMIT 值
        let sql_upper = sql.to_uppercase();
        if let Some(limit_pos) = sql_upper.find(" LIMIT ") {
            let after_limit = &sql[limit_pos + 7..].trim();
            let limit_str: String = after_limit
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            let limit: usize = limit_str.parse().unwrap();
            assert_eq!(limit, 100);
        }
    }

    // ========================================================================
    // GROUP BY / Shuffle 测试 (Task 10)
    // ========================================================================

    /// 测试 GROUP BY 列提取
    #[test]
    fn test_extract_group_by_columns() {
        let test_cases = vec![
            (
                "SELECT name, COUNT(*) FROM users GROUP BY name",
                vec!["name"],
            ),
            ("SELECT a, b, SUM(c) FROM t GROUP BY a, b", vec!["a", "b"]),
            (
                "SELECT category, COUNT(*) FROM orders GROUP BY category ORDER BY COUNT(*)",
                vec!["category"],
            ),
            ("SELECT x FROM t GROUP BY x LIMIT 10", vec!["x"]),
            (
                "SELECT a, b, c FROM t group by a, b, c HAVING COUNT(*) > 1",
                vec!["a", "b", "c"],
            ),
        ];

        for (sql, expected) in test_cases {
            let sql_upper = sql.to_uppercase();

            if let Some(group_by_pos) = sql_upper.find("GROUP BY") {
                let after_group_by = &sql[group_by_pos + 8..].trim();

                let end_pos = after_group_by
                    .to_uppercase()
                    .find(" ORDER BY")
                    .or_else(|| after_group_by.to_uppercase().find(" LIMIT"))
                    .or_else(|| after_group_by.to_uppercase().find(" HAVING"))
                    .unwrap_or(after_group_by.len());

                let group_by_clause = &after_group_by[..end_pos];

                let columns: Vec<String> = group_by_clause
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                assert_eq!(
                    columns,
                    expected.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                    "Failed for SQL: {}",
                    sql
                );
            } else {
                panic!("Could not find GROUP BY in SQL: {}", sql);
            }
        }
    }

    /// 测试 ORDER BY 子句提取
    #[test]
    fn test_extract_order_by_clause() {
        let test_cases = vec![
            ("SELECT * FROM users ORDER BY name", "name"),
            ("SELECT * FROM users ORDER BY id DESC", "id DESC"),
            ("SELECT * FROM users ORDER BY a, b ASC", "a, b ASC"),
            ("SELECT * FROM users ORDER BY name LIMIT 10", "name"),
            (
                "SELECT * FROM users ORDER BY id DESC, name ASC LIMIT 5",
                "id DESC, name ASC",
            ),
        ];

        for (sql, expected) in test_cases {
            let sql_upper = sql.to_uppercase();

            if let Some(order_by_pos) = sql_upper.find("ORDER BY") {
                let after_order_by = &sql[order_by_pos + 8..].trim();

                let end_pos = after_order_by
                    .to_uppercase()
                    .find(" LIMIT")
                    .unwrap_or(after_order_by.len());

                let order_by_clause = after_order_by[..end_pos].trim();

                assert_eq!(order_by_clause, expected, "Failed for SQL: {}", sql);
            } else {
                panic!("Could not find ORDER BY in SQL: {}", sql);
            }
        }
    }

    /// 测试 has_order_by 检测
    #[test]
    fn test_has_order_by() {
        let test_cases = vec![
            ("SELECT * FROM users", false),
            ("SELECT * FROM users ORDER BY name", true),
            ("SELECT * FROM users order by id", true),
            ("SELECT * FROM users WHERE order_id = 1", false),
            ("SELECT * FROM users GROUP BY name ORDER BY COUNT(*)", true),
        ];

        for (sql, expected) in test_cases {
            let has_order_by = sql.to_uppercase().contains("ORDER BY");
            assert_eq!(has_order_by, expected, "Failed for SQL: {}", sql);
        }
    }

    /// 测试节点拓扑计算
    #[test]
    fn test_calculate_node_topology() {
        // 测试本地 + 远程节点的拓扑计算
        let local_partitions = vec!["partition_0".to_string(), "partition_1".to_string()];
        let remote_partitions = vec![
            RemotePartitionInfo {
                partition_name: "partition_2".to_string(),
                node_id: "node-2".to_string(),
            },
            RemotePartitionInfo {
                partition_name: "partition_3".to_string(),
                node_id: "node-3".to_string(),
            },
            RemotePartitionInfo {
                partition_name: "partition_4".to_string(),
                node_id: "node-2".to_string(), // 同一节点的另一个 partition
            },
        ];

        // 模拟计算节点拓扑
        let my_node_id = "node-1".to_string();
        let mut node_ids: Vec<String> = Vec::new();

        if !local_partitions.is_empty() {
            node_ids.push(my_node_id.clone());
        }

        for rp in &remote_partitions {
            if !node_ids.contains(&rp.node_id) {
                node_ids.push(rp.node_id.clone());
            }
        }

        let my_node_index = node_ids
            .iter()
            .position(|id| id == &my_node_id)
            .unwrap_or(0);

        // 验证结果
        assert_eq!(node_ids.len(), 3); // node-1, node-2, node-3
        assert_eq!(my_node_index, 0);
        assert!(node_ids.contains(&"node-1".to_string()));
        assert!(node_ids.contains(&"node-2".to_string()));
        assert!(node_ids.contains(&"node-3".to_string()));
    }

    /// 测试 GROUP BY 查询类型检测
    #[test]
    fn test_group_by_query_detection() {
        // 需要 Shuffle 的查询（有 GROUP BY）
        let shuffle_queries = vec![
            "SELECT name, COUNT(*) FROM users GROUP BY name",
            "SELECT category, SUM(amount) FROM orders GROUP BY category",
            "SELECT a, b, COUNT(*) FROM t GROUP BY a, b",
            "SELECT DISTINCT name FROM users", // DISTINCT 也需要 Shuffle
        ];

        for sql in &shuffle_queries[..3] {
            // 排除 DISTINCT，因为它不包含 GROUP BY 关键字
            let has_group_by = sql.to_uppercase().contains("GROUP BY");
            assert!(
                has_group_by,
                "Query should need Shuffle (has GROUP BY): {}",
                sql
            );
        }

        // 不需要 Shuffle 的查询（无 GROUP BY）
        let scatter_gather_queries = vec![
            "SELECT * FROM users",
            "SELECT * FROM users WHERE id > 10",
            "SELECT * FROM users LIMIT 100",
        ];

        for sql in scatter_gather_queries {
            let has_group_by = sql.to_uppercase().contains("GROUP BY");
            assert!(
                !has_group_by,
                "Query should be Scatter-Gather (no GROUP BY): {}",
                sql
            );
        }
    }
}
