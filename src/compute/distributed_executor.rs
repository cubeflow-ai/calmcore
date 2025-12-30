//! Distributed executor using datafusion-distributed
//!
//! 集成 datafusion-distributed 实现真正的分布式查询

use std::sync::Arc;

use datafusion::datasource::empty::EmptyTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::*;
use datafusion_distributed::{DistributedExt, DistributedPhysicalOptimizerRule};

use super::natural_order_executor::NaturalOrderExecutor;
use super::{CalmChannelResolver, PartitionAwareTaskEstimator};
use crate::catalog::Catalog;
use crate::cluster::ClusterManager;
use crate::compute::information_schema_executor::InformationSchemaExecutor;
use crate::compute::{SqlNormalizer, UnionTableProvider};
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

/// 分布式 DataFusion 执行器
///
/// 使用 datafusion-distributed 提供真正的分布式查询能力
pub struct DistributedDataFusionExecutor {
    engine: Arc<Engine>,
    catalog: Arc<Catalog>,
    cluster_manager: Option<Arc<ClusterManager>>,
    natural_order_executor: NaturalOrderExecutor,
    information_schema_executor: InformationSchemaExecutor,
}

impl DistributedDataFusionExecutor {
    pub fn new(
        engine: Arc<Engine>,
        catalog: Arc<Catalog>,
        cluster_manager: Option<Arc<ClusterManager>>,
    ) -> Self {
        Self {
            natural_order_executor: NaturalOrderExecutor::new(engine.clone()),
            information_schema_executor: InformationSchemaExecutor::new(
                engine.clone(),
                catalog.clone(),
            ),
            engine,
            catalog,
            cluster_manager,
        }
    }

    /// 执行 SQL 查询（流式版本，支持分布式）
    pub async fn execute_sql_stream(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
        log::info!("🌐 [Distributed Executor] Executing SQL: {}", sql);

        // 标准化 SQL
        let normalized = SqlNormalizer::normalize(sql)?;

        if normalized.rewritten_sql != sql {
            log::info!(
                "🔄 [SQL Normalized] {} -> {}",
                sql,
                normalized.rewritten_sql
            );
        }

        // 特殊处理: INFORMATION_SCHEMA 查询
        let normalized_upper = normalized.rewritten_sql.to_uppercase();
        if normalized_upper.contains("INFORMATION_SCHEMA") {
            log::info!("🗂️ [INFORMATION_SCHEMA] Detected metadata query");
            return self
                .information_schema_executor
                .execute_stream(&normalized.rewritten_sql)
                .await;
        }

        // 特殊处理: Natural Order 优化
        if normalized_upper.contains("ORDER BY _NATURE")
            || normalized_upper.contains("ORDER BY `_NATURE`")
        {
            log::info!("🌿 [Natural Order] Using streaming cursor");
            return self.execute_natural_order(&normalized.rewritten_sql).await;
        }

        // 提取表名（需要在创建 Context 前获取）
        let table_name = self.extract_table_name(&normalized.rewritten_sql)?;
        log::info!("📋 [Distributed Query] Target table: {}", table_name);

        // 🎯 打印分区过滤信息
        if normalized.partition_filters.has_filter {
            log::info!(
                "🔍 [Partition Filter] Exact matches: {:?}, LIKE patterns: {:?}",
                normalized.partition_filters.exact_matches,
                normalized.partition_filters.like_patterns
            );
        } else {
            log::debug!("📊 [No Partition Filter] Will scan all partitions");
        }

        // 创建分布式 SessionContext（基于目标表的节点分布）
        let ctx = if let Some(_cluster_manager) = &self.cluster_manager {
            // 分布式模式：基于目标表创建 ChannelResolver
            self.create_distributed_context(&table_name).await?
        } else {
            // 单机模式（fallback）
            SessionContext::new()
        };

        // 注册 TableProvider（传递 partition_filters）
        self.register_table(&ctx, &table_name, &normalized.partition_filters)
            .await?;

        // 执行查询
        let df = ctx
            .sql(&normalized.rewritten_sql)
            .await
            .map_err(|e| CoreError::Internal(e.to_string()))?;

        let stream = df
            .execute_stream()
            .await
            .map_err(|e| CoreError::Internal(e.to_string()))?;

        Ok(stream)
    }

    /// 创建分布式 SessionContext（基于目标表的节点分布）
    async fn create_distributed_context(&self, table_name: &str) -> CoreResult<SessionContext> {
        use crate::compute::LazyPartitionCodec;
        use datafusion_distributed::DistributedExt;

        // 🎯 关键：基于目标表创建 ChannelResolver
        // 这样只会连接到拥有该表数据的节点
        let channel_resolver =
            CalmChannelResolver::new_for_table(&self.catalog, table_name).await?;

        // 创建 TaskEstimator (不要包装在 Arc 中，trait 本身会处理)
        let task_estimator = PartitionAwareTaskEstimator::new(self.catalog.clone());

        // 创建分布式优化规则
        let distributed_rule = Arc::new(DistributedPhysicalOptimizerRule);

        // 获取本地节点 ID
        let my_node_id = if let Some(cluster_manager) = &self.cluster_manager {
            cluster_manager.node_id().to_string()
        } else {
            "standalone".to_string()
        };

        log::info!(
            "🆔 [DistributedExecutor] Injecting node_id '{}' into SessionConfig for table '{}'",
            my_node_id,
            table_name
        );

        // 创建 SessionConfig 并注入 Engine 和 node_id
        let config = SessionConfig::default()
            .with_extension(self.engine.clone())
            .with_extension(Arc::new(my_node_id.clone()));

        // 构建 SessionState
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_physical_optimizer_rule(distributed_rule)
            .with_distributed_task_estimator(task_estimator)
            .with_distributed_channel_resolver(channel_resolver)
            .with_distributed_user_codec(LazyPartitionCodec) // ✅ 注册自定义序列化 codec
            .build();

        log::info!(
            "✅ [Distributed Context] Created for table '{}' with datafusion-distributed + LazyPartitionCodec",
            table_name
        );

        Ok(SessionContext::new_with_state(state))
    }

    /// Natural Order 执行
    async fn execute_natural_order(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::stream;

        let stream_result = self
            .natural_order_executor
            .execute_natural_cursor(sql)
            .await?;

        let schema = stream_result.schema.clone();
        let mut receiver = stream_result.receiver;

        let stream = stream::poll_fn(move |cx| {
            use std::task::Poll;

            match receiver.poll_recv(cx) {
                Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(
                    datafusion::error::DataFusionError::External(Box::new(e)),
                ))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        });

        let adapter = RecordBatchStreamAdapter::new(schema, stream);
        Ok(Box::pin(adapter))
    }

    /// 从 SQL 中提取表名
    fn extract_table_name(&self, sql: &str) -> CoreResult<String> {
        // 简单的表名提取逻辑
        let sql_upper = sql.to_uppercase();

        if let Some(from_pos) = sql_upper.find("FROM") {
            let after_from = &sql[from_pos + 4..].trim();
            let table_name = after_from
                .split_whitespace()
                .next()
                .ok_or_else(|| CoreError::Internal("Cannot extract table name".to_string()))?;

            Ok(table_name.to_string())
        } else {
            Err(CoreError::Internal("No FROM clause found".to_string()))
        }
    }

    /// 注册表到 SessionContext（只注册本地节点拥有的 partition）
    ///
    /// **分布式架构设计**：
    /// - 每个节点只扫描自己拥有的 partition（基于 PartitionMeta.owner）
    /// - 支持 _partition 过滤：根据 WHERE _partition='xxx' 条件裁剪分区
    /// - 使用本地的 UnionTableProvider + MultiSegmentExec（支持内存数据和标记删除）
    /// - datafusion-distributed 协调多节点查询，但每个节点独立执行本地数据扫描
    /// - 不涉及跨节点序列化自定义执行计划
    async fn register_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        partition_filters: &crate::compute::PartitionFilters,
    ) -> CoreResult<()> {
        log::info!(
            "📋 [DistributedExecutor] Registering table '{}' (local partitions only)",
            table_name
        );

        // 从 catalog 获取表信息
        let table_info = self.catalog.get_or_load_table(table_name).await?;
        let partition_meta = table_info.partitions.read().await;

        // 获取本地节点 ID
        let my_node_id = if let Some(cluster_manager) = &self.cluster_manager {
            cluster_manager.node_id().to_string()
        } else {
            // 单机模式：所有 partition 都是本地的
            "standalone".to_string()
        };

        // 🎯 应用 _partition 过滤
        let all_partition_names: Vec<String> = partition_meta.keys().cloned().collect();

        log::info!(
            "🔍 [Partition Debug] All partition names in catalog: {:?}",
            all_partition_names
        );
        log::info!(
            "🔍 [Partition Debug] Filter exact_matches: {:?}, like_patterns: {:?}",
            partition_filters.exact_matches,
            partition_filters.like_patterns
        );

        let filtered_partition_names = partition_filters.resolve_partitions(&all_partition_names);

        if partition_filters.has_filter {
            log::info!(
                "🔍 [Partition Pruning] {} partitions matched filter (total: {})",
                filtered_partition_names.len(),
                all_partition_names.len()
            );
            log::info!(
                "🔍 [Partition Pruning] Matched partitions: {:?}",
                filtered_partition_names
            );
        }

        // 只加载本地节点拥有的、且通过过滤的 Partition
        let mut local_partitions = Vec::new();
        let mut partition_owners = std::collections::HashMap::new();

        // 🔍 调试：检查 Engine 中实际有哪些分区
        let engine_partitions = self.engine.list_all_spartitions().await;
        let table_partitions_in_engine: Vec<String> = engine_partitions
            .iter()
            .filter(|(t, _)| t == table_name)
            .map(|(_, p)| p.clone())
            .collect();
        log::info!(
            "🔍 [Engine Debug] Partitions in Engine for table '{}': {:?}",
            table_name,
            table_partitions_in_engine
        );

        for (partition_name, partition_info) in partition_meta.iter() {
            // ✅ 应用分区过滤 - 先过滤再记录
            if !filtered_partition_names.contains(partition_name) {
                log::debug!("⏭️  Skipping filtered partition: {}", partition_name);
                continue;
            }

            // 🎯 记录通过过滤的 partition 的 owner（用于 LazyPartitionExec 路由）
            partition_owners.insert(partition_name.clone(), partition_info.owner.clone());

            // 🎯 关键：直接尝试从本地 Engine 获取，如果存在就说明是本地分区
            // 不需要检查 owner，因为 Engine 中只有本地分区
            if let Some(partition) = self.engine.get_partition(table_name, partition_name).await {
                local_partitions.push(partition);
                log::info!(
                    "✅ Loaded local partition: {} (owner: {})",
                    partition_name,
                    partition_info.owner
                );
            } else {
                log::info!(
                    "⏭️  Partition '{}' not in local Engine (owner: {})",
                    partition_name,
                    partition_info.owner
                );
            }
        }

        log::info!(
            "📦 [DistributedExecutor] Found {} local partitions for table '{}' on node '{}' (after filtering)",
            local_partitions.len(),
            table_name,
            my_node_id
        );

        // 🎯 关键：即使本地没有匹配的分区，接收请求的节点也必须注册表
        // 这样查询规划器才能找到表定义，然后 datafusion-distributed 会从其他节点获取数据
        if local_partitions.is_empty() {
            log::info!(
                "ℹ️  No local partitions matched filter for table '{}' on node '{}', registering EmptyTable for schema",
                table_name,
                my_node_id
            );

            // 从 catalog 获取表的 schema
            let schema = table_info.table.schema.to_arrow_schema();
        }

        let local_partition_len = local_partitions.len();

        // 创建 UnionTableProvider (只有非空时才创建)
        let union_provider = UnionTableProvider::new(
            local_partitions,
            table_name.to_string(),
            self.engine.clone(),
            partition_owners,
        )?;

        // 注册表
        ctx.register_table(table_name, Arc::new(union_provider))
            .map_err(|e| CoreError::Internal(e.to_string()))?;

        log::info!(
            "✅ [DistributedExecutor] Table '{}' registered successfully with {} local partitions",
            table_name,
            local_partition_len
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_extraction() {
        let executor = DistributedDataFusionExecutor::new(
            Arc::new(Engine::new("test".to_string())),
            Arc::new(Catalog::new()),
            None,
        );

        let sql = "SELECT * FROM users WHERE id = 1";
        let table = executor.extract_table_name(sql).unwrap();
        assert_eq!(table, "users");
    }
}
