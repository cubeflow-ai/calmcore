// Executor 层
mod ballista_executor;
mod information_schema_executor;
pub mod natural_order_executor; // 公开给 MySQL protocol 层使用

// Table Provider 层
pub mod table_provider;

// SQL 工具
pub mod sql_normalizer;

// 分布式查询模块（零侵入设计，不影响单机代码路径）
pub mod distributed;

use std::sync::Arc;

use crate::engine::Engine;
use crate::utils::error::CoreResult;

use ballista_executor::DataFusionExecutor;
use distributed::DistributedExecutor;

// Re-exports
pub use sql_normalizer::{NormalizedSql, PartitionFilters, SqlNormalizer};
pub use table_provider::{PartitionTableProvider, UnionTableProvider};

/// 执行器后端类型
///
/// 根据 Engine 的配置决定使用哪种执行器：
/// - Local: 单机模式，使用 DataFusion 执行器
/// - Distributed: 分布式模式，使用分布式执行器
enum ExecutorBackend {
    /// 本地执行器（单机模式）
    Local(DataFusionExecutor),
    /// 分布式执行器（集群模式）
    Distributed(DistributedExecutor),
}

/// 查询执行器（路由器）
///
/// 核心职责：
/// 1. SQL 标准化和查询分析
/// 2. 根据 Engine 配置自动选择执行器类型
/// 3. 返回查询结果
///
/// 设计原则：
/// - 零侵入：上层代码不感知单机/分布式
/// - 单一职责：只做路由
/// - 执行逻辑由专门的 executor 负责
pub struct Executor {
    backend: ExecutorBackend,
}

impl Executor {
    /// 创建新的查询执行器
    ///
    /// 根据 Engine 的分布式上下文决定使用哪种后端：
    /// - 有分布式上下文：使用 DistributedExecutor
    /// - 无分布式上下文：使用 DataFusionExecutor（单机模式）
    pub fn new(engine: Arc<Engine>) -> Self {
        // 注意：这里使用 try_read 避免异步，因为 new() 不是 async
        // 分布式上下文在启动时设置，之后不会改变
        let distributed_context = {
            // 使用 blocking 方式获取锁，因为这是初始化阶段
            futures::executor::block_on(async { engine.get_distributed_context().await })
        };

        let backend = if let Some(ctx) = distributed_context {
            log::debug!("[Executor] Using distributed backend");
            ExecutorBackend::Distributed(DistributedExecutor::new(
                engine,
                ctx.cluster_manager,
                ctx.partition_manager,
                ctx.config,
            ))
        } else {
            log::debug!("[Executor] Using local backend");
            ExecutorBackend::Local(DataFusionExecutor::new(engine))
        };

        Self { backend }
    }

    /// 执行 SQL 查询（流式版本）
    ///
    /// 返回 DataFusion 的原生 Stream，避免全部加载到内存
    /// 自动根据后端类型选择执行路径
    pub async fn execute_sql_stream(
        &self,
        sql: &str,
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        match &self.backend {
            ExecutorBackend::Local(executor) => executor.execute_sql_stream(sql).await,
            ExecutorBackend::Distributed(executor) => executor.execute_sql(sql).await,
        }
    }
}
