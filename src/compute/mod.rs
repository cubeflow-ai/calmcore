// Executor 层
mod ballista_executor;
mod information_schema_executor;
pub mod natural_order_executor; // 公开给 MySQL protocol 层使用

// Table Provider 层
pub mod table_provider;

// SQL 工具
pub mod sql_normalizer;

use std::sync::Arc;

use crate::engine::Engine;
use crate::utils::error::CoreResult;

use ballista_executor::DataFusionExecutor;

// Re-exports
pub use sql_normalizer::{NormalizedSql, PartitionFilters, SqlNormalizer};
pub use table_provider::{PartitionTableProvider, UnionTableProvider};

/// 查询执行器（路由器）
///
/// 核心职责：
/// 1. SQL 标准化和查询分析
/// 2. 根据 QueryType 路由到专门的执行器
/// 3. 返回查询结果
///
/// 设计原则：
/// - 扁平化路由，无嵌套分支
/// - 单一职责,只做路由
/// - 执行逻辑由专门的 executor 负责
pub struct Executor {
    datafusion_executor: DataFusionExecutor,
}

impl Executor {
    /// 创建新的查询执行器
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            datafusion_executor: DataFusionExecutor::new(engine),
        }
    }

    /// 执行 SQL 查询（流式版本）
    ///
    /// 返回 DataFusion 的原生 Stream，避免全部加载到内存
    pub async fn execute_sql_stream(
        &self,
        sql: &str,
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        self.datafusion_executor.execute_sql_stream(sql).await
    }
}
