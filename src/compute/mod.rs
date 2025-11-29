// Executor 层
mod ballista_executor;
mod natural_order_executor;

// Table Provider 层
pub mod table_provider;

// SQL 工具
pub mod sql_normalizer;

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;

use crate::engine::Engine;
use crate::utils::error::CoreResult;

use ballista_executor::DataFusionExecutor;

// Re-exports
pub use sql_normalizer::SqlNormalizer;
pub use table_provider::{PartitionTableProvider, UnionTableProvider};

/// 查询结果
#[derive(Debug)]
pub struct QueryResult {
    /// 查询结果数据
    pub batch: RecordBatch,
    /// 命中的文档数量（在索引中匹配的记录数）
    pub matched_docs: usize,
}

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
    engine: Arc<Engine>,
}

impl Executor {
    /// 创建新的查询执行器
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            datafusion_executor: DataFusionExecutor::new(engine.clone()),
            engine,
        }
    }

    /// 执行 SQL 查询
    ///
    /// 所有查询都通过 DataFusion 执行
    pub async fn execute_sql(&self, sql: &str) -> CoreResult<QueryResult> {
        // 直接使用 DataFusion executor
        self.datafusion_executor.execute_sql(sql).await
    }
}
