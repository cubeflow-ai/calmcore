//! 远程表 Provider - 将远程分区包装为 TableProvider

use crate::compute::federation::flight_executor::FlightExecutor;
use crate::compute::federation::remote_scan_exec::RemoteScanExec;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::unparser::plan_to_sql;
use std::any::Any;
use std::sync::Arc;

/// 远程表 Provider
///
/// 将远程节点的分区包装为 DataFusion TableProvider
/// 通过 Arrow Flight 与远程节点通信
pub struct RemoteTableProvider {
    /// 表名
    table_name: String,
    /// 分区名列表
    partition_names: Vec<String>,
    /// Schema
    schema: SchemaRef,
    /// Flight 执行器
    executor: Arc<FlightExecutor>,
}

impl RemoteTableProvider {
    /// 创建新的远程表 Provider
    pub fn new(
        table_name: String,
        partition_names: Vec<String>,
        schema: SchemaRef,
        executor: Arc<FlightExecutor>,
    ) -> Self {
        Self {
            table_name,
            partition_names,
            schema,
            executor,
        }
    }
}

#[async_trait]
impl TableProvider for RemoteTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // 支持 filter 下推到远程节点
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // 使用 DataFusion 官方的 plan_to_sql() 来生成 SQL
        // 注意：不要在 SQL 中包含 _partition 条件！
        // 远程节点作为 partition owner，自然只会扫描它拥有的分区

        // 1. 构建 LogicalPlan（从 TableScan 开始）
        let mut plan_builder = LogicalPlanBuilder::scan(
            self.table_name.clone(),
            datafusion::datasource::provider_as_source(Arc::new(
                datafusion::datasource::empty::EmptyTable::new(self.schema.clone()),
            )),
            None,
        )?;

        // 2. 应用用户的 filters（不包含 _partition）
        if !filters.is_empty() {
            for filter in filters {
                plan_builder = plan_builder.filter(filter.clone())?;
            }
        }

        // 3. 应用 projection
        if let Some(proj) = projection {
            if proj.is_empty() {
                // 🚀 COUNT(*) 优化：空投影表示聚合查询，投影一个常量列以保持有效性
                // 远程节点会返回最小数据量（只有行数，无实际列数据）
                log::debug!(
                    "[RemoteTableProvider] Empty projection detected (COUNT query), using minimal projection"
                );
                plan_builder = plan_builder.project(vec![Expr::Literal(
                    datafusion::scalar::ScalarValue::Int32(Some(1)),
                    None,
                )])?;
            } else {
                let exprs: Vec<Expr> = proj
                    .iter()
                    .map(|i| {
                        Expr::Column(datafusion::common::Column::from(
                            self.schema.field(*i).name(),
                        ))
                    })
                    .collect();
                plan_builder = plan_builder.project(exprs)?;
            }
        }

        // 4. 应用 limit
        if let Some(n) = limit {
            plan_builder = plan_builder.limit(0, Some(n))?;
        }

        let logical_plan = plan_builder.build()?;

        // 5. 使用 DataFusion 官方的 unparser 将 LogicalPlan 转回 SQL
        let ast = plan_to_sql(&logical_plan)?;
        let sql = ast.to_string();

        log::debug!(
            "[RemoteTableProvider] Generated SQL (via plan_to_sql) for remote node {} (partitions: {:?}): {}",
            self.executor.node_id(),
            self.partition_names,
            sql
        );

        // 创建 RemoteScanExec
        // partition_names 仅用于日志记录，远程节点会自动扫描它拥有的所有分区
        Ok(Arc::new(RemoteScanExec::new(
            self.table_name.clone(),
            self.partition_names.clone(),
            self.schema.clone(),
            sql,
            projection.cloned(),
            self.executor.clone(),
        )))
    }
}

impl std::fmt::Debug for RemoteTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteTableProvider")
            .field("table_name", &self.table_name)
            .field("partition_count", &self.partition_names.len())
            .field("node_id", &self.executor.node_id())
            .finish()
    }
}
