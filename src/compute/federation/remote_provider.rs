//! 远程表 Provider - 将远程分区包装为 TableProvider

use crate::compute::federation::flight_executor::FlightExecutor;
use crate::compute::federation::remote_scan_exec::RemoteScanExec;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
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
        // 构造 SELECT 子句
        let select_clause = if let Some(proj) = projection {
            let schema = self.schema();
            let fields: Vec<String> = proj
                .iter()
                .map(|i| schema.field(*i).name().clone())
                .collect();
            fields.join(", ")
        } else {
            "*".to_string()
        };

        // 构造 WHERE 子句（filter pushdown）
        let where_clause = if !filters.is_empty() {
            let filter_strs: Vec<String> = filters
                .iter()
                .filter_map(|expr| Self::expr_to_sql(expr).ok())
                .collect();

            if !filter_strs.is_empty() {
                format!(" WHERE {}", filter_strs.join(" AND "))
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        // 构造完整 SQL
        let mut sql = format!(
            "SELECT {} FROM {}{}",
            select_clause, self.table_name, where_clause
        );

        if let Some(n) = limit {
            sql = format!("{} LIMIT {}", sql, n);
        }

        log::debug!(
            "[RemoteTableProvider] Generated SQL for partitions {:?}: {}",
            self.partition_names,
            sql
        );

        // 创建 RemoteScanExec
        Ok(Arc::new(RemoteScanExec {
            table_name: self.table_name.clone(),
            partition_ids: self.partition_names.clone(),
            schema: self.schema.clone(),
            sql,
            projection: projection.cloned(),
            executor: self.executor.clone(),
        }))
    }
}

impl RemoteTableProvider {
    /// 将 DataFusion Expr 转换为 SQL WHERE 条件（简化版本）
    fn expr_to_sql(expr: &Expr) -> DataFusionResult<String> {
        use datafusion::logical_expr::Operator;
        use datafusion::scalar::ScalarValue;

        match expr {
            Expr::BinaryExpr(binary_expr) => {
                let left = Self::expr_to_sql(binary_expr.left.as_ref())?;
                let right = Self::expr_to_sql(binary_expr.right.as_ref())?;
                let op = match binary_expr.op {
                    Operator::Eq => "=",
                    Operator::NotEq => "!=",
                    Operator::Lt => "<",
                    Operator::LtEq => "<=",
                    Operator::Gt => ">",
                    Operator::GtEq => ">=",
                    Operator::And => "AND",
                    Operator::Or => "OR",
                    _ => {
                        return Err(datafusion::error::DataFusionError::NotImplemented(format!(
                            "Operator {:?} not supported in filter pushdown",
                            binary_expr.op
                        )))
                    }
                };
                Ok(format!("{} {} {}", left, op, right))
            }
            Expr::Column(col) => Ok(col.name.clone()),
            Expr::Literal(scalar, _metadata) => match scalar {
                ScalarValue::Utf8(Some(s)) => Ok(format!("'{}'", s.replace("'", "''"))),
                ScalarValue::Int64(Some(i)) => Ok(i.to_string()),
                ScalarValue::UInt64(Some(u)) => Ok(u.to_string()),
                ScalarValue::Float64(Some(f)) => Ok(f.to_string()),
                ScalarValue::Boolean(Some(b)) => Ok(b.to_string()),
                _ => Err(datafusion::error::DataFusionError::NotImplemented(format!(
                    "Scalar value {:?} not supported in filter pushdown",
                    scalar
                ))),
            },
            _ => Err(datafusion::error::DataFusionError::NotImplemented(format!(
                "Expression {:?} not supported in filter pushdown",
                expr
            ))),
        }
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
