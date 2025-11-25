/// 结果合并工具
///
/// 提供 RecordBatch 合并、空结果创建等功能
use std::sync::Arc;

use datafusion::arrow::array::{new_empty_array, ArrayRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;

use crate::compute::PartitionTableProvider;
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

/// 结果合并器
pub struct ResultMerger;

impl ResultMerger {
    pub fn new() -> Self {
        Self
    }

    /// 合并多个 RecordBatch 为一个
    pub fn concat_batches(&self, batches: Vec<RecordBatch>) -> CoreResult<RecordBatch> {
        if batches.is_empty() {
            return Err(CoreError::Internal("No batches to concat".to_string()));
        }

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        use datafusion::arrow::compute::concat_batches;
        let schema = batches[0].schema();
        concat_batches(&schema, &batches)
            .map_err(|e| CoreError::Internal(format!("Failed to concat batches: {}", e)))
    }

    /// 通过执行 SQL 查询来创建空的 RecordBatch（获取正确的 schema）
    pub async fn create_empty_batch_from_sql(
        &self,
        sql: &str,
        table_name: &str,
        engine: &Arc<Engine>,
    ) -> CoreResult<RecordBatch> {
        let ctx = SessionContext::new();

        // 获取第一个 partition 来注册表（只是为了获取 schema）
        let partition_names = engine.list_partitions(table_name).await;
        let first_partition_name = partition_names.into_iter().next().ok_or_else(|| {
            CoreError::NotExisted(format!("No partitions found for table '{}'", table_name))
        })?;

        let partition = engine
            .get_partition(table_name, &first_partition_name)
            .await
            .ok_or_else(|| {
                CoreError::NotExisted(format!(
                    "Partition {} not found for table '{}'",
                    first_partition_name, table_name
                ))
            })?;

        let provider = Arc::new(PartitionTableProvider::new(partition));
        ctx.register_table(table_name, provider).map_err(|e| {
            CoreError::Internal(format!("Failed to register table '{}': {}", table_name, e))
        })?;

        // 执行查询但添加 WHERE 1=0 来确保没有结果，只获取 schema
        let empty_sql = self.make_empty_sql(sql, table_name);

        let df = ctx
            .sql(&empty_sql)
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to create empty dataframe: {}", e)))?;

        // 在 collect 之前获取 schema
        let schema = df.schema().inner().clone();

        let batches = df
            .collect()
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to collect empty batches: {}", e)))?;

        // 如果有结果（不应该，因为 WHERE 1=0），取第一个
        if let Some(batch) = batches.first() {
            return Ok(batch.clone());
        }

        // 否则创建一个真正空的 batch
        self.create_empty_batch_with_schema(schema)
    }

    /// 使用给定 schema 创建空的 RecordBatch
    pub fn create_empty_batch_with_schema(
        &self,
        schema: Arc<datafusion::arrow::datatypes::Schema>,
    ) -> CoreResult<RecordBatch> {
        let empty_columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();

        RecordBatch::try_new(schema, empty_columns)
            .map_err(|e| CoreError::Internal(format!("Failed to create empty RecordBatch: {}", e)))
    }

    /// 将 SQL 改造为返回空结果（添加 WHERE 1=0）
    fn make_empty_sql(&self, sql: &str, table_name: &str) -> String {
        let mut empty_sql = if sql.to_uppercase().contains("WHERE") {
            // 如果已经有 WHERE 子句，在现有条件前添加 FALSE AND (...)
            let where_pos = sql.to_uppercase().find("WHERE").unwrap();
            let before_where = &sql[..where_pos];
            let after_where = &sql[where_pos + 5..]; // 跳过 "WHERE"

            // 查找 ORDER BY、GROUP BY、HAVING、LIMIT 等子句的位置
            let order_pos = after_where.to_uppercase().find("ORDER BY");
            let group_pos = after_where.to_uppercase().find("GROUP BY");
            let having_pos = after_where.to_uppercase().find("HAVING");
            let limit_pos = after_where.to_uppercase().find("LIMIT");

            // 找到最小的位置（最早出现的子句）
            let min_pos = [order_pos, group_pos, having_pos, limit_pos]
                .iter()
                .filter_map(|&pos| pos)
                .min();

            if let Some(pos) = min_pos {
                // 如果有其他子句，只包装 WHERE 条件部分
                let where_condition = after_where[..pos].trim();
                let rest = &after_where[pos..];
                format!(
                    "{}WHERE 1=0 AND ({}) {}",
                    before_where, where_condition, rest
                )
            } else {
                // 如果没有其他子句，包装整个 WHERE 条件
                format!("{}WHERE 1=0 AND ({})", before_where, after_where.trim())
            }
        } else {
            // 如果没有 WHERE 子句，简单添加 WHERE 1=0
            sql.replace(
                &format!("FROM {}", table_name),
                &format!("FROM {} WHERE 1=0", table_name),
            )
        };

        // 移除 ORDER BY _nature 子句（因为 _nature 是虚拟列，不在表 schema 中）
        log::debug!("Original empty SQL: {}", empty_sql);

        let sql_upper = empty_sql.to_uppercase();
        if let Some(order_pos) = sql_upper.find("ORDER BY") {
            // 获取 ORDER BY 之后的内容
            let after_order_upper = sql_upper[order_pos + 8..].trim_start(); // "ORDER BY".len() = 8

            // 检查是否是 ORDER BY _nature (允许空格和反引号)
            let is_nature_order = after_order_upper.starts_with("_NATURE")
                || after_order_upper.starts_with("`_NATURE`");

            log::debug!(
                "Found ORDER BY at position {}, is_nature_order: {}",
                order_pos,
                is_nature_order
            );

            if is_nature_order {
                // 找到 LIMIT 或 OFFSET 的位置
                let after_order = &sql_upper[order_pos..];
                let limit_pos = after_order.find("LIMIT");
                let offset_pos = after_order.find("OFFSET");

                // 找到第一个出现的位置
                let end_pos = match (limit_pos, offset_pos) {
                    (Some(l), Some(o)) => order_pos + l.min(o),
                    (Some(l), None) => order_pos + l,
                    (None, Some(o)) => order_pos + o,
                    (None, None) => empty_sql.len(),
                };

                // 移除 ORDER BY _nature 部分
                let before = empty_sql[..order_pos].trim();
                let after = if end_pos < empty_sql.len() {
                    empty_sql[end_pos..].trim()
                } else {
                    ""
                };

                empty_sql = if after.is_empty() {
                    before.to_string()
                } else {
                    format!("{} {}", before, after)
                };

                log::debug!("Removed ORDER BY _nature, new SQL: {}", empty_sql);
            }
        }

        empty_sql
    }
}
