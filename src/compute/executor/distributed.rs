use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;

use crate::compute::optimizer::{analyze_query, QueryType};
use crate::compute::sql_normalizer::SqlNormalizer;
use crate::compute::PartitionTableProvider;
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

use super::aggregation::AggregationMerger;
use super::query_builder::QueryBuilder;

/// 查询结果
#[derive(Debug)]
pub struct QueryResult {
    /// 查询结果数据
    pub batch: RecordBatch,
    /// 命中的文档数量（在索引中匹配的记录数）
    pub matched_docs: usize,
}

/// 分布式查询执行器
///
/// 负责协调多个 partition 的查询执行和结果合并
pub struct DistributedExecutor {
    engine: Arc<Engine>,
    query_builder: QueryBuilder,
    aggregation_merger: AggregationMerger,
}

impl DistributedExecutor {
    /// 创建新的分布式查询执行器
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine: engine.clone(),
            query_builder: QueryBuilder::new(engine.clone()),
            aggregation_merger: AggregationMerger::new(),
        }
    }

    /// 执行 SQL 查询
    ///
    /// 根据查询类型分为两个分支：
    /// 1. 聚合查询 → execute_aggregation_query
    /// 2. 普通查询 → execute_query
    pub async fn execute_sql(&self, sql: &str) -> CoreResult<QueryResult> {
        log::info!(
            "📥 [DistributedExecutor::execute_sql] Received SQL: {}",
            sql
        );

        // 🔧 标准化SQL：验证语法并转换MySQL特有语法
        let (statement, normalized_sql) = SqlNormalizer::normalize(sql)?;

        // 如果SQL被转换了，记录日志
        if normalized_sql != sql {
            log::info!(
                "🔄 [DistributedExecutor] SQL normalized: {} -> {}",
                sql,
                normalized_sql
            );
        }

        let plan = analyze_query(statement);

        if let Some(ref p) = &plan {
            log::info!(
                "🔍 [DistributedExecutor::execute_sql] Query type: {:?}",
                p.query_type
            );
        }

        // 分支 1：聚合查询（需要特殊合并）
        if let Some(ref p) = plan {
            if matches!(p.query_type, QueryType::Aggregation) {
                log::info!("📊 [DistributedExecutor::execute_sql] Executing aggregation query");
                return self.execute_aggregation_query(&normalized_sql).await;
            }
        }

        // 分支 2：普通查询
        // 提取需要的参数：sort_fields 和 sort_limit_info
        let (sort_fields, sort_limit_info) = if let Some(p) = plan {
            match p.query_type {
                QueryType::SortLimit(info) => (Some(info.sort_fields.clone()), Some(info)),
                _ => (None, None),
            }
        } else {
            // 即使没有识别为 SortLimit,也尝试提取 ORDER BY 字段
            (self.extract_order_by_fields(&normalized_sql), None)
        };

        self.execute_query(&normalized_sql, sort_fields, sort_limit_info)
            .await
    }

    /// 从 SQL 中提取 ORDER BY 字段 (简单解析,用于 hints 下发)
    fn extract_order_by_fields(&self, sql: &str) -> Option<Vec<(String, bool)>> {
        let sql_lower = sql.to_lowercase();

        // 查找 ORDER BY 子句
        if let Some(order_by_start) = sql_lower.find("order by") {
            // 🔧 修复：在小写版本中切片，避免字节索引错误
            let after_order_by = &sql_lower[order_by_start + 8..]; // "order by".len() == 8

            // 找到下一个 SQL 关键字或结束
            let order_clause = after_order_by
                .split_whitespace()
                .take_while(|w| !w.starts_with("limit") && !w.starts_with("offset"))
                .collect::<Vec<_>>()
                .join(" ");

            // 解析字段和方向
            let mut fields = Vec::new();
            for part in order_clause.split(',') {
                let tokens: Vec<&str> = part.trim().split_whitespace().collect();
                if !tokens.is_empty() {
                    let field_name = tokens[0].to_lowercase(); // 🔧 统一转小写
                    let ascending = tokens
                        .get(1)
                        .map(|s| *s != "desc") // 🔧 已经是小写，直接比较
                        .unwrap_or(true);
                    fields.push((field_name, ascending));
                }
            }

            if !fields.is_empty() {
                return Some(fields);
            }
        }

        None
    }

    /// 从 SQL 中移除 OFFSET，替换为新的 LIMIT
    /// 用于分区执行：让每个分区返回足够的数据 (offset + limit)
    fn remove_offset_from_sql(&self, sql: &str, new_limit: usize) -> String {
        use datafusion::sql::sqlparser::ast::Statement;
        use datafusion::sql::sqlparser::dialect::MySqlDialect;
        use datafusion::sql::sqlparser::parser::Parser;

        let dialect = MySqlDialect {};
        let mut statements = match Parser::parse_sql(&dialect, sql) {
            Ok(stmts) => stmts,
            Err(_) => return sql.to_string(), // 解析失败，返回原 SQL
        };

        if statements.is_empty() {
            return sql.to_string();
        }

        let statement = &mut statements[0];
        if let Statement::Query(query) = statement {
            // 简单方法：基于原始 SQL 文本替换
            // 由于 AST 构造比较复杂，我们直接做字符串替换
            let sql_lower = sql.to_lowercase();

            // 查找 LIMIT 子句位置
            if let Some(limit_pos) = sql_lower.find("limit") {
                // 提取 LIMIT 之前的部分
                let before_limit = &sql[..limit_pos];
                // 查找 ORDER BY（如果有）
                let mut new_sql = String::from(before_limit);

                // 添加新的 LIMIT (不带 OFFSET)
                new_sql.push_str(&format!("LIMIT {}", new_limit));

                return new_sql;
            }
        }

        sql.to_string()
    }

    /// 确保排序字段在SELECT列表中
    /// 如果用户查询 SELECT a, b ORDER BY c，我们需要改写为 SELECT a, b, c ORDER BY c
    /// 这样TopKMerger才能正确排序
    fn ensure_sort_fields_in_select(
        &self,
        sql: &str,
        sort_fields: &[(String, bool)],
    ) -> CoreResult<String> {
        use datafusion::sql::sqlparser::ast::Statement;
        use datafusion::sql::sqlparser::dialect::MySqlDialect;
        use datafusion::sql::sqlparser::parser::Parser;

        // 解析SQL
        let dialect = MySqlDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| CoreError::InvalidParam(format!("Failed to parse SQL: {}", e)))?;

        if statements.is_empty() {
            return Ok(sql.to_string());
        }

        if let Statement::Query(query) = &statements[0] {
            // 检查是否是SELECT *
            if let datafusion::sql::sqlparser::ast::SetExpr::Select(select) = &*query.body {
                // 如果是SELECT *，不需要修改
                if select
                    .projection
                    .iter()
                    .any(|p| matches!(p, datafusion::sql::sqlparser::ast::SelectItem::Wildcard(_)))
                {
                    return Ok(sql.to_string());
                }

                // 检查排序字段是否都在SELECT列表中
                let selected_fields: Vec<String> = select
                    .projection
                    .iter()
                    .filter_map(|item| {
                        if let datafusion::sql::sqlparser::ast::SelectItem::UnnamedExpr(expr) = item
                        {
                            if let datafusion::sql::sqlparser::ast::Expr::Identifier(ident) = expr {
                                return Some(ident.value.to_lowercase());
                            }
                        }
                        None
                    })
                    .collect();

                let mut missing_fields = Vec::new();
                for (field_name, _) in sort_fields {
                    let field_lower = field_name.to_lowercase();
                    if !selected_fields.contains(&field_lower) {
                        missing_fields.push(field_name.clone());
                    }
                }

                // 如果有缺失的排序字段，添加到SELECT列表
                if !missing_fields.is_empty() {
                    // 使用字符串拼接方式添加字段
                    return self.add_fields_to_select_string(sql, &missing_fields);
                }
            }
        }

        Ok(sql.to_string())
    }

    /// 字符串方式添加字段到SELECT列表
    fn add_fields_to_select_string(&self, sql: &str, fields: &[String]) -> CoreResult<String> {
        let sql_upper = sql.to_uppercase();

        // 找到SELECT和FROM之间的位置
        if let Some(select_pos) = sql_upper.find("SELECT") {
            if let Some(from_pos) = sql_upper.find("FROM") {
                let select_end = select_pos + 6; // "SELECT".len()
                let projection = &sql[select_end..from_pos].trim();
                let rest = &sql[from_pos..];

                // 添加缺失字段
                let additional_fields = fields.join(", ");
                let new_sql = format!("SELECT {}, {} {}", projection, additional_fields, rest);

                log::info!(
                    "🔧 [ensure_sort_fields_in_select] Added missing sort fields: {:?}",
                    fields
                );
                log::info!("🔧 [ensure_sort_fields_in_select] Original SQL: {}", sql);
                log::info!(
                    "🔧 [ensure_sort_fields_in_select] Modified SQL: {}",
                    new_sql
                );

                return Ok(new_sql);
            }
        }

        Ok(sql.to_string())
    }

    /// 执行普通查询（非聚合）
    ///
    /// # 参数
    /// * `sql` - SQL 查询语句
    /// * `sort_fields` - ORDER BY 字段（用于下发给 SegmentScanner）
    /// * `sort_limit_info` - 完整的 sort + limit 信息（用于最终合并）
    async fn execute_query(
        &self,
        sql: &str,
        sort_fields: Option<Vec<(String, bool)>>,
        sort_limit_info: Option<crate::compute::optimizer::SortLimitInfo>,
    ) -> CoreResult<QueryResult> {
        let table_name = self.query_builder.extract_table_name(sql)?;
        let meta = self.engine.get_table_meta(&table_name)?;

        // 根据分区策略获取分区列表
        let partition_ids = match &meta.partition_strategy {
            crate::catalog::PartitionStrategy::Custom => {
                // Custom 分区：动态查询实际存在的分区
                self.engine.list_partitions(&table_name).await
            }
            _ => {
                // 其他分区策略：使用 parallel_workers 生成分区 ID
                (0..meta.parallel_workers)
                    .map(|idx| {
                        meta.partition_strategy
                            .generate_partition_id(&table_name, idx, None)
                    })
                    .collect()
            }
        };

        // 🔧 提取 limit hint 和生成分区 SQL
        let (limit_hint, partition_sql) = if let Some(info) = &sort_limit_info {
            // 如果有 OFFSET，分区需要返回 offset + limit 条数据
            let partition_limit = info.offset.unwrap_or(0) + info.limit;
            let mut sql_for_partition = self.remove_offset_from_sql(sql, partition_limit);

            // 🔧 确保排序字段在SELECT列表中
            sql_for_partition =
                self.ensure_sort_fields_in_select(&sql_for_partition, &info.sort_fields)?;

            (Some(partition_limit), sql_for_partition)
        } else {
            (None, sql.to_string())
        };

        log::info!(
            "🔍 [execute_query] table='{}', partitions={}, has_sort={}, limit_hint={:?}, partition_sql='{}'",
            table_name,
            partition_ids.len(),
            sort_fields.is_some(),
            limit_hint,
            partition_sql
        );

        // 并行查询所有 partition
        let mut all_batches = Vec::new();

        for partition_id in &partition_ids {
            match self
                .execute_on_partition(
                    &table_name,
                    partition_id,
                    &partition_sql,
                    sort_fields.clone(),
                    limit_hint,
                )
                .await
            {
                Ok(batches) => {
                    all_batches.extend(batches);
                }
                Err(e) => {
                    log::warn!("⚠️  Partition {} failed: {}", partition_id, e);
                }
            }
        }

        let total_rows_before: usize = all_batches.iter().map(|b| b.num_rows()).sum();
        log::info!(
            "📊 [execute_query] Collected {} batches with {} total rows from {} partitions",
            all_batches.len(),
            total_rows_before,
            partition_ids.len()
        );

        // matched_docs 就是返回的总行数（在 LIMIT 之前）
        let matched_docs = total_rows_before;

        // 如果有 ORDER BY，做最终排序
        let final_batch = if all_batches.is_empty() {
            // 无论是否有排序，都需要处理空结果
            self.create_empty_batch_from_sql(sql, &table_name).await?
        } else if let Some(info) = sort_limit_info {
            self.apply_final_sort_limit(all_batches, &info)?
        } else {
            // 没有排序，合并所有 batches
            self.concat_batches(all_batches)?
        };

        Ok(QueryResult {
            batch: final_batch,
            matched_docs,
        })
    }

    /// 在单个 partition 上执行查询
    async fn execute_on_partition(
        &self,
        table_name: &str,
        partition_id: &str,
        sql: &str,
        sort_hints: Option<Vec<(String, bool)>>,
        limit_hint: Option<usize>,
    ) -> CoreResult<Vec<RecordBatch>> {
        log::info!(
            "🔧 [execute_on_partition] partition_id={}, table='{}', sql='{}', sort_hints={:?}, limit_hint={:?}",
            partition_id,
            table_name,
            sql,
            sort_hints,
            limit_hint
        );

        let ctx = SessionContext::new();

        let partition = self
            .engine
            .get_partition(table_name, partition_id)
            .await
            .ok_or_else(|| {
                CoreError::NotExisted(format!(
                    "Partition {} not found for table '{}'",
                    partition_id, table_name
                ))
            })?;

        // 🔧 使用带 hints 的 Provider（sort 和 limit 都是 Option，自动传递）
        let provider = Arc::new(
            crate::compute::PartitionTableProviderWithHints::new_with_hints(
                partition, sort_hints, limit_hint,
            ),
        );

        ctx.register_table(table_name, provider)
            .map_err(|e| CoreError::Internal(format!("Failed to register table: {}", e)))?;

        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| CoreError::InvalidParam(format!("Query parse error: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| CoreError::Internal(format!("Query execution error: {}", e)))?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        log::info!(
            "✅ [execute_on_partition] partition_id={}, returned {} batches with {} total rows",
            partition_id,
            batches.len(),
            total_rows
        );

        Ok(batches)
    }
    /// 应用最终的排序和 LIMIT（在协调节点）
    /// 使用 DataFusion API 实现
    fn apply_final_sort_limit(
        &self,
        batches: Vec<RecordBatch>,
        info: &crate::compute::optimizer::SortLimitInfo,
    ) -> CoreResult<RecordBatch> {
        if batches.is_empty() {
            // 创建空的RecordBatch，使用第一个batch的schema（如果有的话）
            // 由于这个方法被调用时batches为空，我们需要另一种方式获取schema
            // 暂时返回错误，让调用方处理
            return Err(CoreError::Internal(
                "No data to process in apply_final_sort_limit".to_string(),
            ));
        }

        // 如果没有排序字段，这是纯 LIMIT 查询，直接应用 limit
        if info.sort_fields.is_empty() {
            log::info!(
                "🔍 [apply_final_sort_limit] Pure LIMIT query, applying limit={}, offset={:?} to {} batches",
                info.limit,
                info.offset,
                batches.len()
            );

            let offset = info.offset.unwrap_or(0);
            let limit = info.limit;

            // 保存第一个 batch 的 schema 用于创建空 RecordBatch
            let first_batch_schema = batches.first().map(|b| b.schema());

            // 跳过 offset 行，取 limit 行
            let mut result_batches = Vec::new();
            let mut rows_skipped = 0;
            let mut rows_collected = 0;

            for batch in batches {
                let batch_rows = batch.num_rows();

                // 跳过 offset 行
                if rows_skipped < offset {
                    let skip = (offset - rows_skipped).min(batch_rows);
                    rows_skipped += skip;

                    if skip >= batch_rows {
                        // 整个 batch 都被跳过
                        continue;
                    }

                    // 部分跳过，切片保留剩余部分
                    let remaining = batch.slice(skip, batch_rows - skip);
                    let take = (limit - rows_collected).min(remaining.num_rows());
                    let sliced = remaining.slice(0, take);
                    rows_collected += take;
                    result_batches.push(sliced);
                } else if rows_collected < limit {
                    // 收集数据
                    let take = (limit - rows_collected).min(batch_rows);
                    let sliced = batch.slice(0, take);
                    rows_collected += take;
                    result_batches.push(sliced);
                }

                // 已经收集够了
                if rows_collected >= limit {
                    break;
                }
            }

            log::info!(
                "✅ [apply_final_sort_limit] Pure LIMIT result: {} rows",
                rows_collected
            );

            return if result_batches.is_empty() {
                // 创建空的 RecordBatch，使用保存的 schema
                if let Some(schema) = first_batch_schema {
                    self.create_empty_batch_with_schema(schema)
                } else {
                    Err(CoreError::Internal("No data to process".to_string()))
                }
            } else {
                self.concat_batches(result_batches)
            };
        }

        log::info!(
            "🔄 [apply_final_sort_limit] Sorting {} batches by {:?}, limit={}, offset={:?}",
            batches.len(),
            info.sort_fields,
            info.limit,
            info.offset
        );

        // 方案：使用 TopKMerger（已经实现好的）
        // 未来可以考虑使用 DataFusion 的 sort + limit API
        use crate::compute::TopKMerger;

        let merger = TopKMerger::new(info.sort_fields.clone(), info.limit, info.offset);

        // 将所有 batches 作为一个 partition 的结果
        let result = merger
            .merge(vec![batches])
            .map_err(|e| CoreError::Internal(format!("Failed to merge results: {}", e)))?;

        log::info!(
            "✅ [apply_final_sort_limit] Sorted and limited to {} rows",
            result.num_rows()
        );

        Ok(result)
    }

    /// 合并多个 RecordBatch 为一个
    fn concat_batches(&self, batches: Vec<RecordBatch>) -> CoreResult<RecordBatch> {
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

    /// 在单个分区上执行 SQL 查询（旧实现，保留用于聚合查询）
    async fn execute_sql_on_partition_old(
        &self,
        table_name: &str,
        partition_id: &str,
        sql: &str,
    ) -> CoreResult<Vec<RecordBatch>> {
        eprintln!(
            "🔍 [execute_sql_on_partition] Starting query on partition {} for table '{}'",
            partition_id, table_name
        );
        eprintln!("🔍 [execute_sql_on_partition] SQL: {}", sql);

        let ctx = SessionContext::new();

        // 获取分区
        let partition = self
            .engine
            .get_partition(table_name, partition_id)
            .await
            .ok_or_else(|| {
                crate::utils::error::CoreError::NotExisted(format!(
                    "Partition {} not found for table '{}'",
                    partition_id, table_name
                ))
            })?;

        eprintln!(
            "🔍 [execute_sql_on_partition] Got partition {}, registering table...",
            partition_id
        );

        // 注册分区到 DataFusion 的默认 catalog 和 schema
        let provider = Arc::new(PartitionTableProvider::new(partition));

        // 使用 catalog API 注册表，这样可以在 datafusion.public.{table_name} 下找到
        if let Err(e) = ctx.register_table(table_name, provider) {
            eprintln!(
                "❌ [execute_sql_on_partition] Failed to register table: {}",
                e
            );
            return Err(crate::utils::error::CoreError::Internal(format!(
                "Failed to register table: {}",
                e
            )));
        }

        eprintln!(
            "🔍 [execute_sql_on_partition] Table '{}' registered, executing SQL...",
            table_name
        );

        // 验证表是否真的注册成功
        let catalog = ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let registered_tables: Vec<String> = schema.table_names();
        eprintln!(
            "🔍 [execute_sql_on_partition] Registered tables in datafusion.public: {:?}",
            registered_tables
        );

        // 执行查询
        let df = ctx.sql(sql).await.map_err(|e| {
            eprintln!("❌ [execute_sql_on_partition] SQL parse error: {}", e);
            eprintln!(
                "❌ [execute_sql_on_partition] Available tables: {:?}",
                registered_tables
            );
            crate::utils::error::CoreError::InvalidParam(format!("Query parse error: {}", e))
        })?;

        eprintln!("🔍 [execute_sql_on_partition] SQL parsed successfully, collecting results...");

        let batches = df.collect().await.map_err(|e| {
            eprintln!("❌ [execute_sql_on_partition] Query execution error: {}", e);
            crate::utils::error::CoreError::Internal(format!("Query execution error: {}", e))
        })?;

        eprintln!(
            "🔍 [execute_sql_on_partition] Partition {} returned {} batches",
            partition_id,
            batches.len()
        );

        Ok(batches)
    }

    /// 执行分布式聚合查询
    ///
    /// 支持的聚合函数: COUNT, SUM, AVG, MAX, MIN
    /// 支持 GROUP BY 子句
    async fn execute_aggregation_query(&self, sql: &str) -> CoreResult<QueryResult> {
        // 提取表名
        let table_name = self.query_builder.extract_table_name(sql)?;

        let meta = self.engine.get_table_meta(&table_name)?;

        // 根据分区策略获取分区列表
        let partition_ids = match &meta.partition_strategy {
            crate::catalog::PartitionStrategy::Custom => {
                // Custom 分区：动态查询实际存在的分区
                self.engine.list_partitions(&table_name).await
            }
            _ => {
                // 其他分区策略：使用 parallel_workers 生成分区 ID
                (0..meta.parallel_workers)
                    .map(|idx| {
                        meta.partition_strategy
                            .generate_partition_id(&table_name, idx, None)
                    })
                    .collect()
            }
        };

        eprintln!(
            "🔍 [DistributedExecutor] Executing distributed aggregation on table '{}' with {} partitions",
            table_name, partition_ids.len()
        );

        // 并行在所有分区上执行聚合
        let mut partition_results = Vec::new();

        for partition_id in &partition_ids {
            match self
                .execute_sql_on_partition_old(&table_name, partition_id, sql)
                .await
            {
                Ok(batches) => {
                    partition_results.push(batches);
                }
                Err(e) => {
                    eprintln!(
                        "⚠️  [DistributedExecutor] Failed to execute aggregation on partition {}: {}",
                        partition_id, e
                    );
                }
            }
        }

        if partition_results.is_empty() {
            return Err(crate::utils::error::CoreError::Internal(
                "No partition returned results".to_string(),
            ));
        }

        // 合并各分区的聚合结果
        let merged_batch = self
            .aggregation_merger
            .merge_aggregation_results(&partition_results, sql)?;

        let matched_docs = merged_batch.num_rows();

        Ok(QueryResult {
            batch: merged_batch,
            matched_docs,
        })
    }

    /// 通过执行SQL查询来创建空的RecordBatch（获取正确的schema）
    async fn create_empty_batch_from_sql(
        &self,
        sql: &str,
        table_name: &str,
    ) -> CoreResult<RecordBatch> {
        // 创建一个临时的SessionContext来执行查询并获取schema
        let ctx = SessionContext::new();

        // 获取表的元数据以生成 partition_id
        let meta = self.engine.get_table_meta(table_name)?;

        // 根据分区策略获取第一个分区 ID
        let partition_id = match &meta.partition_strategy {
            crate::catalog::PartitionStrategy::Custom => {
                // Custom 分区：查询实际存在的第一个分区
                let partitions = self.engine.list_partitions(table_name).await;
                partitions.into_iter().next().ok_or_else(|| {
                    CoreError::NotExisted(format!("No partitions found for table '{}'", table_name))
                })?
            }
            _ => {
                // 其他分区策略：使用索引 0 生成分区 ID
                meta.partition_strategy
                    .generate_partition_id(table_name, 0, None)
            }
        };

        // 获取第一个partition来注册表（只是为了获取schema）
        let partition = self
            .engine
            .get_partition(table_name, &partition_id)
            .await
            .ok_or_else(|| {
                CoreError::NotExisted(format!(
                    "Partition {} not found for table '{}'",
                    partition_id, table_name
                ))
            })?;

        let provider = Arc::new(crate::compute::PartitionTableProvider::new(partition));
        ctx.register_table(table_name, provider).map_err(|e| {
            CoreError::Internal(format!("Failed to register table '{}': {}", table_name, e))
        })?;

        // 执行查询但添加WHERE 1=0来确保没有结果，只获取schema
        let empty_sql = if sql.to_uppercase().contains("WHERE") {
            // 如果已经有WHERE子句，在现有条件前添加FALSE AND (...)
            let where_pos = sql.to_uppercase().find("WHERE").unwrap();
            let before_where = &sql[..where_pos];
            let after_where = &sql[where_pos + 5..]; // 跳过"WHERE"

            // 查找ORDER BY、GROUP BY、HAVING、LIMIT等子句的位置
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
                // 如果有其他子句，只包装WHERE条件部分
                let where_condition = &after_where[..pos].trim();
                let rest = &after_where[pos..];
                format!(
                    "{}WHERE 1=0 AND ({}) {}",
                    before_where, where_condition, rest
                )
            } else {
                // 如果没有其他子句，包装整个WHERE条件
                format!("{}WHERE 1=0 AND ({})", before_where, after_where.trim())
            }
        } else {
            // 如果没有WHERE子句，简单添加WHERE 1=0
            sql.replace(
                &format!("FROM {}", table_name),
                &format!("FROM {} WHERE 1=0", table_name),
            )
        };

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

        // 如果有结果（不应该，因为WHERE 1=0），取第一个
        if let Some(batch) = batches.first() {
            return Ok(batch.clone());
        }

        // 否则创建一个真正空的batch
        self.create_empty_batch_with_schema(schema)
    }

    /// 使用给定schema创建空的RecordBatch
    fn create_empty_batch_with_schema(
        &self,
        schema: Arc<datafusion::arrow::datatypes::Schema>,
    ) -> CoreResult<RecordBatch> {
        use datafusion::arrow::array::new_empty_array;

        let empty_columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();

        RecordBatch::try_new(schema, empty_columns)
            .map_err(|e| CoreError::Internal(format!("Failed to create empty RecordBatch: {}", e)))
    }
}
