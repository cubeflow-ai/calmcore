use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;

use crate::compute::optimizer::{analyze_query, ExecutionHints, QueryType};
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

    /// 执行 SQL 查询 - 扁平化路由
    ///
    /// 根据 QueryType 直接路由到对应的执行函数，无嵌套判断
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

        // 分析查询，获取执行计划
        let plan = analyze_query(statement);

        // 根据 QueryType 路由到对应的执行函数
        match plan {
            Some(plan) => {
                log::info!("🔍 [DistributedExecutor] Query type: {:?}", plan.query_type);

                match plan.query_type {
                    // ===== 聚合查询路径 =====
                    QueryType::CountOnly(info) => {
                        log::info!(
                            "⚡ [Fast Path] COUNT(*) {} WHERE",
                            if info.has_where { "with" } else { "without" }
                        );
                        self.execute_count_only(&plan.table_name, &normalized_sql, &info)
                            .await
                    }
                    QueryType::CountWithSingleGroupBy(info) => {
                        log::info!(
                            "⚡ [Fast Path] COUNT(*) with single GROUP BY: {}",
                            info.group_by_field
                        );
                        self.execute_count_with_single_group_by(&plan.table_name, &info)
                            .await
                    }
                    QueryType::GeneralAggregation(_info) => {
                        log::info!("📊 [General Path] Aggregation query");
                        self.execute_general_aggregation(&normalized_sql).await
                    }

                    // ===== 非聚合查询路径 =====
                    QueryType::NaturalOrder(info) => {
                        log::info!(
                            "🌿 [Natural Order] ORDER BY _nature - metadata-driven deep pagination"
                        );
                        self.execute_natural_order(&normalized_sql, &plan.table_name, info)
                            .await
                    }
                    QueryType::ParallelSortLimit(info) => {
                        log::info!("🔀 [Parallel] ORDER BY + LIMIT");
                        self.execute_parallel_sort_limit(&normalized_sql, &plan.table_name, info)
                            .await
                    }
                    QueryType::SerialLimit(info) => {
                        log::info!("➡️  [Serial] Pure LIMIT with early termination");
                        self.execute_serial_limit(&normalized_sql, &plan.table_name, info)
                            .await
                    }
                    QueryType::ParallelSortStreaming(info) => {
                        log::info!("🌊 [Streaming] ORDER BY without LIMIT");
                        self.execute_parallel_sort_streaming(
                            &normalized_sql,
                            &plan.table_name,
                            info,
                        )
                        .await
                    }
                    QueryType::SerialFullScan => {
                        log::info!("📄 [Serial] Full scan in natural order");
                        self.execute_serial_full_scan(
                            &normalized_sql,
                            &plan.table_name,
                            &plan.execution_hints,
                        )
                        .await
                    }
                }
            }
            None => {
                // 无法识别的查询，使用通用 DataFusion 执行
                log::warn!("⚠️  Unrecognized query pattern, using DataFusion fallback");
                self.execute_with_datafusion_fallback(&normalized_sql).await
            }
        }
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
        if let Statement::Query(_query) = statement {
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

        let partition_names = self.engine.list_partitions(&table_name).await;

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
            partition_names.len(),
            sort_fields.is_some(),
            limit_hint,
            partition_sql
        );

        // 🚀 并行查询所有 partition
        let mut all_batches = Vec::new();

        let futures: Vec<_> = partition_names
            .iter()
            .map(|partition_name| {
                let table_name = table_name.clone();
                let partition_name = partition_name.clone();
                let partition_sql = partition_sql.clone();
                let sort_fields = sort_fields.clone();
                async move {
                    self.execute_on_partition(
                        &table_name,
                        &partition_name,
                        &partition_sql,
                        sort_fields,
                        limit_hint,
                    )
                    .await
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(batches) => {
                    all_batches.extend(batches);
                }
                Err(e) => {
                    log::warn!("⚠️  Partition {} failed: {}", partition_names[idx], e);
                }
            }
        }

        let total_rows_before: usize = all_batches.iter().map(|b| b.num_rows()).sum();
        log::info!(
            "📊 [execute_query] Collected {} batches with {} total rows from {} partitions",
            all_batches.len(),
            total_rows_before,
            partition_names.len()
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
        partition_name: &str,
        sql: &str,
        sort_hints: Option<Vec<(String, bool)>>,
        limit_hint: Option<usize>,
    ) -> CoreResult<Vec<RecordBatch>> {
        log::info!(
            "🔧 [execute_on_partition] partition_name={}, table='{}', sql='{}', sort_hints={:?}, limit_hint={:?}",
            partition_name,
            table_name,
            sql,
            sort_hints,
            limit_hint
        );

        let ctx = SessionContext::new();

        let partition = self
            .engine
            .get_partition(table_name, partition_name)
            .await
            .ok_or_else(|| {
                CoreError::NotExisted(format!(
                    "Partition {} not found for table '{}'",
                    partition_name, table_name
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
            "✅ [execute_on_partition] partition_name={}, returned {} batches with {} total rows",
            partition_name,
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
        partition_name: &str,
        sql: &str,
    ) -> CoreResult<Vec<RecordBatch>> {
        eprintln!(
            "🔍 [execute_sql_on_partition] Starting query on partition {} for table '{}'",
            partition_name, table_name
        );
        eprintln!("🔍 [execute_sql_on_partition] SQL: {}", sql);

        let ctx = SessionContext::new();

        // 获取分区
        let partition = self
            .engine
            .get_partition(table_name, partition_name)
            .await
            .ok_or_else(|| {
                crate::utils::error::CoreError::NotExisted(format!(
                    "Partition {} not found for table '{}'",
                    partition_name, table_name
                ))
            })?;

        eprintln!(
            "🔍 [execute_sql_on_partition] Got partition {}, registering table...",
            partition_name
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
            partition_name,
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

        // 根据分区策略获取分区列表
        let partition_names = self.engine.list_partitions(&table_name).await;

        eprintln!(
            "🔍 [DistributedExecutor] Executing distributed aggregation on table '{}' with {} partitions",
            table_name, partition_names.len()
        );

        // 🚀 并行在所有分区上执行聚合
        let mut partition_results = Vec::new();

        let futures: Vec<_> = partition_names
            .iter()
            .map(|partition_name| {
                let table_name = table_name.clone();
                let partition_name = partition_name.clone();
                let sql = sql.to_string();
                async move {
                    self.execute_sql_on_partition_old(&table_name, &partition_name, &sql)
                        .await
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(batches) => {
                    partition_results.push(batches);
                }
                Err(e) => {
                    eprintln!(
                        "⚠️  [DistributedExecutor] Failed to execute aggregation on partition {}: {}",
                        partition_names[idx], e
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

        // 直接从 engine 获取第一个 partition
        let partition_names = self.engine.list_partitions(table_name).await;
        let first_partition_name = partition_names.into_iter().next().ok_or_else(|| {
            CoreError::NotExisted(format!("No partitions found for table '{}'", table_name))
        })?;

        // 获取第一个partition来注册表（只是为了获取schema）
        let partition = self
            .engine
            .get_partition(table_name, &first_partition_name)
            .await
            .ok_or_else(|| {
                CoreError::NotExisted(format!(
                    "Partition {} not found for table '{}'",
                    first_partition_name, table_name
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

    // ===== 聚合查询执行方法 =====

    /// 执行 COUNT(*) 查询（无 GROUP BY）
    ///
    /// 快速路径优化：
    /// - 无 WHERE：直接统计所有分区的总行数（最快）
    /// - 有 WHERE：计算 filter bitmap 的 cardinality（极快，无需读取数据）
    async fn execute_count_only(
        &self,
        table_name: &str,
        sql: &str,
        info: &crate::compute::optimizer::CountOnlyInfo,
    ) -> CoreResult<QueryResult> {
        let total_count = if info.has_where {
            // ⚡ 快速路径：COUNT(*) + WHERE
            // 只计算 bitmap cardinality，不读取任何行数据
            log::info!("🎯 [COUNT Optimization] Using bitmap cardinality (no data read)");
            self.execute_count_with_filter(table_name, sql).await?
        } else {
            // ⚡ 最快路径：COUNT(*) 无 WHERE
            // 直接返回总行数
            log::info!("🎯 [COUNT Optimization] Using total count (instant)");
            self.get_total_count(table_name).await?
        };

        // 构造返回结果
        use datafusion::arrow::array::UInt64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let array = Arc::new(UInt64Array::from(vec![total_count])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array])
            .map_err(|e| CoreError::Internal(format!("Failed to create count batch: {}", e)))?;

        Ok(QueryResult {
            batch,
            matched_docs: total_count as usize,
        })
    }

    /// 获取表的总行数（无 WHERE）
    async fn get_total_count(&self, table_name: &str) -> CoreResult<u64> {
        // 直接从 engine 获取所有 partition 列表
        let partition_names = self.engine.list_partitions(table_name).await;

        // 🚀 并行统计所有分区的行数
        let futures: Vec<_> = partition_names
            .iter()
            .map(|partition_name| {
                let table_name = table_name.to_string();
                let partition_name = partition_name.clone();
                async move {
                    if let Some(partition) = self
                        .engine
                        .get_partition(&table_name, &partition_name)
                        .await
                    {
                        partition.total_count()
                    } else {
                        0u64
                    }
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        let total_count: u64 = results.into_iter().sum();

        Ok(total_count)
    }

    /// 执行 COUNT(*) + WHERE：只计算 bitmap cardinality
    ///
    /// 🚀 极速优化：零数据读取，只计算 bitmap cardinality
    ///
    /// # 架构设计
    /// 1. 解析 SQL → 提取 filters (DataFusion LogicalPlan)
    /// 2. 遍历所有 Partition → Segment
    /// 3. 每个 Segment 计算 filter bitmap (利用索引)
    /// 4. 返回 bitmap.len() 之和 (不读取任何行数据)
    ///
    /// # 性能提升
    /// - 传统方式: 扫描 → 过滤 → 计数 (需读取所有匹配行)
    /// - 优化方式: 计算 bitmap → len() (只读索引,零行数据)
    /// - 提升倍数: 100-1000x (取决于数据量和命中率)
    async fn execute_count_with_filter(&self, table_name: &str, sql: &str) -> CoreResult<u64> {
        use datafusion::prelude::*;

        // Step 1: 解析 SQL,提取 filters
        let ctx = SessionContext::new();

        // 直接从 engine 获取所有 partition 列表
        let partition_names = self.engine.list_partitions(table_name).await;

        // 为了提取 filters,需要注册一个临时表
        // 使用第一个 partition 的 schema
        if let Some(first_partition) = self
            .engine
            .get_partition(table_name, &partition_names[0])
            .await
        {
            let provider = Arc::new(crate::compute::PartitionTableProvider::new(
                first_partition.clone(),
            ));
            ctx.register_table(table_name, provider)
                .map_err(|e| CoreError::Internal(format!("Failed to register table: {}", e)))?;
        } else {
            return Err(CoreError::Internal("No partitions found".to_string()));
        }

        // 解析 SQL 获取 LogicalPlan
        let logical_plan = ctx
            .sql(sql)
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to parse SQL: {}", e)))?
            .into_unoptimized_plan();

        // Step 2: 从 LogicalPlan 提取 filters
        let filters = self.extract_filters_from_plan(&logical_plan);

        log::info!(
            "🎯 [COUNT Bitmap] Extracted {} filters from SQL",
            filters.len()
        );

        // Step 3: 🚀 并行计算所有 partition 的 bitmap cardinality
        let futures: Vec<_> = partition_names
            .iter()
            .map(|partition_name| {
                let table_name = table_name.to_string();
                let partition_name = partition_name.clone();
                let filters = filters.clone();
                async move {
                    if let Some(partition) = self
                        .engine
                        .get_partition(&table_name, &partition_name)
                        .await
                    {
                        self.count_partition_with_filters(&partition, &filters)
                    } else {
                        Ok(0u64)
                    }
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        let total_matched: u64 = results.into_iter().filter_map(|r| r.ok()).sum();

        log::info!(
            "✅ [COUNT Bitmap] Total matched: {} (zero rows read)",
            total_matched
        );

        Ok(total_matched)
    }

    /// 从 LogicalPlan 提取 WHERE 条件 (filters)
    ///
    /// DataFusion 的 LogicalPlan 结构:
    /// - Aggregate(COUNT) → Projection → Filter(WHERE) → TableScan
    ///
    /// 我们需要找到 Filter 节点并提取表达式
    fn extract_filters_from_plan(
        &self,
        plan: &datafusion::logical_expr::LogicalPlan,
    ) -> Vec<datafusion::logical_expr::Expr> {
        use datafusion::logical_expr::LogicalPlan;

        let mut filters = Vec::new();

        // 递归遍历 LogicalPlan 树
        match plan {
            LogicalPlan::Filter(filter) => {
                // Filter.predicate 是字段,不是方法
                filters.push(filter.predicate.clone());
            }
            _ => {
                // 递归检查子节点
                for input in plan.inputs() {
                    filters.extend(self.extract_filters_from_plan(input));
                }
            }
        }

        filters
    }

    /// 计算单个 Partition 中匹配 filters 的文档数
    ///
    /// # 核心优化
    /// 遍历 Partition 的所有 Segments (current + frozen),
    /// 使用 SegmentScanner::create_plan 计算 bitmap,
    /// 但只返回 bitmap.len(),不实际读取行数据
    fn count_partition_with_filters(
        &self,
        partition: &Arc<crate::partition::Partition>,
        filters: &[datafusion::logical_expr::Expr],
    ) -> CoreResult<u64> {
        let mut total_count = 0u64;
        let schema = partition.arrow_schema.clone();

        // 处理 current segment
        {
            let current_segment = partition.get_current_segment();
            if current_segment.doc_count() > 0 {
                let count = self.count_segment_with_filters(&current_segment, filters, &schema)?;
                total_count += count;
            }
        }

        // 处理 frozen segments
        {
            let frozen_segments = partition.get_frozen_segments();
            for (_seg_id, segment) in frozen_segments.iter() {
                let count = self.count_segment_with_filters(segment, filters, &schema)?;
                total_count += count;
            }
        }

        Ok(total_count)
    }

    /// 计算单个 Segment 中匹配 filters 的文档数
    ///
    /// 🚀 核心优化: 直接调用 SegmentScanner::count_matches
    ///
    /// # 工作流程
    /// 1. 创建 SegmentScanner (不会触发数据读取)
    /// 2. 调用 count_matches(filters) → 计算 bitmap cardinality
    /// 3. 返回 bitmap.len() (零行数据读取)
    ///
    /// # 性能
    /// - 只使用索引计算 bitmap
    /// - 不读取任何行数据
    /// - 比传统 COUNT 快 100-1000 倍
    fn count_segment_with_filters(
        &self,
        segment: &crate::segment::Segment,
        filters: &[datafusion::logical_expr::Expr],
        schema: &Arc<datafusion::arrow::datatypes::Schema>,
    ) -> CoreResult<u64> {
        use crate::compute::segment_scanner::SegmentScanner;

        // 获取 segment 数据 (都是 Arc/轻量级操作)
        let index_readers = segment.get_index_readers();
        let doc_count = segment.doc_count();
        let deleted = segment.get_deleted();
        let row_data = segment.get_row_data();

        // 创建 SegmentScanner (不会触发任何 I/O)
        let scanner =
            SegmentScanner::new(schema.clone(), row_data, index_readers, doc_count, deleted);

        // ⚡ 核心优化: 使用 count_matches 直接返回 bitmap cardinality
        // 不会读取任何行数据,只使用索引计算 bitmap
        let count = scanner.count_matches(filters);

        Ok(count)
    }

    /// 执行 COUNT(*) + 单字段 GROUP BY
    /// 快速路径：使用倒排索引的 bitmap 计数
    async fn execute_count_with_single_group_by(
        &self,
        table_name: &str,
        info: &crate::compute::optimizer::CountGroupByInfo,
    ) -> CoreResult<QueryResult> {
        // TODO: 实现基于倒排索引的快速 GROUP BY COUNT
        // 目前先回退到通用聚合
        log::warn!("⚠️  COUNT with GROUP BY fast path not yet implemented, falling back to general aggregation");

        let sql = format!(
            "SELECT {}, COUNT(*) FROM {} GROUP BY {}",
            info.group_by_field, table_name, info.group_by_field
        );
        self.execute_general_aggregation(&sql).await
    }

    /// 执行通用聚合查询
    /// 使用 DataFusion 的聚合能力 + 协调节点合并
    async fn execute_general_aggregation(&self, sql: &str) -> CoreResult<QueryResult> {
        // 重命名原来的 execute_aggregation_query
        self.execute_aggregation_query(sql).await
    }

    // ===== 非聚合查询执行方法 =====

    /// 执行并行排序 + LIMIT（ORDER BY + LIMIT）
    async fn execute_parallel_sort_limit(
        &self,
        sql: &str,
        _table_name: &str,
        info: crate::compute::optimizer::SortLimitInfo,
    ) -> CoreResult<QueryResult> {
        // 重用现有的 execute_query 逻辑
        self.execute_query(sql, Some(info.sort_fields.clone()), Some(info))
            .await
    }

    /// 执行串行 LIMIT（只有 LIMIT，无 ORDER BY）
    /// 优化：串行遍历 segment，达到 limit 就停止
    async fn execute_serial_limit(
        &self,
        sql: &str,
        table_name: &str,
        info: crate::compute::optimizer::PureLimitInfo,
    ) -> CoreResult<QueryResult> {
        // 直接从 engine 获取所有 partition 列表
        let partition_names = self.engine.list_partitions(table_name).await;

        let mut all_batches = Vec::new();
        let mut collected_rows = 0;
        let target_rows = info.offset.unwrap_or(0) + info.limit;

        // 串行遍历分区，达到目标行数就停止
        for partition_name in &partition_names {
            if collected_rows >= target_rows {
                log::info!(
                    "✅ Early termination: collected {} >= target {}",
                    collected_rows,
                    target_rows
                );
                break;
            }

            match self
                .execute_on_partition(
                    table_name,
                    partition_name,
                    sql,
                    None,
                    Some(target_rows - collected_rows),
                )
                .await
            {
                Ok(batches) => {
                    for batch in batches {
                        collected_rows += batch.num_rows();
                        all_batches.push(batch);

                        if collected_rows >= target_rows {
                            break;
                        }
                    }
                }
                Err(e) => {
                    log::warn!("⚠️  Partition {} failed: {}", partition_name, e);
                }
            }
        }

        let matched_docs = collected_rows;

        // 应用 OFFSET（如果有）
        let final_batch = if all_batches.is_empty() {
            self.create_empty_batch_from_sql(sql, table_name).await?
        } else {
            let merged = self.concat_batches(all_batches)?;
            if let Some(offset) = info.offset {
                if offset > 0 && merged.num_rows() > offset {
                    merged.slice(offset, (merged.num_rows() - offset).min(info.limit))
                } else if offset >= merged.num_rows() {
                    // offset 超出范围，返回空结果
                    self.create_empty_batch_with_schema(merged.schema())?
                } else {
                    merged.slice(0, info.limit.min(merged.num_rows()))
                }
            } else {
                merged.slice(0, info.limit.min(merged.num_rows()))
            }
        };

        Ok(QueryResult {
            batch: final_batch,
            matched_docs,
        })
    }

    /// 执行自然顺序查询（ORDER BY _nature）
    /// 使用元数据驱动的深度分页，避免读取不必要的数据
    async fn execute_natural_order(
        &self,
        sql: &str,
        table_name: &str,
        info: crate::compute::optimizer::NaturalOrderInfo,
    ) -> CoreResult<QueryResult> {
        use crate::compute::executor::natural_order_executor::NaturalOrderExecutor;

        let executor = NaturalOrderExecutor::new(self.engine.clone());

        let offset = info.offset.unwrap_or(0);
        let limit = info.limit;

        log::info!(
            "🌿 [Natural Order] table={}, offset={}, limit={}, has_where={}, where_clause={:?}",
            table_name,
            offset,
            limit,
            info.has_where_filter,
            info.where_clause
        );

        let result = executor
            .execute_natural_order(sql, table_name, limit, offset, info.where_clause.as_deref())
            .await?;

        // 将 natural_order_executor 的 QueryResult 转换为 distributed 的 QueryResult
        Ok(QueryResult {
            batch: result.batch,
            matched_docs: result.matched_docs,
        })
    }
    /// 执行并行排序流式查询（ORDER BY 无 LIMIT）
    async fn execute_parallel_sort_streaming(
        &self,
        sql: &str,
        _table_name: &str,
        info: crate::compute::optimizer::SortStreamingInfo,
    ) -> CoreResult<QueryResult> {
        // 重用现有的 execute_query，但不传 limit
        self.execute_query(sql, Some(info.sort_fields), None).await
    }

    /// 执行串行全表扫描（无 ORDER BY，无 LIMIT）
    async fn execute_serial_full_scan(
        &self,
        sql: &str,
        table_name: &str,
        hints: &ExecutionHints,
    ) -> CoreResult<QueryResult> {
        // 🔧 从SQL中移除 _partition 和 _segment 条件（这些是虚拟字段，实际表中不存在）
        let cleaned_sql = self.remove_virtual_columns_from_sql(sql)?;

        // 🔧 提取 LIMIT/OFFSET 信息用于下推
        let (limit_hint, offset_value) = self.extract_limit_offset_from_sql(sql);

        log::info!("🔧 [execute_serial_full_scan] Original SQL: {}", sql);
        log::info!("🔧 [execute_serial_full_scan] Cleaned SQL: {}", cleaned_sql);
        log::info!(
            "🔧 [execute_serial_full_scan] LIMIT hint: {:?}, OFFSET: {:?}",
            limit_hint,
            offset_value
        );

        // 检查是否指定了 partition/segment
        if let Some(ref partition_name) = hints.target_partition {
            log::info!(
                "🎯 [Partition Filter] Querying specific partition: {}",
                partition_name
            );

            // 检查是否还指定了 segment
            if let Some(ref segment_name) = hints.target_segment {
                log::info!(
                    "🎯 [Segment Filter] Querying specific segment: {}",
                    segment_name
                );
                // TODO: 实现单个 segment 查询
                // 目前先查询整个 partition
            }

            // 🚀 计算需要从partition拉取的行数：OFFSET + LIMIT
            let fetch_limit = if let Some(limit) = limit_hint {
                Some(offset_value.unwrap_or(0) + limit)
            } else {
                None
            };

            // 只查询指定的 partition
            let batches = self
                .execute_on_partition(table_name, partition_name, &cleaned_sql, None, fetch_limit)
                .await?;

            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let matched_docs = total_rows;

            let final_batch = if batches.is_empty() {
                self.create_empty_batch_from_sql(sql, table_name).await?
            } else {
                // 合并所有batches
                let merged = self.concat_batches(batches)?;

                // 🔧 应用 OFFSET 和 LIMIT
                if let Some(offset) = offset_value {
                    if offset >= merged.num_rows() {
                        // offset超出范围，返回空结果
                        self.create_empty_batch_with_schema(merged.schema())?
                    } else if let Some(limit) = limit_hint {
                        // 有offset和limit
                        let remaining = merged.num_rows() - offset;
                        merged.slice(offset, remaining.min(limit))
                    } else {
                        // 只有offset
                        merged.slice(offset, merged.num_rows() - offset)
                    }
                } else if let Some(limit) = limit_hint {
                    // 只有limit
                    merged.slice(0, limit.min(merged.num_rows()))
                } else {
                    // 无offset和limit
                    merged
                }
            };

            return Ok(QueryResult {
                batch: final_batch,
                matched_docs,
            });
        }

        // 没有指定 partition,执行全表扫描
        log::info!("📄 [Full Scan] Scanning all partitions");
        let partition_names = self.engine.list_partitions(table_name).await;

        // 🚀 计算需要从每个partition拉取的行数
        let fetch_limit = if let Some(limit) = limit_hint {
            Some(offset_value.unwrap_or(0) + limit)
        } else {
            None
        };

        let mut all_batches = Vec::new();

        for partition_name in &partition_names {
            match self
                .execute_on_partition(table_name, partition_name, &cleaned_sql, None, fetch_limit)
                .await
            {
                Ok(batches) => all_batches.extend(batches),
                Err(e) => {
                    log::warn!("⚠️  Partition {} failed: {}", partition_name, e);
                }
            }
        }

        let matched_docs = all_batches.iter().map(|b| b.num_rows()).sum();

        let final_batch = if all_batches.is_empty() {
            self.create_empty_batch_from_sql(sql, table_name).await?
        } else {
            let merged = self.concat_batches(all_batches)?;

            // 🔧 应用 OFFSET 和 LIMIT
            if let Some(offset) = offset_value {
                if offset >= merged.num_rows() {
                    self.create_empty_batch_with_schema(merged.schema())?
                } else if let Some(limit) = limit_hint {
                    let remaining = merged.num_rows() - offset;
                    merged.slice(offset, remaining.min(limit))
                } else {
                    merged.slice(offset, merged.num_rows() - offset)
                }
            } else if let Some(limit) = limit_hint {
                merged.slice(0, limit.min(merged.num_rows()))
            } else {
                merged
            }
        };

        Ok(QueryResult {
            batch: final_batch,
            matched_docs,
        })
    }

    /// DataFusion 回退执行（无法识别的查询）
    async fn execute_with_datafusion_fallback(&self, sql: &str) -> CoreResult<QueryResult> {
        // 使用通用的 query builder 执行
        self.execute_query(sql, None, None).await
    }

    /// 从SQL中移除 _partition 和 _segment 虚拟字段
    /// 这些字段只用于路由，不是实际表字段
    fn remove_virtual_columns_from_sql(&self, sql: &str) -> CoreResult<String> {
        use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr, SetExpr, Statement, Value};
        use datafusion::sql::sqlparser::dialect::MySqlDialect;
        use datafusion::sql::sqlparser::parser::Parser;

        let dialect = MySqlDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| CoreError::InvalidParam(format!("Failed to parse SQL: {}", e)))?;

        if statements.is_empty() {
            return Ok(sql.to_string());
        }

        if let Statement::Query(ref mut query) = statements[0] {
            if let SetExpr::Select(ref mut select) = *query.body {
                if let Some(ref mut selection) = select.selection {
                    // 递归移除 _partition 和 _segment 条件
                    *selection = self.filter_virtual_columns_from_expr(selection.clone());

                    // 如果WHERE条件被完全移除（只剩下虚拟字段），移除整个WHERE子句
                    if Self::is_true_expr(selection) {
                        select.selection = None;
                    }
                }
            }
        }

        Ok(statements[0].to_string())
    }

    /// 递归过滤表达式中的虚拟字段条件
    fn filter_virtual_columns_from_expr(
        &self,
        expr: datafusion::sql::sqlparser::ast::Expr,
    ) -> datafusion::sql::sqlparser::ast::Expr {
        use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr, Value, ValueWithSpan};

        match expr {
            Expr::BinaryOp { left, op, right } => {
                // 检查是否是虚拟字段条件
                if matches!(op, BinaryOperator::Eq) {
                    if let Expr::Identifier(ref ident) = *left {
                        let field_name = ident.value.as_str();
                        if field_name == "_partition" || field_name == "_segment" {
                            // 返回 TRUE，这样在 AND 连接中会被忽略
                            return Expr::Value(ValueWithSpan {
                                value: Value::Boolean(true),
                                span: datafusion::sql::sqlparser::tokenizer::Span::empty(),
                            });
                        }
                    }
                }

                // 如果是 AND/OR，递归处理
                match op {
                    BinaryOperator::And => {
                        let new_left = self.filter_virtual_columns_from_expr(*left);
                        let new_right = self.filter_virtual_columns_from_expr(*right);

                        // 优化：如果一边是TRUE，返回另一边
                        if Self::is_true_expr(&new_left) {
                            return new_right;
                        }
                        if Self::is_true_expr(&new_right) {
                            return new_left;
                        }

                        Expr::BinaryOp {
                            left: Box::new(new_left),
                            op,
                            right: Box::new(new_right),
                        }
                    }
                    BinaryOperator::Or => {
                        let new_left = self.filter_virtual_columns_from_expr(*left);
                        let new_right = self.filter_virtual_columns_from_expr(*right);

                        Expr::BinaryOp {
                            left: Box::new(new_left),
                            op,
                            right: Box::new(new_right),
                        }
                    }
                    _ => Expr::BinaryOp { left, op, right },
                }
            }
            _ => expr,
        }
    }

    /// 检查表达式是否是 TRUE
    fn is_true_expr(expr: &datafusion::sql::sqlparser::ast::Expr) -> bool {
        use datafusion::sql::sqlparser::ast::{Expr, Value, ValueWithSpan};
        matches!(
            expr,
            Expr::Value(ValueWithSpan {
                value: Value::Boolean(true),
                ..
            })
        )
    }

    /// 从SQL中提取 LIMIT 和 OFFSET
    fn extract_limit_offset_from_sql(&self, sql: &str) -> (Option<usize>, Option<usize>) {
        // 使用简单的字符串解析，更可靠
        let sql_lower = sql.to_lowercase();

        let mut limit = None;
        let mut offset = None;

        // 查找 LIMIT 子句
        if let Some(limit_pos) = sql_lower.find("limit") {
            let after_limit = &sql[limit_pos + 5..].trim();

            // LIMIT N OFFSET M 或 LIMIT M, N (MySQL 风格)
            if let Some(comma_pos) = after_limit.find(',') {
                // LIMIT OFFSET, LIMIT 形式
                let offset_str = after_limit[..comma_pos].trim();
                let limit_str = after_limit[comma_pos + 1..]
                    .split_whitespace()
                    .next()
                    .unwrap_or("");

                offset = offset_str.parse().ok();
                limit = limit_str.parse().ok();
            } else {
                // LIMIT N [OFFSET M] 形式
                let parts: Vec<&str> = after_limit.split_whitespace().collect();
                if !parts.is_empty() {
                    limit = parts[0].parse().ok();
                }

                // 查找 OFFSET
                if let Some(offset_idx) = parts.iter().position(|&s| s == "offset") {
                    if offset_idx + 1 < parts.len() {
                        offset = parts[offset_idx + 1].parse().ok();
                    }
                }
            }
        }

        (limit, offset)
    }
}
