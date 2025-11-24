/// 查询计划分析器
///
/// 基于 DataFusion AST 的SQL分析器，生产可用
///
/// 特点：
/// - 使用 DataFusion 的 AST 解析，准确可靠
/// - 支持常见SQL模式：ORDER BY + LIMIT、纯LIMIT、聚合查询
/// - 适用于大多数OLAP查询场景
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast::{
    Expr, Offset, OrderByExpr, Query, SelectItem, SetExpr, TableFactor, Value,
};

/// 查询类型 - 扁平化的执行计划
#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    // ===== 聚合查询 =====
    /// COUNT(*) 且无 GROUP BY - 最快路径：直接返回总行数或 bitmap cardinality
    CountOnly(CountOnlyInfo),

    /// COUNT(*) 且 GROUP BY 单字段 - 快速路径：使用倒排索引 bitmap 计数
    CountWithSingleGroupBy(CountGroupByInfo),

    /// 通用聚合查询 - 正常 DataFusion 聚合流程
    GeneralAggregation(AggregationInfo),

    // ===== 非聚合查询 =====
    /// ORDER BY _nature - 元数据驱动的自然顺序扫描（支持深度分页）
    NaturalOrder(NaturalOrderInfo),

    /// 有 ORDER BY + LIMIT - 并行查询 + TopK 合并
    ParallelSortLimit(SortLimitInfo),

    /// 只有 LIMIT 无 ORDER BY - 串行扫描 + 早停
    SerialLimit(PureLimitInfo),

    /// 有 ORDER BY 无 LIMIT - 并行查询 + 流式输出
    ParallelSortStreaming(SortStreamingInfo),

    /// 无 ORDER BY 无 LIMIT - 串行自然顺序扫描
    SerialFullScan,
}

// ===== 聚合查询相关结构 =====

/// 自然顺序查询信息
#[derive(Debug, Clone, PartialEq)]
pub struct NaturalOrderInfo {
    pub limit: usize,
    pub offset: Option<usize>,
    pub has_where_filter: bool,
    /// WHERE 条件的 SQL 文本（如果有）
    pub where_clause: Option<String>,
}

/// COUNT(*) 无 GROUP BY 信息
#[derive(Debug, Clone, PartialEq)]
pub struct CountOnlyInfo {
    /// 是否有 WHERE 条件（用于决定是直接返回总数还是计算 bitmap）
    pub has_where: bool,
}

/// COUNT + 单字段 GROUP BY 信息
#[derive(Debug, Clone, PartialEq)]
pub struct CountGroupByInfo {
    pub group_by_field: String,
}

/// 通用聚合查询信息
#[derive(Debug, Clone, PartialEq)]
pub struct AggregationInfo {
    pub group_by_count: usize,
    pub has_count: bool,
    pub has_sum: bool,
    pub has_avg: bool,
    pub has_max: bool,
    pub has_min: bool,
}

// ===== 非聚合查询相关结构 =====

/// ORDER BY + LIMIT 查询信息
#[derive(Debug, Clone, PartialEq)]
pub struct SortLimitInfo {
    /// 排序字段列表 [(field_name, ascending)]
    pub sort_fields: Vec<(String, bool)>,

    /// LIMIT 数量
    pub limit: usize,

    /// OFFSET（可选）
    pub offset: Option<usize>,

    /// 是否可以使用索引优化（单字段排序 + 该字段有 range 条件）
    pub can_use_index: bool,

    /// 如果可以使用索引，这是相关的 range 条件
    pub range_condition: Option<RangeCondition>,

    /// 是否有 WHERE 条件（用于传递到 segment scanner）
    pub has_where_filter: bool,
}

/// 纯 LIMIT 查询信息（无 ORDER BY）
#[derive(Debug, Clone, PartialEq)]
pub struct PureLimitInfo {
    pub limit: usize,
    pub offset: Option<usize>,
    pub has_where_filter: bool,
}

/// 流式排序查询信息（有 ORDER BY 无 LIMIT）
#[derive(Debug, Clone, PartialEq)]
pub struct SortStreamingInfo {
    pub sort_fields: Vec<(String, bool)>,
    pub has_where_filter: bool,
}

/// Range 条件
#[derive(Debug, Clone, PartialEq)]
pub struct RangeCondition {
    pub field_name: String,
    pub start: Option<ScalarValue>,
    pub start_inclusive: bool,
    pub end: Option<ScalarValue>,
    pub end_inclusive: bool,
}

/// 执行提示 - 用于指导查询执行策略
#[derive(Debug, Clone)]
pub struct ExecutionHints {
    /// 是否有 WHERE 条件
    pub has_where: bool,

    /// WHERE 条件表达式（用于后续 bitmap 计算）
    pub where_conditions: Vec<WhereCondition>,

    /// 查询的字段列表（用于判断索引覆盖）
    pub projection_fields: Vec<String>,

    /// 是否是 SELECT *
    pub is_select_star: bool,

    /// 指定的 partition (如果 WHERE 条件包含 _partition = 'xxx')
    pub target_partition: Option<String>,

    /// 指定的 segment (如果 WHERE 条件包含 _segment = 'xxx')
    pub target_segment: Option<String>,
}

/// WHERE 条件信息
#[derive(Debug, Clone, PartialEq)]
pub struct WhereCondition {
    pub field_name: String,
    pub condition_type: ConditionType,
}

/// 条件类型
#[derive(Debug, Clone, PartialEq)]
pub enum ConditionType {
    Equality, // =
    Range,    // >, >=, <, <=, BETWEEN
    In,       // IN
    Like,     // LIKE
    Other,    // 其他
}

/// 查询计划
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub table_name: String,
    pub query_type: QueryType,
    pub execution_hints: ExecutionHints,
    pub original_sql: String,
}

/// 分析 SQL 查询，返回查询计划
///
/// 这里做所有的查询分类判断，返回明确的执行计划类型
/// 执行器只需要根据 QueryType 直接路由到对应的执行函数
pub fn analyze_query(statement: Statement) -> Option<QueryPlan> {
    // DataFusion Statement 包装了 sqlparser Statement
    let inner_statement = match statement {
        Statement::Statement(boxed) => boxed,
        _ => return None,
    };

    // 只处理 SELECT 语句
    let query = match *inner_statement {
        datafusion::sql::sqlparser::ast::Statement::Query(query) => query,
        _ => return None,
    };

    // 提取表名
    let table_name = extract_table_name_from_query(&query)?;

    // ===== 分析 WHERE 条件和 SELECT 字段 =====
    let execution_hints = analyze_execution_hints(&query);

    // ===== 第一步：检查是否是聚合查询 =====
    if is_aggregation_query_from_ast(&query) {
        let query_type = classify_aggregation_query(&query)?;
        return Some(QueryPlan {
            table_name,
            query_type,
            execution_hints,
            original_sql: String::new(),
        });
    }

    // ===== 第二步：检查非聚合查询类型 =====
    let has_limit = extract_limit_offset_from_ast(&query).is_some();
    let has_order_by = query.order_by.as_ref().map_or(false, |ob| {
        matches!(&ob.kind, datafusion::sql::sqlparser::ast::OrderByKind::Expressions(exprs) if !exprs.is_empty())
    });
    let has_where = execution_hints.has_where;

    // 🚀 检查是否是 ORDER BY _nature
    let is_natural_order = query.order_by.as_ref().map_or(false, |ob| {
        if let datafusion::sql::sqlparser::ast::OrderByKind::Expressions(exprs) = &ob.kind {
            exprs.iter().any(|expr| {
                if let Expr::Identifier(ident) = &expr.expr {
                    ident.value == "_nature"
                } else {
                    false
                }
            })
        } else {
            false
        }
    });

    let query_type = match (has_order_by, has_limit, is_natural_order) {
        // ORDER BY _nature -> 自然顺序查询（深度分页优化）
        (true, _, true) => {
            let (limit, offset) =
                extract_limit_offset_from_ast(&query).unwrap_or((usize::MAX, None));

            // 提取 WHERE 条件的 SQL 文本
            let where_clause = extract_where_clause_sql(&query);

            QueryType::NaturalOrder(NaturalOrderInfo {
                limit,
                offset,
                has_where_filter: has_where,
                where_clause,
            })
        }
        // ORDER BY + LIMIT -> 并行 TopK
        (true, true, false) => {
            let mut sort_limit_info = analyze_sort_limit_from_ast(&query, &table_name)?;
            sort_limit_info.has_where_filter = has_where;
            QueryType::ParallelSortLimit(sort_limit_info)
        }
        // 只有 LIMIT -> 串行早停
        (false, true, _) => {
            let (limit, offset) = extract_limit_offset_from_ast(&query)?;
            QueryType::SerialLimit(PureLimitInfo {
                limit,
                offset,
                has_where_filter: has_where,
            })
        }
        // 只有 ORDER BY -> 并行流式
        (true, false, false) => {
            let sort_fields = query.order_by.as_ref().and_then(|ob| match &ob.kind {
                datafusion::sql::sqlparser::ast::OrderByKind::Expressions(exprs) => {
                    extract_sort_fields_from_ast(exprs)
                }
                _ => None,
            })?;
            QueryType::ParallelSortStreaming(SortStreamingInfo {
                sort_fields,
                has_where_filter: has_where,
            })
        }
        // 都没有 -> 串行全表扫描
        (false, false, _) => QueryType::SerialFullScan,
    };

    Some(QueryPlan {
        table_name,
        query_type,
        execution_hints,
        original_sql: String::new(),
    })
}

/// 分析执行提示信息
fn analyze_execution_hints(query: &Query) -> ExecutionHints {
    let body = &query.body;
    let select = match body.as_ref() {
        SetExpr::Select(select) => select,
        _ => {
            return ExecutionHints {
                has_where: false,
                where_conditions: vec![],
                projection_fields: vec![],
                is_select_star: false,
                target_partition: None,
                target_segment: None,
            }
        }
    };

    // 分析 WHERE 条件
    let (has_where, where_conditions, target_partition, target_segment) =
        if let Some(selection) = &select.selection {
            let conditions = extract_where_conditions(selection);
            let (partition, segment) = extract_partition_segment_hints(selection);
            (true, conditions, partition, segment)
        } else {
            (false, vec![], None, None)
        };

    // 分析 SELECT 字段
    let mut projection_fields = Vec::new();
    let mut is_select_star = false;

    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                is_select_star = true;
            }
            SelectItem::UnnamedExpr(expr) => {
                if let Some(field_name) = extract_field_name_from_expr(expr) {
                    projection_fields.push(field_name);
                }
            }
            SelectItem::ExprWithAlias { expr, .. } => {
                if let Some(field_name) = extract_field_name_from_expr(expr) {
                    projection_fields.push(field_name);
                }
            }
            _ => {}
        }
    }

    ExecutionHints {
        has_where,
        where_conditions,
        projection_fields,
        is_select_star,
        target_partition,
        target_segment,
    }
}

/// 从 Query 中提取 WHERE 条件的 SQL 文本
fn extract_where_clause_sql(query: &datafusion::sql::sqlparser::ast::Query) -> Option<String> {
    use datafusion::sql::sqlparser::ast::SetExpr;

    if let SetExpr::Select(select) = query.body.as_ref() {
        if let Some(selection) = &select.selection {
            // 将 WHERE 表达式转换回 SQL 文本
            return Some(format!("{}", selection));
        }
    }
    None
}

/// 从 WHERE 表达式中提取条件信息
fn extract_where_conditions(expr: &Expr) -> Vec<WhereCondition> {
    let mut conditions = Vec::new();

    match expr {
        // 等值条件: field = value
        Expr::BinaryOp { left, op, right } => {
            use datafusion::sql::sqlparser::ast::BinaryOperator;

            if let Some(field_name) = extract_field_name_from_expr(left) {
                let condition_type = match op {
                    BinaryOperator::Eq => ConditionType::Equality,
                    BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq => ConditionType::Range,
                    _ => ConditionType::Other,
                };
                conditions.push(WhereCondition {
                    field_name,
                    condition_type,
                });
            }

            // 递归处理 AND 条件
            if matches!(op, BinaryOperator::And) {
                conditions.extend(extract_where_conditions(left));
                conditions.extend(extract_where_conditions(right));
            }
        }
        // BETWEEN: field BETWEEN x AND y
        Expr::Between {
            expr: field_expr, ..
        } => {
            if let Some(field_name) = extract_field_name_from_expr(field_expr) {
                conditions.push(WhereCondition {
                    field_name,
                    condition_type: ConditionType::Range,
                });
            }
        }
        // IN: field IN (...)
        Expr::InList {
            expr: field_expr, ..
        } => {
            if let Some(field_name) = extract_field_name_from_expr(field_expr) {
                conditions.push(WhereCondition {
                    field_name,
                    condition_type: ConditionType::In,
                });
            }
        }
        // LIKE: field LIKE pattern
        Expr::Like {
            expr: field_expr, ..
        } => {
            if let Some(field_name) = extract_field_name_from_expr(field_expr) {
                conditions.push(WhereCondition {
                    field_name,
                    condition_type: ConditionType::Like,
                });
            }
        }
        _ => {}
    }

    conditions
}

/// 从表达式中提取字段名
fn extract_field_name_from_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.to_string().to_lowercase()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|i| i.to_string().to_lowercase()),
        _ => None,
    }
}

/// 从 WHERE 表达式中提取 _partition 和 _segment 的值
/// 例如: WHERE _partition = '16_xxx' AND _segment = '0-575880'
fn extract_partition_segment_hints(expr: &Expr) -> (Option<String>, Option<String>) {
    let mut partition = None;
    let mut segment = None;

    match expr {
        Expr::BinaryOp { left, op, right } => {
            use datafusion::sql::sqlparser::ast::BinaryOperator;

            // 处理等值条件: field = value
            if matches!(op, BinaryOperator::Eq) {
                if let Some(field_name) = extract_field_name_from_expr(left) {
                    if field_name == "_partition" {
                        // 提取 partition 值
                        if let Some(value) = extract_string_value_from_expr(right) {
                            partition = Some(value);
                        }
                    } else if field_name == "_segment" {
                        // 提取 segment 值
                        if let Some(value) = extract_string_value_from_expr(right) {
                            segment = Some(value);
                        }
                    }
                }
            }

            // 递归处理 AND 条件
            if matches!(op, BinaryOperator::And) {
                let (left_partition, left_segment) = extract_partition_segment_hints(left);
                let (right_partition, right_segment) = extract_partition_segment_hints(right);

                partition = partition.or(left_partition).or(right_partition);
                segment = segment.or(left_segment).or(right_segment);
            }
        }
        _ => {}
    }

    (partition, segment)
}

/// 从表达式中提取字符串值
fn extract_string_value_from_expr(expr: &Expr) -> Option<String> {
    use datafusion::sql::sqlparser::ast::ValueWithSpan;

    match expr {
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            Value::SingleQuotedString(s) => Some(s.clone()),
            Value::DoubleQuotedString(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// 从 Query AST 中提取表名
fn extract_table_name_from_query(query: &Query) -> Option<String> {
    let body = &query.body;

    let select = match body.as_ref() {
        SetExpr::Select(select) => select,
        _ => return None,
    };

    // 从 FROM 子句中提取表名
    if select.from.is_empty() {
        return None;
    }

    let table_with_joins = &select.from[0];
    let table_factor = &table_with_joins.relation;

    match table_factor {
        TableFactor::Table { name, .. } => {
            // 获取表名（可能是多部分的，如 schema.table）
            // name.0 是 Vec<ObjectNamePart>
            name.0.last().map(|part| part.to_string())
        }
        _ => None,
    }
}
/// 对聚合查询进行分类，返回具体的执行计划类型
fn classify_aggregation_query(query: &Query) -> Option<QueryType> {
    let body = &query.body;
    let select = match body.as_ref() {
        SetExpr::Select(select) => select,
        _ => return None,
    };

    // 提取 GROUP BY 信息
    let group_by_fields = match &select.group_by {
        datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .filter_map(|expr| match expr {
                Expr::Identifier(ident) => Some(ident.to_string().to_lowercase()),
                Expr::CompoundIdentifier(idents) => {
                    idents.last().map(|i| i.to_string().to_lowercase())
                }
                _ => None,
            })
            .collect::<Vec<_>>(),
        _ => vec![],
    };

    // 分析 SELECT 列表中的聚合函数
    let mut has_count = false;
    let mut has_sum = false;
    let mut has_avg = false;
    let mut has_max = false;
    let mut has_min = false;
    let mut total_agg_functions = 0;

    for item in &select.projection {
        if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
            if let Expr::Function(func) = expr {
                let fn_name = func.name.0.last().map(|i| i.to_string().to_lowercase());
                match fn_name.as_deref() {
                    Some("count") => {
                        has_count = true;
                        total_agg_functions += 1;
                    }
                    Some("sum") => {
                        has_sum = true;
                        total_agg_functions += 1;
                    }
                    Some("avg") => {
                        has_avg = true;
                        total_agg_functions += 1;
                    }
                    Some("max") => {
                        has_max = true;
                        total_agg_functions += 1;
                    }
                    Some("min") => {
                        has_min = true;
                        total_agg_functions += 1;
                    }
                    _ => {}
                }
            }
        }
    }

    // ===== 快速路径 1: COUNT(*) 且无 GROUP BY =====
    if group_by_fields.is_empty() && has_count && total_agg_functions == 1 {
        // 检查是否有 WHERE 条件
        let has_where = select.selection.is_some();
        return Some(QueryType::CountOnly(CountOnlyInfo { has_where }));
    }

    // ===== 快速路径 2: COUNT(*) 且 GROUP BY 单字段 =====
    if group_by_fields.len() == 1 && has_count && total_agg_functions == 1 {
        return Some(QueryType::CountWithSingleGroupBy(CountGroupByInfo {
            group_by_field: group_by_fields[0].clone(),
        }));
    }

    // ===== 通用聚合路径 =====
    Some(QueryType::GeneralAggregation(AggregationInfo {
        group_by_count: group_by_fields.len(),
        has_count,
        has_sum,
        has_avg,
        has_max,
        has_min,
    }))
}

/// 检查是否是聚合查询（通过 AST）
fn is_aggregation_query_from_ast(query: &Query) -> bool {
    let body = &query.body;

    let select = match body.as_ref() {
        SetExpr::Select(select) => select,
        _ => return false,
    };

    // 检查是否有 GROUP BY
    // 新版 sqlparser: group_by 是 GroupByExpr enum
    match &select.group_by {
        datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(exprs, _)
            if !exprs.is_empty() =>
        {
            return true;
        }
        _ => {}
    }

    // 检查 SELECT 列表中是否有聚合函数
    for item in &select.projection {
        if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
            if is_aggregate_expr(expr) {
                return true;
            }
        }
    }

    false
}

/// 检查表达式是否包含聚合函数
fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => {
            let fn_name = func.name.0.last().map(|i| i.to_string().to_lowercase());
            matches!(
                fn_name.as_deref(),
                Some("count") | Some("sum") | Some("avg") | Some("max") | Some("min")
            )
        }
        _ => false,
    }
}

/// 从 Query AST 中分析 ORDER BY + LIMIT
fn analyze_sort_limit_from_ast(query: &Query, _table_name: &str) -> Option<SortLimitInfo> {
    // 提取 LIMIT 和 OFFSET
    let (limit, offset) = extract_limit_offset_from_ast(query)?;

    // 提取 ORDER BY 字段
    let sort_fields = if let Some(ref order_by) = query.order_by {
        match &order_by.kind {
            datafusion::sql::sqlparser::ast::OrderByKind::Expressions(exprs) => {
                if !exprs.is_empty() {
                    extract_sort_fields_from_ast(exprs)?
                } else {
                    vec![] // 纯 LIMIT 查询
                }
            }
            _ => vec![],
        }
    } else {
        vec![] // 纯 LIMIT 查询
    };

    // 检查是否可以使用索引优化
    let can_use_index = sort_fields.len() == 1;
    let range_condition = if can_use_index {
        extract_range_condition_from_ast(query, &sort_fields[0].0)
    } else {
        None
    };

    Some(SortLimitInfo {
        sort_fields,
        limit,
        offset,
        can_use_index: can_use_index && range_condition.is_some(),
        range_condition,
        has_where_filter: false, // 将在 analyze_query 中设置
    })
}

/// 从 Query AST 中提取 LIMIT 和 OFFSET
fn extract_limit_offset_from_ast(query: &Query) -> Option<(usize, Option<usize>)> {
    // 新版 sqlparser 使用 limit_clause 字段
    let limit_clause = query.limit_clause.as_ref()?;

    // LimitClause 可能是不同的变体
    use datafusion::sql::sqlparser::ast::LimitClause;

    match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by: _,
        } => {
            // 提取 LIMIT
            let limit_val = match limit {
                Some(expr) => match expr {
                    Expr::Value(value) => match &value.value {
                        Value::Number(n, _) => n.parse::<usize>().ok()?,
                        _ => return None,
                    },
                    _ => return None,
                },
                None => return None,
            };

            // 提取 OFFSET
            let offset_val = match offset {
                Some(Offset { value, .. }) => match value {
                    Expr::Value(value_with_span) => match &value_with_span.value {
                        Value::Number(n, _) => n.parse::<usize>().ok(),
                        _ => None,
                    },
                    _ => None,
                },
                None => None,
            };

            Some((limit_val, offset_val))
        }
        // MySQL 专用语法: LIMIT offset, limit
        LimitClause::OffsetCommaLimit { offset, limit } => {
            // MySQL LIMIT offset, limit 语法
            // AST 中的 offset 字段对应 SQL 中的第一个参数(offset)
            // AST 中的 limit 字段对应 SQL 中的第二个参数(limit)
            let offset_val = match offset {
                Expr::Value(value_with_span) => match &value_with_span.value {
                    Value::Number(n, _) => n.parse::<usize>().ok()?,
                    _ => return None,
                },
                _ => return None,
            };

            let limit_val = match limit {
                Expr::Value(value_with_span) => match &value_with_span.value {
                    Value::Number(n, _) => n.parse::<usize>().ok()?,
                    _ => return None,
                },
                _ => return None,
            };

            Some((limit_val, Some(offset_val)))
        }
    }
}
/// 从 ORDER BY AST 中提取排序字段
fn extract_sort_fields_from_ast(order_by: &[OrderByExpr]) -> Option<Vec<(String, bool)>> {
    let mut fields = Vec::new();

    for order_expr in order_by {
        // 提取字段名
        let field_name = match &order_expr.expr {
            Expr::Identifier(ident) => ident.to_string().to_lowercase(),
            Expr::CompoundIdentifier(idents) => idents.last()?.to_string().to_lowercase(),
            _ => continue,
        };

        // 判断是否升序（None 表示默认升序）
        let ascending = order_expr.options.asc.unwrap_or(true);

        fields.push((field_name, ascending));
    }

    if fields.is_empty() {
        None
    } else {
        Some(fields)
    }
}

/// 从 WHERE 子句中提取 range 条件（针对特定字段）
fn extract_range_condition_from_ast(query: &Query, field_name: &str) -> Option<RangeCondition> {
    let body = &query.body;

    let select = match body.as_ref() {
        SetExpr::Select(select) => select,
        _ => return None,
    };

    // 提取 WHERE 子句
    let selection = select.selection.as_ref()?;

    // 尝试从 WHERE 条件中提取 range 信息
    extract_range_from_expr(selection, field_name)
}

/// 从表达式中提取 range 条件
fn extract_range_from_expr(expr: &Expr, field_name: &str) -> Option<RangeCondition> {
    match expr {
        // BETWEEN 条件
        Expr::Between {
            expr: field_expr,
            negated: false,
            low,
            high,
        } => {
            if matches_field(field_expr, field_name) {
                let start = extract_i64_value(low)?;
                let end = extract_i64_value(high)?;
                return Some(RangeCondition {
                    field_name: field_name.to_string(),
                    start: Some(ScalarValue::Int64(Some(start))),
                    start_inclusive: true,
                    end: Some(ScalarValue::Int64(Some(end))),
                    end_inclusive: true,
                });
            }
        }
        // 二元操作 (>=, >, <=, <, AND)
        Expr::BinaryOp { left, op, right } => {
            use datafusion::sql::sqlparser::ast::BinaryOperator;

            // 处理 AND 条件 - 递归查找
            if matches!(op, BinaryOperator::And) {
                // 尝试从左边提取
                if let Some(cond) = extract_range_from_expr(left, field_name) {
                    return Some(cond);
                }
                // 尝试从右边提取
                if let Some(cond) = extract_range_from_expr(right, field_name) {
                    return Some(cond);
                }
                return None;
            }

            // 处理比较操作
            if matches_field(left, field_name) {
                if let Some(value) = extract_i64_value(right) {
                    return Some(match op {
                        BinaryOperator::GtEq => RangeCondition {
                            field_name: field_name.to_string(),
                            start: Some(ScalarValue::Int64(Some(value))),
                            start_inclusive: true,
                            end: None,
                            end_inclusive: false,
                        },
                        BinaryOperator::Gt => RangeCondition {
                            field_name: field_name.to_string(),
                            start: Some(ScalarValue::Int64(Some(value))),
                            start_inclusive: false,
                            end: None,
                            end_inclusive: false,
                        },
                        BinaryOperator::LtEq => RangeCondition {
                            field_name: field_name.to_string(),
                            start: None,
                            start_inclusive: false,
                            end: Some(ScalarValue::Int64(Some(value))),
                            end_inclusive: true,
                        },
                        BinaryOperator::Lt => RangeCondition {
                            field_name: field_name.to_string(),
                            start: None,
                            start_inclusive: false,
                            end: Some(ScalarValue::Int64(Some(value))),
                            end_inclusive: false,
                        },
                        _ => return None,
                    });
                }
            }
        }
        _ => {}
    }

    None
}

/// 检查表达式是否匹配指定字段名
fn matches_field(expr: &Expr, field_name: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.to_string().to_lowercase() == field_name,
        Expr::CompoundIdentifier(idents) => {
            idents.last().map(|i| i.to_string().to_lowercase()) == Some(field_name.to_string())
        }
        _ => false,
    }
}

/// 从表达式中提取 i64 值
fn extract_i64_value(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Value(value_with_span) => match &value_with_span.value {
            Value::Number(n, _) => n.parse::<i64>().ok(),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute::sql_normalizer::SqlNormalizer;

    fn parse_sql(sql: &str) -> Statement {
        let (statement, _) = SqlNormalizer::normalize(sql).unwrap();
        statement
    }

    #[test]
    fn test_simple_query() {
        let sql = "SELECT * FROM users WHERE age > 20";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        // 简单查询应该返回 SerialFullScan
        assert!(matches!(plan.query_type, QueryType::SerialFullScan));
    }

    #[test]
    fn test_sort_limit_query() {
        let sql = "SELECT * FROM users WHERE age BETWEEN 20 AND 30 ORDER BY age ASC LIMIT 10";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        assert_eq!(plan.table_name, "users");

        if let QueryType::ParallelSortLimit(info) = plan.query_type {
            assert_eq!(info.sort_fields.len(), 1);
            assert_eq!(info.sort_fields[0].0, "age");
            assert_eq!(info.sort_fields[0].1, true); // ASC
            assert_eq!(info.limit, 10);
            assert_eq!(info.can_use_index, true);
        } else {
            panic!("Expected ParallelSortLimit query type");
        }
    }

    #[test]
    fn test_multi_field_sort() {
        let sql = "SELECT * FROM users ORDER BY age DESC, name ASC LIMIT 10";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        if let QueryType::ParallelSortLimit(info) = plan.query_type {
            assert_eq!(info.sort_fields.len(), 2);
            assert_eq!(info.sort_fields[0].0, "age");
            assert_eq!(info.sort_fields[0].1, false); // DESC
            assert_eq!(info.can_use_index, false); // 多字段排序不能用索引
        } else {
            panic!("Expected ParallelSortLimit query type");
        }
    }

    #[test]
    fn test_aggregation_query() {
        let sql = "SELECT COUNT(*) FROM users WHERE age > 20";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        // COUNT(*) 无 GROUP BY 应该是 CountOnly
        if let QueryType::CountOnly(info) = plan.query_type {
            assert!(info.has_where); // 有 WHERE 条件
        } else {
            panic!("Expected CountOnly query type");
        }
    }

    #[test]
    fn test_mysql_limit_with_offset() {
        // MySQL 风格: LIMIT offset, limit
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 5, 10";
        let statement = parse_sql(sql);
        let plan_result = analyze_query(statement);

        // Debug: 如果没有返回 plan, 打印 SQL
        if plan_result.is_none() {
            eprintln!("analyze_query returned None for SQL: {}", sql);
            // 尝试直接调用 SqlNormalizer 看看转换后的 SQL
            let (_, normalized) =
                crate::compute::sql_normalizer::SqlNormalizer::normalize(sql).unwrap();
            eprintln!("Normalized SQL: {}", normalized);
        }

        let plan = plan_result.unwrap();

        if let QueryType::ParallelSortLimit(info) = plan.query_type {
            assert_eq!(info.limit, 10);
            assert_eq!(info.offset, Some(5));
        } else {
            panic!("Expected ParallelSortLimit query type");
        }
    }

    #[test]
    fn test_mysql_limit_zero_offset() {
        // LIMIT 0,10 应该返回 10 条记录
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 0, 10";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        if let QueryType::ParallelSortLimit(info) = plan.query_type {
            assert_eq!(info.limit, 10);
            assert_eq!(info.offset, Some(0));
        } else {
            panic!("Expected ParallelSortLimit query type");
        }
    }

    #[test]
    fn test_mysql_limit_without_space() {
        // LIMIT 0,10 没有空格的情况
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 0,10";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        if let QueryType::ParallelSortLimit(info) = plan.query_type {
            assert_eq!(info.limit, 10);
            assert_eq!(info.offset, Some(0));
        } else {
            panic!("Expected ParallelSortLimit query type");
        }
    }

    #[test]
    fn test_mysql_limit_offset_without_space() {
        // LIMIT 5,20 没有空格
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 5,20";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        if let QueryType::ParallelSortLimit(info) = plan.query_type {
            assert_eq!(info.limit, 20);
            assert_eq!(info.offset, Some(5));
        } else {
            panic!("Expected ParallelSortLimit query type");
        }
    }

    #[test]
    fn test_standard_sql_limit_offset() {
        // 标准 SQL: LIMIT limit OFFSET offset
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 10 OFFSET 5";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        if let QueryType::ParallelSortLimit(info) = plan.query_type {
            assert_eq!(info.limit, 10);
            assert_eq!(info.offset, Some(5));
        } else {
            panic!("Expected ParallelSortLimit query type");
        }
    }

    #[test]
    fn test_pure_limit() {
        // 纯 LIMIT（无 OFFSET）
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 10";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        if let QueryType::ParallelSortLimit(info) = plan.query_type {
            assert_eq!(info.limit, 10);
            assert_eq!(info.offset, None);
        } else {
            panic!("Expected ParallelSortLimit query type");
        }
    }
}
