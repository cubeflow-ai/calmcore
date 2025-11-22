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

/// 查询类型
#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    /// 聚合查询（需要在 DistributedExecutor 层合并）
    Aggregation,

    /// ORDER BY + LIMIT 查询（需要在协调节点做最终排序）
    SortLimit(SortLimitInfo),
}

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

/// 查询计划
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub table_name: String,
    pub query_type: QueryType,
    pub original_sql: String,
}

/// 分析 SQL 查询，返回查询计划
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

    // 检查是否是聚合查询
    if is_aggregation_query_from_ast(&query) {
        return Some(QueryPlan {
            table_name,
            query_type: QueryType::Aggregation,
            original_sql: String::new(), // AST 模式不需要保存原始 SQL
        });
    }

    // 检查是否有 ORDER BY + LIMIT
    if let Some(sort_limit_info) = analyze_sort_limit_from_ast(&query, &table_name) {
        return Some(QueryPlan {
            table_name,
            query_type: QueryType::SortLimit(sort_limit_info),
            original_sql: String::new(),
        });
    }

    // 其他查询不需要特殊处理，返回 None
    None
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
        let plan = analyze_query(statement);

        // 简单查询不需要特殊处理，返回 None
        assert!(plan.is_none());
    }

    #[test]
    fn test_sort_limit_query() {
        let sql = "SELECT * FROM users WHERE age BETWEEN 20 AND 30 ORDER BY age ASC LIMIT 10";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        assert_eq!(plan.table_name, "users");

        if let QueryType::SortLimit(info) = plan.query_type {
            assert_eq!(info.sort_fields.len(), 1);
            assert_eq!(info.sort_fields[0].0, "age");
            assert_eq!(info.sort_fields[0].1, true); // ASC
            assert_eq!(info.limit, 10);
            assert_eq!(info.can_use_index, true);
        } else {
            panic!("Expected SortLimit query type");
        }
    }

    #[test]
    fn test_multi_field_sort() {
        let sql = "SELECT * FROM users ORDER BY age DESC, name ASC LIMIT 10";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        if let QueryType::SortLimit(info) = plan.query_type {
            assert_eq!(info.sort_fields.len(), 2);
            assert_eq!(info.sort_fields[0].0, "age");
            assert_eq!(info.sort_fields[0].1, false); // DESC
            assert_eq!(info.can_use_index, false); // 多字段排序不能用索引
        } else {
            panic!("Expected SortLimit query type");
        }
    }

    #[test]
    fn test_aggregation_query() {
        let sql = "SELECT COUNT(*) FROM users WHERE age > 20";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        assert_eq!(plan.query_type, QueryType::Aggregation);
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

        if let QueryType::SortLimit(info) = plan.query_type {
            assert_eq!(info.limit, 10);
            assert_eq!(info.offset, Some(5));
        } else {
            panic!("Expected SortLimit query type");
        }
    }

    #[test]
    fn test_mysql_limit_zero_offset() {
        // LIMIT 0,10 应该返回 10 条记录
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 0, 10";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        if let QueryType::SortLimit(info) = plan.query_type {
            assert_eq!(info.limit, 10);
            assert_eq!(info.offset, Some(0));
        } else {
            panic!("Expected SortLimit query type");
        }
    }

    #[test]
    fn test_mysql_limit_without_space() {
        // LIMIT 0,10 没有空格的情况
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 0,10";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        if let QueryType::SortLimit(info) = plan.query_type {
            assert_eq!(info.limit, 10);
            assert_eq!(info.offset, Some(0));
        } else {
            panic!("Expected SortLimit query type");
        }
    }

    #[test]
    fn test_mysql_limit_offset_without_space() {
        // LIMIT 5,20 没有空格
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 5,20";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        if let QueryType::SortLimit(info) = plan.query_type {
            assert_eq!(info.limit, 20);
            assert_eq!(info.offset, Some(5));
        } else {
            panic!("Expected SortLimit query type");
        }
    }

    #[test]
    fn test_standard_sql_limit_offset() {
        // 标准 SQL: LIMIT limit OFFSET offset
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 10 OFFSET 5";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        if let QueryType::SortLimit(info) = plan.query_type {
            assert_eq!(info.limit, 10);
            assert_eq!(info.offset, Some(5));
        } else {
            panic!("Expected SortLimit query type");
        }
    }

    #[test]
    fn test_pure_limit() {
        // 纯 LIMIT（无 OFFSET）
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 10";
        let statement = parse_sql(sql);
        let plan = analyze_query(statement).unwrap();

        if let QueryType::SortLimit(info) = plan.query_type {
            assert_eq!(info.limit, 10);
            assert_eq!(info.offset, None);
        } else {
            panic!("Expected SortLimit query type");
        }
    }
}
