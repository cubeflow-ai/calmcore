/// SQL 解析和处理工具
///
/// 提供 SQL 字符串的解析、修改、清理等功能
use crate::utils::error::{CoreError, CoreResult};
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, SetExpr, Statement, Value, ValueWithSpan,
};
use datafusion::sql::sqlparser::dialect::MySqlDialect;
use datafusion::sql::sqlparser::parser::Parser;

/// SQL 工具
pub struct SqlUtils;

impl SqlUtils {
    /// 从 SQL 中移除 _partition 和 _segment 虚拟字段
    /// 这些字段只用于路由，不是实际表字段
    pub fn remove_virtual_columns(sql: &str) -> CoreResult<String> {
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
                    *selection = Self::filter_virtual_columns_from_expr(selection.clone());

                    // 如果 WHERE 条件被完全移除（只剩下虚拟字段），移除整个 WHERE 子句
                    if Self::is_true_expr(selection) {
                        select.selection = None;
                    }
                }
            }
        }

        Ok(statements[0].to_string())
    }

    /// 从 SQL 中提取 LIMIT 和 OFFSET
    pub fn extract_limit_offset(sql: &str) -> (Option<usize>, Option<usize>) {
        let sql_lower = sql.to_lowercase();

        let mut limit = None;
        let mut offset = None;

        // 查找 LIMIT 子句
        if let Some(limit_pos) = sql_lower.find("limit") {
            let after_limit = sql[limit_pos + 5..].trim();

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

    /// 从 SQL 中移除 OFFSET，替换为新的 LIMIT
    pub fn remove_offset(sql: &str, new_limit: usize) -> String {
        let sql_lower = sql.to_lowercase();

        // 查找 LIMIT 子句位置
        if let Some(limit_pos) = sql_lower.find("limit") {
            let before_limit = &sql[..limit_pos];
            format!("{}LIMIT {}", before_limit, new_limit)
        } else {
            sql.to_string()
        }
    }

    /// 确保排序字段在 SELECT 列表中
    pub fn ensure_sort_fields_in_select(
        sql: &str,
        sort_fields: &[(String, bool)],
    ) -> CoreResult<String> {
        let dialect = MySqlDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| CoreError::InvalidParam(format!("Failed to parse SQL: {}", e)))?;

        if statements.is_empty() {
            return Ok(sql.to_string());
        }

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                // 如果是 SELECT *，不需要修改
                if select
                    .projection
                    .iter()
                    .any(|p| matches!(p, datafusion::sql::sqlparser::ast::SelectItem::Wildcard(_)))
                {
                    return Ok(sql.to_string());
                }

                // 检查排序字段是否都在 SELECT 列表中
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

                // 如果有缺失的排序字段，添加到 SELECT 列表
                if !missing_fields.is_empty() {
                    return Self::add_fields_to_select(sql, &missing_fields);
                }
            }
        }

        Ok(sql.to_string())
    }

    /// 递归过滤表达式中的虚拟字段条件
    fn filter_virtual_columns_from_expr(expr: Expr) -> Expr {
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
                        let new_left = Self::filter_virtual_columns_from_expr(*left);
                        let new_right = Self::filter_virtual_columns_from_expr(*right);

                        // 优化：如果一边是 TRUE，返回另一边
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
                        let new_left = Self::filter_virtual_columns_from_expr(*left);
                        let new_right = Self::filter_virtual_columns_from_expr(*right);

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
    fn is_true_expr(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::Value(ValueWithSpan {
                value: Value::Boolean(true),
                ..
            })
        )
    }

    /// 字符串方式添加字段到 SELECT 列表
    fn add_fields_to_select(sql: &str, fields: &[String]) -> CoreResult<String> {
        let sql_upper = sql.to_uppercase();

        // 找到 SELECT 和 FROM 之间的位置
        if let Some(select_pos) = sql_upper.find("SELECT") {
            if let Some(from_pos) = sql_upper.find("FROM") {
                let select_end = select_pos + 6; // "SELECT".len()
                let projection = sql[select_end..from_pos].trim();
                let rest = &sql[from_pos..];

                // 添加缺失字段
                let additional_fields = fields.join(", ");
                let new_sql = format!("SELECT {}, {} {}", projection, additional_fields, rest);

                return Ok(new_sql);
            }
        }

        Ok(sql.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_virtual_columns() {
        let sql = "SELECT * FROM t WHERE _partition='p1' AND age > 18";
        let result = SqlUtils::remove_virtual_columns(sql).unwrap();
        assert!(result.contains("age > 18"));
        assert!(!result.contains("_partition"));
    }

    #[test]
    fn test_extract_limit_offset() {
        let sql = "SELECT * FROM t LIMIT 100 OFFSET 50";
        let (limit, offset) = SqlUtils::extract_limit_offset(sql);
        assert_eq!(limit, Some(100));
        assert_eq!(offset, Some(50));
    }
}
