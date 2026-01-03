/// SQL 标准化器
///
/// 将MySQL特有的SQL语法转换为DataFusion支持的标准SQL语法
/// 同时验证SQL的合法性
use crate::utils::error::{CoreError, CoreResult};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr, SetExpr, Value, ValueWithSpan};
use datafusion::sql::sqlparser::dialect::MySqlDialect;
use regex::Regex;
use std::collections::HashSet;

/// SQL 标准化结果
#[derive(Debug, Clone)]
pub struct NormalizedSql {
    /// 原始 AST Statement
    pub statement: Statement,

    /// 改写后的 SQL（移除了 _partition 条件）
    pub rewritten_sql: String,

    /// 提取出的分区过滤条件
    pub partition_filters: PartitionFilters,

    /// 是否需要注入 `_score` 虚拟列
    pub needs_score_column: bool,
}

/// 分区过滤条件
#[derive(Debug, Clone, Default)]
pub struct PartitionFilters {
    /// 精确匹配的分区名列表
    /// 例如: _partition = '20240101' 或 _partition IN ('20240101', '20240102')
    pub exact_matches: Vec<String>,

    /// LIKE 模式列表
    /// 例如: _partition LIKE '2024%' 或 _partition LIKE '202401__'
    pub like_patterns: Vec<String>,

    /// 是否有 _partition 条件
    pub has_filter: bool,
}

impl PartitionFilters {
    /// 判断是否需要扫描所有分区
    pub fn scan_all(&self) -> bool {
        !self.has_filter
    }

    /// 解析分区列表（根据过滤条件）
    pub fn resolve_partitions(&self, all_partitions: &[String]) -> Vec<String> {
        log::debug!(
            "🔍 [resolve_partitions] has_filter={}, exact_matches={:?}, like_patterns={:?}, all_partitions={:?}",
            self.has_filter,
            self.exact_matches,
            self.like_patterns,
            all_partitions
        );

        if !self.has_filter {
            // 没有过滤条件，返回所有分区
            log::debug!(
                "🔍 [resolve_partitions] No filter, returning all {} partitions",
                all_partitions.len()
            );
            return all_partitions.to_vec();
        }

        let mut matched = HashSet::new();

        // 1. 添加精确匹配的分区
        for partition in &self.exact_matches {
            log::debug!(
                "🔍 [resolve_partitions] Checking exact match: '{}' in {:?}",
                partition,
                all_partitions
            );
            if all_partitions.contains(partition) {
                log::debug!("✅ [resolve_partitions] Matched: '{}'", partition);
                matched.insert(partition.clone());
            } else {
                log::debug!("❌ [resolve_partitions] Not found: '{}'", partition);
            }
        }

        // 2. 处理 LIKE 模式
        for pattern in &self.like_patterns {
            if let Ok(regex_pattern) = Self::convert_sql_like_to_regex(pattern) {
                for partition in all_partitions {
                    if regex_pattern.is_match(partition) {
                        matched.insert(partition.clone());
                    }
                }
            }
        }

        let result: Vec<String> = matched.into_iter().collect();
        log::debug!(
            "🔍 [resolve_partitions] Final matched partitions: {:?}",
            result
        );
        result
    }

    /// 将 SQL LIKE 模式转换为正则表达式
    ///
    /// SQL LIKE:
    /// - `%` 匹配任意字符序列
    /// - `_` 匹配单个字符
    ///
    /// 例如:
    /// - `2024%` -> `^2024.*$`
    /// - `2024__01` -> `^2024..01$`
    fn convert_sql_like_to_regex(pattern: &str) -> Result<Regex, regex::Error> {
        // 转义特殊字符，除了 % 和 _
        let mut regex_pattern = String::new();
        regex_pattern.push('^');

        for ch in pattern.chars() {
            match ch {
                '%' => regex_pattern.push_str(".*"),
                '_' => regex_pattern.push('.'),
                // 转义正则表达式特殊字符
                '.' | '+' | '*' | '?' | '^' | '$' | '(' | ')' | '[' | ']' | '{' | '}' | '|'
                | '\\' => {
                    regex_pattern.push('\\');
                    regex_pattern.push(ch);
                }
                _ => regex_pattern.push(ch),
            }
        }

        regex_pattern.push('$');
        Regex::new(&regex_pattern)
    }
}

pub struct SqlNormalizer;

impl SqlNormalizer {
    /// 标准化SQL语句（新版本，返回 NormalizedSql）
    ///
    /// 1. 验证SQL语法
    /// 2. 提取 _partition 过滤条件
    /// 3. 改写 SQL，移除 _partition 条件
    /// 4. 将MySQL特有语法转换为标准SQL
    /// 5. 将 Timestamp 字段的 Int64 比较值转换为 CAST 表达式
    pub fn normalize(sql: &str) -> CoreResult<NormalizedSql> {
        let dialect = MySqlDialect {};

        let mut statements = DFParser::parse_sql_with_dialect(sql, &dialect)?;

        let statement = if statements.len() != 1 {
            return Err(CoreError::InvalidParam(format!(
                "Expected 1 SQL statement, got {}",
                statements.len()
            )));
        } else {
            statements.pop_front().ok_or_else(|| {
                CoreError::InvalidParam("Failed to extract SQL statement".to_string())
            })?
        };

        // 🎯 提取 _partition 过滤条件
        let partition_filters = Self::extract_partition_filters(&statement)?;

        // 🔧 改写 SQL，移除 _partition 条件
        let rewritten_statement = Self::remove_partition_conditions(statement.clone())?;
        let mut rewritten_sql = rewritten_statement.to_string();

        // 🔧 修复 Timestamp 字段类型不匹配问题
        rewritten_sql = Self::fix_timestamp_comparisons(&rewritten_sql);

        let needs_score_column = Self::detect_score_usage(&rewritten_sql);

        Ok(NormalizedSql {
            statement,
            rewritten_sql,
            partition_filters,
            needs_score_column,
        })
    }

    /// 检测 SQL 中是否引用了 `_score` 虚拟列
    fn detect_score_usage(sql: &str) -> bool {
        sql.to_lowercase().contains("_score")
    }

    /// 提取 _partition 过滤条件
    fn extract_partition_filters(statement: &Statement) -> CoreResult<PartitionFilters> {
        let mut filters = PartitionFilters::default();

        if let Statement::Statement(boxed) = statement {
            if let datafusion::sql::sqlparser::ast::Statement::Query(query) = boxed.as_ref() {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(selection) = &select.selection {
                        Self::extract_from_expr(selection, &mut filters);
                    }
                }
            }
        }

        filters.has_filter = !filters.exact_matches.is_empty() || !filters.like_patterns.is_empty();

        Ok(filters)
    }

    /// 从表达式中递归提取 _partition 条件
    fn extract_from_expr(expr: &Expr, filters: &mut PartitionFilters) {
        match expr {
            // _partition = 'value'
            Expr::BinaryOp { left, op, right } => {
                if *op == BinaryOperator::Eq && Self::is_partition_field(left) {
                    if let Some(value) = Self::extract_string_literal(right) {
                        log::debug!("🔍 [extract_from_expr] Found _partition = '{}'", value);
                        filters.exact_matches.push(value);
                    }
                } else if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                    // 递归处理 AND/OR
                    Self::extract_from_expr(left, filters);
                    Self::extract_from_expr(right, filters);
                }
            }
            // _partition IN ('val1', 'val2')
            Expr::InList { expr, list, .. } => {
                if Self::is_partition_field(expr) {
                    for item in list {
                        if let Some(value) = Self::extract_string_literal(item) {
                            filters.exact_matches.push(value);
                        }
                    }
                }
            }
            // _partition LIKE 'pattern'
            Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
                if Self::is_partition_field(expr) {
                    if let Some(pattern_str) = Self::extract_string_literal(pattern) {
                        filters.like_patterns.push(pattern_str);
                    }
                }
            }
            Expr::Nested(inner) => {
                Self::extract_from_expr(inner, filters);
            }
            _ => {}
        }
    }

    /// 判断是否是 _partition 字段
    fn is_partition_field(expr: &Expr) -> bool {
        match expr {
            Expr::Identifier(ident) => ident.value == "_partition",
            Expr::CompoundIdentifier(idents) => {
                idents.last().map(|i| i.value.as_str()) == Some("_partition")
            }
            _ => false,
        }
    }

    /// 提取字符串字面量
    fn extract_string_literal(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Value(value_with_span) => match &value_with_span.value {
                Value::SingleQuotedString(s) => Some(s.clone()),
                Value::DoubleQuotedString(s) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    /// 移除 _partition 条件
    fn remove_partition_conditions(statement: Statement) -> CoreResult<Statement> {
        match statement {
            Statement::Statement(boxed) => {
                match *boxed {
                    datafusion::sql::sqlparser::ast::Statement::Query(mut query) => {
                        // 修改 query 的 body
                        if let SetExpr::Select(mut select) = *query.body {
                            if let Some(selection) = select.selection {
                                select.selection = Some(Self::remove_partition_expr(selection));
                            }
                            query.body = Box::new(SetExpr::Select(select));
                        }

                        Ok(Statement::Statement(Box::new(
                            datafusion::sql::sqlparser::ast::Statement::Query(query),
                        )))
                    }
                    other => {
                        // 非 Query 语句，直接返回
                        Ok(Statement::Statement(Box::new(other)))
                    }
                }
            }
            // 处理其他 DataFusion 特有的 Statement 类型
            other => Ok(other),
        }
    }

    /// 递归移除表达式中的 _partition 条件
    fn remove_partition_expr(expr: Expr) -> Expr {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // 先递归处理子表达式
                if matches!(op, BinaryOperator::And) {
                    let left_cleaned = Self::remove_partition_expr(*left);
                    let right_cleaned = Self::remove_partition_expr(*right);

                    // 如果左边是 _partition 条件（被替换为 TRUE），返回右边
                    if Self::is_true_value(&left_cleaned) {
                        return right_cleaned;
                    }

                    // 如果右边是 _partition 条件（被替换为 TRUE），返回左边
                    if Self::is_true_value(&right_cleaned) {
                        return left_cleaned;
                    }

                    return Expr::BinaryOp {
                        left: Box::new(left_cleaned),
                        op,
                        right: Box::new(right_cleaned),
                    };
                }

                // 检查是否是 _partition 等值条件
                if matches!(op, BinaryOperator::Eq) && Self::is_partition_field(&left) {
                    return Expr::Value(ValueWithSpan::from(Value::Boolean(true)));
                }

                Expr::BinaryOp { left, op, right }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                if Self::is_partition_field(&expr) {
                    return Expr::Value(ValueWithSpan::from(Value::Boolean(true)));
                }
                Expr::InList {
                    expr,
                    list,
                    negated,
                }
            }
            Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
                any,
            } => {
                if Self::is_partition_field(&expr) {
                    return Expr::Value(ValueWithSpan::from(Value::Boolean(true)));
                }
                Expr::Like {
                    negated,
                    expr,
                    pattern,
                    escape_char,
                    any,
                }
            }
            Expr::Nested(inner) => {
                let cleaned = Self::remove_partition_expr(*inner);
                if Self::is_true_value(&cleaned) {
                    Expr::Value(ValueWithSpan::from(Value::Boolean(true)))
                } else {
                    Expr::Nested(Box::new(cleaned))
                }
            }
            _ => expr,
        }
    }

    /// 判断是否是 _partition 相关条件
    fn is_partition_condition(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryOp { left, op, .. } => {
                matches!(op, BinaryOperator::Eq) && Self::is_partition_field(left)
            }
            Expr::InList { expr, .. } | Expr::Like { expr, .. } | Expr::ILike { expr, .. } => {
                Self::is_partition_field(expr)
            }
            _ => false,
        }
    }

    /// 判断表达式是否是 TRUE 值
    fn is_true_value(expr: &Expr) -> bool {
        match expr {
            Expr::Value(value_with_span) => matches!(value_with_span.value, Value::Boolean(true)),
            _ => false,
        }
    }

    /// 修复 Timestamp 字段的类型不匹配
    ///
    /// DataFusion 要求 Timestamp 字段与 Timestamp 类型比较，不能直接与 Int64 比较
    /// 这个函数将常见的时间戳字段（如 pickup_datetime, dropoff_datetime, created_at, updated_at 等）
    /// 的 Int64 字面量自动包装为 to_timestamp_millis(value) 函数调用
    /// 使用 to_timestamp_millis 而不是 CAST 可以避免溢出问题
    fn fix_timestamp_comparisons(sql: &str) -> String {
        // 常见的时间戳字段名模式
        let timestamp_fields = vec![
            "pickup_datetime",
            "dropoff_datetime",
            "created_at",
            "updated_at",
            "timestamp",
            "time",
            "datetime",
        ];

        let mut result = sql.to_string();

        for field in timestamp_fields {
            // 匹配模式: field_name [比较符] [数字]
            // 例如: pickup_datetime >= 1704067200000
            let pattern = format!(
                r"(?i)\b{}\s*(>=|<=|>|<|=|!=)\s*(\d+)\b",
                regex::escape(field)
            );

            if let Ok(re) = Regex::new(&pattern) {
                result = re
                    .replace_all(&result, |caps: &regex::Captures| {
                        let operator = &caps[1];
                        let value = &caps[2];

                        // 替换为: field_name [比较符] to_timestamp_millis(数字)
                        // to_timestamp_millis 是 DataFusion 内置函数，将毫秒时间戳转为 Timestamp
                        format!("{} {} to_timestamp_millis({})", field, operator, value)
                    })
                    .to_string();
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_standard_sql() {
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 10";
        let result = SqlNormalizer::normalize(sql).unwrap();
        // AST 转换后可能格式略有不同，只检查包含关键部分
        assert!(result.rewritten_sql.contains("SELECT"));
        assert!(result.rewritten_sql.contains("FROM users"));
        assert!(result.rewritten_sql.contains("ORDER BY age DESC"));
        assert!(result.rewritten_sql.contains("LIMIT 10"));
        assert!(!result.partition_filters.has_filter);
        assert!(!result.needs_score_column);
    }

    #[test]
    fn test_convert_mysql_limit() {
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 0,10";
        let result = SqlNormalizer::normalize(sql).unwrap();

        // 验证 Statement 能够被正确解析
        assert!(matches!(result.statement, Statement::Statement(_)));
    }

    #[test]
    fn test_convert_mysql_limit_with_space() {
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 5, 20";
        let result = SqlNormalizer::normalize(sql).unwrap();

        // 验证 Statement 能够被正确解析
        assert!(matches!(result.statement, Statement::Statement(_)));
    }

    #[test]
    fn test_convert_mysql_limit_zero_offset() {
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 0, 10";
        let result = SqlNormalizer::normalize(sql).unwrap();

        // 验证 Statement 能够被正确解析
        assert!(matches!(result.statement, Statement::Statement(_)));
    }

    #[test]
    fn test_validate_invalid_sql() {
        let sql = "SELCT * FROM users"; // 故意拼错
        let result = SqlNormalizer::normalize(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_standard_limit_offset() {
        let sql = "SELECT * FROM users LIMIT 10 OFFSET 5";
        let result = SqlNormalizer::normalize(sql).unwrap();
        assert!(result.rewritten_sql.contains("LIMIT 10"));
        assert!(result.rewritten_sql.contains("OFFSET 5"));
    }

    #[test]
    fn test_partition_filter_exact_match() {
        let sql = "SELECT * FROM logs WHERE _partition = '20240101' AND level = 'ERROR'";
        let result = SqlNormalizer::normalize(sql).unwrap();

        // 验证提取到了分区过滤条件
        assert!(result.partition_filters.has_filter);
        assert_eq!(result.partition_filters.exact_matches.len(), 1);
        assert_eq!(result.partition_filters.exact_matches[0], "20240101");

        // 验证 SQL 被改写，移除了 _partition 条件
        assert!(!result.rewritten_sql.contains("_partition"));
        assert!(result.rewritten_sql.contains("level = 'ERROR'"));
    }

    #[test]
    fn test_partition_filter_in_list() {
        let sql =
            "SELECT * FROM logs WHERE _partition IN ('20240101', '20240102') AND status = 200";
        let result = SqlNormalizer::normalize(sql).unwrap();

        assert!(result.partition_filters.has_filter);
        assert_eq!(result.partition_filters.exact_matches.len(), 2);
        assert!(result
            .partition_filters
            .exact_matches
            .contains(&"20240101".to_string()));
        assert!(result
            .partition_filters
            .exact_matches
            .contains(&"20240102".to_string()));

        // 验证改写后的 SQL
        assert!(!result.rewritten_sql.contains("_partition"));
        assert!(result.rewritten_sql.contains("status = 200"));
    }

    #[test]
    fn test_partition_filter_like() {
        let sql = "SELECT * FROM logs WHERE _partition LIKE '2024%' AND user_id > 100";
        let result = SqlNormalizer::normalize(sql).unwrap();

        assert!(result.partition_filters.has_filter);
        assert_eq!(result.partition_filters.like_patterns.len(), 1);
        assert_eq!(result.partition_filters.like_patterns[0], "2024%");

        assert!(!result.rewritten_sql.contains("_partition"));
        assert!(result.rewritten_sql.contains("user_id > 100"));
    }

    #[test]
    fn test_partition_filter_resolve() {
        let mut filters = PartitionFilters::default();
        filters.exact_matches.push("20240101".to_string());
        filters.like_patterns.push("202402%".to_string());
        filters.has_filter = true;

        let all_partitions = vec![
            "20240101".to_string(),
            "20240102".to_string(),
            "20240201".to_string(),
            "20240202".to_string(),
            "20240301".to_string(),
        ];

        let matched = filters.resolve_partitions(&all_partitions);

        // 应该匹配: 20240101 (精确) + 20240201, 20240202 (LIKE '202402%')
        assert_eq!(matched.len(), 3);
        assert!(matched.contains(&"20240101".to_string()));
        assert!(matched.contains(&"20240201".to_string()));
        assert!(matched.contains(&"20240202".to_string()));
    }

    #[test]
    fn test_fulltext_functions_survive_normalization() {
        let sql =
            "SELECT id, message FROM logs WHERE text(message, 'error', 1.0) ORDER BY _score DESC";
        let result = SqlNormalizer::normalize(sql).unwrap();

        assert!(result.rewritten_sql.contains("text"));
        assert!(result.rewritten_sql.contains("_score"));
        assert!(!result.partition_filters.has_filter);
        assert!(result.needs_score_column);
    }
}
