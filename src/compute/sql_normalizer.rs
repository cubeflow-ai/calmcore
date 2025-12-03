/// SQL 标准化器
///
/// 将MySQL特有的SQL语法转换为DataFusion支持的标准SQL语法
/// 同时验证SQL的合法性
use crate::utils::error::{CoreError, CoreResult};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::MySqlDialect;
use regex::Regex;

pub struct SqlNormalizer;

impl SqlNormalizer {
    /// 标准化SQL语句
    ///
    /// 1. 验证SQL语法
    /// 2. 将MySQL特有语法转换为标准SQL
    /// 3. 将 Timestamp 字段的 Int64 比较值转换为 CAST 表达式
    /// 4. 自动为重复的投影列添加别名（兼容MySQL行为）
    /// 5. 返回 (Statement AST, 标准化后的SQL字符串)
    pub fn normalize(sql: &str) -> CoreResult<(Statement, String)> {
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

        // 将 AST 转回 SQL 字符串
        let mut normalized = statement.to_string();

        // 🔧 修复 Timestamp 字段类型不匹配问题
        // 将 "pickup_datetime >= 1704067200000" 转换为 "pickup_datetime >= CAST(1704067200000 AS TIMESTAMP)"
        normalized = Self::fix_timestamp_comparisons(&normalized);

        // 🔧 自动为重复的投影列添加别名（兼容MySQL行为）
        // MySQL 允许 SELECT '11.3.83.3', 'r2api', 'r2api' 这样的重复列
        // 但 DataFusion 要求每个投影必须有唯一名称
        normalized = Self::fix_duplicate_projections(&normalized);

        Ok((statement, normalized))
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

    /// 自动为重复的投影列添加别名
    ///
    /// MySQL 允许 SELECT 中有重复的常量列，例如:
    /// SELECT '11.3.83.3', 'r2api', 'r2api', trace_id FROM table
    ///
    /// 但 DataFusion 要求每个投影必须有唯一名称，会报错:
    /// "Projections require unique expression names"
    ///
    /// 此函数自动为重复的列添加别名:
    /// SELECT '11.3.83.3' AS _col0, 'r2api' AS _col1, 'r2api' AS _col2, trace_id FROM table
    fn fix_duplicate_projections(sql: &str) -> String {
        // 简单的正则匹配 SELECT ... FROM 部分
        // 匹配: SELECT [投影列表] FROM
        let re = match Regex::new(r"(?i)SELECT\s+(.*?)\s+FROM\s+") {
            Ok(r) => r,
            Err(_) => return sql.to_string(),
        };

        let captures = match re.captures(sql) {
            Some(c) => c,
            None => return sql.to_string(), // 没有 SELECT FROM 结构，直接返回
        };

        let projection_str = &captures[1];

        // 分割投影列（处理逗号分隔）
        // 注意：这是简化版本，不处理嵌套函数中的逗号
        let projections: Vec<&str> = projection_str.split(',').map(|s| s.trim()).collect();

        // 检测重复的列名
        use std::collections::HashMap;
        let mut column_names: HashMap<String, usize> = HashMap::new();
        let mut needs_alias = vec![false; projections.len()];

        for (idx, proj) in projections.iter().enumerate() {
            // 如果已经有 AS 别名，跳过
            if proj.to_uppercase().contains(" AS ") {
                continue;
            }

            // 提取列的"显示名称"（常量值或字段名）
            let display_name = if proj.starts_with('\'') || proj.starts_with('"') {
                // 字符串常量: '11.3.83.3' 或 "r2api"
                proj.to_string()
            } else if proj.chars().all(|c| c.is_numeric() || c == '.' || c == '-') {
                // 数字常量: 123 或 3.14
                proj.to_string()
            } else {
                // 字段名或表达式
                proj.to_string()
            };

            // 检查是否重复
            if let Some(prev_idx) = column_names.get(&display_name) {
                // 发现重复，标记当前列和之前的列都需要别名
                needs_alias[*prev_idx] = true;
                needs_alias[idx] = true;
            } else {
                column_names.insert(display_name, idx);
            }
        }

        // 如果没有重复，直接返回原SQL
        if !needs_alias.iter().any(|&x| x) {
            return sql.to_string();
        }

        // 重新构建 SELECT 子句，为需要的列添加别名
        let mut new_projections = Vec::new();
        for (idx, proj) in projections.iter().enumerate() {
            if needs_alias[idx] && !proj.to_uppercase().contains(" AS ") {
                // 添加别名 _col0, _col1, ...
                new_projections.push(format!("{} AS _col{}", proj, idx));
            } else {
                new_projections.push(proj.to_string());
            }
        }

        let new_projection_str = new_projections.join(", ");

        // 替换原SQL中的投影部分
        re.replace(sql, format!("SELECT {} FROM ", new_projection_str).as_str())
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_standard_sql() {
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 10";
        let (_statement, normalized) = SqlNormalizer::normalize(sql).unwrap();
        // AST 转换后可能格式略有不同，只检查包含关键部分
        assert!(normalized.contains("SELECT"));
        assert!(normalized.contains("FROM users"));
        assert!(normalized.contains("ORDER BY age DESC"));
        assert!(normalized.contains("LIMIT 10"));
    }

    #[test]
    fn test_convert_mysql_limit() {
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 0,10";
        let (statement, _normalized) = SqlNormalizer::normalize(sql).unwrap();

        // 验证 Statement 能够被正确解析
        // 实际的转换在 plan_analyzer 中处理
        assert!(matches!(statement, Statement::Statement(_)));
    }

    #[test]
    fn test_fix_duplicate_projections() {
        let sql = "SELECT '11.3.83.3', 'r2api', 'r2api', trace_id FROM r2api";
        let (_statement, normalized) = SqlNormalizer::normalize(sql).unwrap();

        // 检查是否添加了别名
        assert!(normalized.contains("AS _col"));
        println!("Normalized SQL: {}", normalized);
    }

    #[test]
    fn test_no_duplicate_projections() {
        let sql = "SELECT '11.3.83.3', 'r2api', trace_id FROM r2api";
        let (_statement, normalized) = SqlNormalizer::normalize(sql).unwrap();

        // 没有重复，不应该添加别名
        assert!(!normalized.contains("AS _col"));
    }

    #[test]
    fn test_convert_mysql_limit_with_space() {
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 5, 20";
        let (statement, _normalized) = SqlNormalizer::normalize(sql).unwrap();

        // 验证 Statement 能够被正确解析
        assert!(matches!(statement, Statement::Statement(_)));
    }

    #[test]
    fn test_convert_mysql_limit_zero_offset() {
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 0, 10";
        let (statement, _normalized) = SqlNormalizer::normalize(sql).unwrap();

        // 验证 Statement 能够被正确解析
        assert!(matches!(statement, Statement::Statement(_)));
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
        let (_statement, normalized) = SqlNormalizer::normalize(sql).unwrap();
        assert!(normalized.contains("LIMIT 10"));
        assert!(normalized.contains("OFFSET 5"));
    }

    #[test]
    fn test_duplicate_literal_columns() {
        // MySQL 允许: SELECT '11.3.83.3', 'r2api', 'r2api', id FROM table
        // 应该转换为: SELECT '11.3.83.3', 'r2api', 'r2api' AS col_1, id FROM table
        let sql = "SELECT '11.3.83.3', 'r2api', 'r2api', id FROM taxi_trips LIMIT 1";
        let (_statement, normalized) = SqlNormalizer::normalize(sql).unwrap();

        println!("Original: {}", sql);
        println!("Normalized: {}", normalized);

        // 验证第二个 'r2api' 被自动添加了别名
        assert!(normalized.contains("AS col_1") || normalized.contains("AS col_2"));
    }

    #[test]
    fn test_multiple_duplicate_columns() {
        // 测试多个重复的情况
        let sql = "SELECT 'a', 'a', 'a', 'b', 'b', id FROM table";
        let (_statement, normalized) = SqlNormalizer::normalize(sql).unwrap();

        println!("Original: {}", sql);
        println!("Normalized: {}", normalized);

        // 应该有多个别名
        assert!(normalized.contains("AS col_"));
    }

    #[test]
    fn test_no_duplicate_columns() {
        // 没有重复的情况应该保持不变
        let sql = "SELECT id, name, age FROM users";
        let (_statement, normalized) = SqlNormalizer::normalize(sql).unwrap();

        println!("Original: {}", sql);
        println!("Normalized: {}", normalized);

        // 不应该添加别名
        assert!(!normalized.contains("AS col_"));
    }

    #[test]
    fn test_duplicate_with_existing_alias() {
        // 测试混合情况：有些列有别名，有些重复
        let sql = "SELECT '11.3.83.3', 'r2api' AS name1, 'r2api' AS name2, id FROM table";
        let (_statement, normalized) = SqlNormalizer::normalize(sql).unwrap();

        println!("Original: {}", sql);
        println!("Normalized: {}", normalized);

        // name1 和 name2 是显式别名，应该保留
        assert!(normalized.contains("name1") || normalized.contains("name2"));
    }
}
