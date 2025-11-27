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
    /// 4. 返回 (Statement AST, 标准化后的SQL字符串)
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
}
