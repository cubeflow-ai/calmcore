/// SQL 标准化器
///
/// 将MySQL特有的SQL语法转换为DataFusion支持的标准SQL语法
/// 同时验证SQL的合法性
use crate::utils::error::{CoreError, CoreResult};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::MySqlDialect;

pub struct SqlNormalizer;

impl SqlNormalizer {
    /// 标准化SQL语句
    ///
    /// 1. 验证SQL语法
    /// 2. 将MySQL特有语法转换为标准SQL
    /// 3. 返回 (Statement AST, 标准化后的SQL字符串)
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

        // 将 AST 转回 SQL 字符串，这会自动标准化格式
        let normalized = statement.to_string();

        Ok((statement, normalized))
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
