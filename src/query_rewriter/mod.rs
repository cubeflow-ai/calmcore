/// SQL 查询改写器
///
/// 用于在查询阶段根据字段的实际类型对查询条件进行改写
/// 主要解决协议层（ES、MySQL等）传入的值与存储层类型不匹配的问题
///
/// 典型场景：
/// - ES range 查询传入日期字符串 "2025-01-11"，但字段是 i64 类型存储毫秒时间戳
/// - MySQL DATE 类型字面量需要转换为时间戳
use crate::schema::field::{FieldOption, FieldType};
use crate::schema::Schema;
use crate::utils::datetime_utils;
use serde_json::Value;

/// 查询改写器
pub struct QueryRewriter<'a> {
    schema: &'a Schema,
}

impl<'a> QueryRewriter<'a> {
    /// 创建新的查询改写器
    pub fn new(schema: &'a Schema) -> Self {
        Self { schema }
    }

    /// 获取字段的类型选项
    fn get_field_option(&self, field_name: &str) -> Option<&FieldOption> {
        self.schema.fields.iter().find(|f| f.name() == field_name)
    }

    /// 获取字段的类型
    fn get_field_type(&self, field_name: &str) -> Option<FieldType> {
        self.get_field_option(field_name).map(|f| f.field_type())
    }

    /// 改写 JSON 值，根据字段类型转换
    ///
    /// # Arguments
    /// * `field_name` - 字段名
    /// * `value` - 原始值
    ///
    /// # Returns
    /// 改写后的值，如果不需要改写则返回原值
    pub fn rewrite_value(&self, field_name: &str, value: &Value) -> Value {
        let field_type = match self.get_field_type(field_name) {
            Some(ft) => ft,
            None => return value.clone(), // 字段不存在，返回原值
        };

        match field_type {
            // 对于时间戳类型的字段，尝试将字符串日期转换为时间戳
            FieldType::I64 | FieldType::U64 => self.rewrite_timestamp_value(field_name, value),
            // 其他类型暂不需要改写
            _ => value.clone(),
        }
    }

    /// 改写时间戳值
    /// 如果是字符串且看起来像日期，则转换为毫秒时间戳
    fn rewrite_timestamp_value(&self, field_name: &str, value: &Value) -> Value {
        match value {
            Value::String(s) => {
                // 检查是否看起来像日期
                if datetime_utils::looks_like_date(s) {
                    match datetime_utils::parse_date_to_timestamp_millis(s) {
                        Ok(timestamp) => {
                            eprintln!(
                                "🔄 [QueryRewriter] Converted field '{}' from date string '{}' to timestamp {}",
                                field_name, s, timestamp
                            );
                            Value::Number(timestamp.into())
                        }
                        Err(e) => {
                            eprintln!(
                                "⚠️  [QueryRewriter] Failed to convert date '{}' for field '{}': {}",
                                s, field_name, e
                            );
                            value.clone()
                        }
                    }
                } else {
                    value.clone()
                }
            }
            Value::Number(_n) => {
                // 如果已经是数字，直接返回
                value.clone()
            }
            _ => value.clone(),
        }
    }

    /// 改写 SQL WHERE 条件中的值
    ///
    /// 将形如 "field >= '2025-01-11'" 这样的条件改写为 "field >= 1736553600000"
    ///
    /// # Arguments
    /// * `field_name` - 字段名
    /// * `sql_value` - SQL 值（可能带引号）
    ///
    /// # Returns
    /// 改写后的 SQL 值
    pub fn rewrite_sql_value(&self, field_name: &str, sql_value: &str) -> String {
        let field_type = match self.get_field_type(field_name) {
            Some(ft) => ft,
            None => return sql_value.to_string(),
        };

        match field_type {
            FieldType::I64 | FieldType::U64 => {
                // 去掉引号
                let trimmed = sql_value.trim().trim_matches('\'').trim_matches('"');

                if datetime_utils::looks_like_date(trimmed) {
                    match datetime_utils::parse_date_to_timestamp_millis(trimmed) {
                        Ok(timestamp) => {
                            eprintln!(
                                "🔄 [QueryRewriter] SQL: Converted field '{}' from '{}' to {}",
                                field_name, trimmed, timestamp
                            );
                            timestamp.to_string()
                        }
                        Err(e) => {
                            eprintln!(
                                "⚠️  [QueryRewriter] SQL: Failed to convert '{}' for field '{}': {}",
                                trimmed, field_name, e
                            );
                            sql_value.to_string()
                        }
                    }
                } else {
                    sql_value.to_string()
                }
            }
            _ => sql_value.to_string(),
        }
    }

    /// 改写范围查询的边界值
    ///
    /// # Arguments
    /// * `field_name` - 字段名
    /// * `start` - 起始值（可选）
    /// * `end` - 结束值（可选）
    ///
    /// # Returns
    /// (改写后的起始值, 改写后的结束值)
    pub fn rewrite_range_bounds(
        &self,
        field_name: &str,
        start: Option<&Value>,
        end: Option<&Value>,
    ) -> (Option<Value>, Option<Value>) {
        let rewritten_start = start.map(|v| self.rewrite_value(field_name, v));
        let rewritten_end = end.map(|v| self.rewrite_value(field_name, v));
        (rewritten_start, rewritten_end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Schema;

    fn create_test_schema() -> Schema {
        Schema {
            name: "test_table".to_string(),
            primary_key: Some("id".to_string()),
            store_source: true,
            persist_policy: Default::default(),
            fields: vec![
                FieldOption::I64 {
                    name: "updateTime".to_string(),
                    index: true,
                },
                FieldOption::I64 {
                    name: "createTime".to_string(),
                    index: true,
                },
                FieldOption::Keyword {
                    name: "name".to_string(),
                    index: true,
                    is_array: false,
                    persist_option: None,
                    case_sensitive: true,
                },
                FieldOption::I32 {
                    name: "age".to_string(),
                    index: true,
                },
            ],
        }
    }

    #[test]
    fn test_rewrite_date_string_to_timestamp() {
        let schema = create_test_schema();
        let rewriter = QueryRewriter::new(&schema);

        // 测试日期字符串转时间戳
        let value = Value::String("2025-01-11".to_string());
        let rewritten = rewriter.rewrite_value("updateTime", &value);

        assert!(rewritten.is_number());
        assert_eq!(rewritten.as_i64().unwrap(), 1736553600000);
    }

    #[test]
    fn test_rewrite_datetime_string_to_timestamp() {
        let schema = create_test_schema();
        let rewriter = QueryRewriter::new(&schema);

        // 测试日期时间字符串
        let value = Value::String("2025-01-11T12:34:56Z".to_string());
        let rewritten = rewriter.rewrite_value("updateTime", &value);

        assert!(rewritten.is_number());
        assert_eq!(rewritten.as_i64().unwrap(), 1736598896000);
    }

    #[test]
    fn test_no_rewrite_for_keyword_field() {
        let schema = create_test_schema();
        let rewriter = QueryRewriter::new(&schema);

        // Keyword 字段不应该被改写
        let value = Value::String("2025-01-11".to_string());
        let rewritten = rewriter.rewrite_value("name", &value);

        assert!(rewritten.is_string());
        assert_eq!(rewritten.as_str().unwrap(), "2025-01-11");
    }

    #[test]
    fn test_no_rewrite_for_i32_field() {
        let schema = create_test_schema();
        let rewriter = QueryRewriter::new(&schema);

        // I32 字段不应该被改写
        let value = Value::String("123".to_string());
        let rewritten = rewriter.rewrite_value("age", &value);

        assert!(rewritten.is_string());
        assert_eq!(rewritten.as_str().unwrap(), "123");
    }

    #[test]
    fn test_rewrite_already_timestamp() {
        let schema = create_test_schema();
        let rewriter = QueryRewriter::new(&schema);

        // 已经是时间戳的值不应该被改写
        let value = Value::Number(1736553600000i64.into());
        let rewritten = rewriter.rewrite_value("updateTime", &value);

        assert!(rewritten.is_number());
        assert_eq!(rewritten.as_i64().unwrap(), 1736553600000);
    }

    #[test]
    fn test_rewrite_sql_value() {
        let schema = create_test_schema();
        let rewriter = QueryRewriter::new(&schema);

        // 测试 SQL 值改写（带引号）
        let rewritten = rewriter.rewrite_sql_value("updateTime", "'2025-01-11'");
        assert_eq!(rewritten, "1736553600000");

        // 测试双引号
        let rewritten = rewriter.rewrite_sql_value("updateTime", "\"2025-01-11\"");
        assert_eq!(rewritten, "1736553600000");

        // 测试不带引号
        let rewritten = rewriter.rewrite_sql_value("updateTime", "2025-01-11");
        assert_eq!(rewritten, "1736553600000");
    }

    #[test]
    fn test_rewrite_range_bounds() {
        let schema = create_test_schema();
        let rewriter = QueryRewriter::new(&schema);

        let start = Value::String("2025-01-11".to_string());
        let end = Value::String("2025-01-12".to_string());

        let (rewritten_start, rewritten_end) =
            rewriter.rewrite_range_bounds("updateTime", Some(&start), Some(&end));

        assert!(rewritten_start.is_some());
        assert!(rewritten_end.is_some());

        assert_eq!(rewritten_start.unwrap().as_i64().unwrap(), 1736553600000);
        assert_eq!(rewritten_end.unwrap().as_i64().unwrap(), 1736640000000);
    }

    #[test]
    fn test_rewrite_unknown_field() {
        let schema = create_test_schema();
        let rewriter = QueryRewriter::new(&schema);

        // 未知字段应该返回原值
        let value = Value::String("2025-01-11".to_string());
        let rewritten = rewriter.rewrite_value("unknown_field", &value);

        assert!(rewritten.is_string());
        assert_eq!(rewritten.as_str().unwrap(), "2025-01-11");
    }
}
