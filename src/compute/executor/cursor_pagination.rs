/// 游标分页支持
///
/// 解决深度分页（Deep Pagination）问题
/// 传统 OFFSET 分页：LIMIT 1000 OFFSET 10000 需要读取 11000 行
/// 游标分页：WHERE id > last_id LIMIT 1000 只需要读取 1000 行

use crate::utils::error::CoreResult;

/// 游标信息
#[derive(Debug, Clone)]
pub struct CursorInfo {
    /// 游标字段名（通常是主键或时间戳）
    pub field_name: String,
    /// 游标值（上次查询的最后一个值）
    pub last_value: String,
    /// 排序方向
    pub ascending: bool,
}

/// 游标分页工具
pub struct CursorPagination;

impl CursorPagination {
    /// 将传统的 OFFSET 分页 SQL 转换为游标分页 SQL
    ///
    /// 例如：
    /// ```sql
    /// -- 传统方式（慢）
    /// SELECT * FROM t ORDER BY id LIMIT 1000 OFFSET 10000
    ///
    /// -- 游标方式（快）
    /// SELECT * FROM t WHERE id > last_id ORDER BY id LIMIT 1000
    /// ```
    pub fn convert_to_cursor_sql(
        sql: &str,
        cursor: Option<&CursorInfo>,
    ) -> CoreResult<String> {
        // 如果没有游标，返回原 SQL
        let cursor = match cursor {
            Some(c) => c,
            None => return Ok(sql.to_string()),
        };

        // 解析 SQL 并添加游标条件
        let sql_upper = sql.to_uppercase();

        // 查找 WHERE 子句
        if let Some(where_pos) = sql_upper.find("WHERE") {
            // 已有 WHERE，添加 AND 条件
            let before_where = &sql[..where_pos + 5]; // 包含 "WHERE"
            let after_where = &sql[where_pos + 5..];

            // 查找 ORDER BY 位置
            let order_pos = after_where.to_uppercase().find("ORDER BY");

            let (where_part, rest) = if let Some(pos) = order_pos {
                (&after_where[..pos], &after_where[pos..])
            } else {
                (after_where, "")
            };

            let operator = if cursor.ascending { ">" } else { "<" };
            let cursor_condition = format!(" AND {} {} '{}'", cursor.field_name, operator, cursor.last_value);

            Ok(format!(
                "{}{}{}{}",
                before_where, where_part, cursor_condition, rest
            ))
        } else {
            // 没有 WHERE，添加新的 WHERE 子句
            // 查找 FROM table_name 后的位置
            if let Some(from_pos) = sql_upper.find("FROM") {
                // 查找 FROM 后的第一个空格或关键字
                let after_from = &sql[from_pos + 4..].trim();
                let parts: Vec<&str> = after_from.split_whitespace().collect();

                if parts.is_empty() {
                    return Ok(sql.to_string());
                }

                let table_name = parts[0];
                let after_table = &after_from[table_name.len()..];

                let operator = if cursor.ascending { ">" } else { "<" };
                let cursor_condition = format!(" WHERE {} {} '{}'", cursor.field_name, operator, cursor.last_value);

                Ok(format!(
                    "{}FROM {}{}{}",
                    &sql[..from_pos],
                    table_name,
                    cursor_condition,
                    after_table
                ))
            } else {
                Ok(sql.to_string())
            }
        }
    }

    /// 从结果中提取下一个游标值
    ///
    /// # 参数
    /// * `batch` - 查询结果
    /// * `field_name` - 游标字段名
    ///
    /// # 返回
    /// 最后一行的游标字段值（用于下次查询）
    pub fn extract_next_cursor(
        batch: &datafusion::arrow::record_batch::RecordBatch,
        field_name: &str,
    ) -> Option<String> {
        if batch.num_rows() == 0 {
            return None;
        }

        // 查找字段索引
        let schema = batch.schema();
        let field_index = schema.fields().iter().position(|f| f.name() == field_name)?;

        // 获取最后一行的值
        let column = batch.column(field_index);
        let last_row_index = batch.num_rows() - 1;

        // 根据数据类型提取值
        use datafusion::arrow::array::*;
        use datafusion::arrow::datatypes::DataType;

        match schema.field(field_index).data_type() {
            DataType::Int64 => {
                let array = column.as_any().downcast_ref::<Int64Array>()?;
                Some(array.value(last_row_index).to_string())
            }
            DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>()?;
                Some(array.value(last_row_index).to_string())
            }
            DataType::Timestamp(_, _) => {
                let array = column.as_any().downcast_ref::<TimestampNanosecondArray>()?;
                Some(array.value(last_row_index).to_string())
            }
            _ => None,
        }
    }

    /// 移除 SQL 中的 OFFSET 子句
    ///
    /// 游标分页不需要 OFFSET
    pub fn remove_offset(sql: &str) -> String {
        let sql_lower = sql.to_lowercase();

        // 查找 OFFSET 关键字
        if let Some(offset_pos) = sql_lower.find(" offset ") {
            // 查找 OFFSET 后的数字和后续的空格或关键字
            let after_offset = &sql[offset_pos + 8..]; // " offset " 长度为 8
            let parts: Vec<&str> = after_offset.split_whitespace().collect();

            if parts.len() >= 1 {
                // 移除 OFFSET 及其数字
                let before_offset = &sql[..offset_pos];
                let after_number = &after_offset[parts[0].len()..];
                return format!("{}{}", before_offset, after_number);
            }
        }

        sql.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_to_cursor_sql() {
        let sql = "SELECT * FROM t ORDER BY id LIMIT 1000";
        let cursor = CursorInfo {
            field_name: "id".to_string(),
            last_value: "12345".to_string(),
            ascending: true,
        };

        let result = CursorPagination::convert_to_cursor_sql(sql, Some(&cursor)).unwrap();
        assert!(result.contains("WHERE id > '12345'"));
    }

    #[test]
    fn test_remove_offset() {
        let sql = "SELECT * FROM t LIMIT 1000 OFFSET 500";
        let result = CursorPagination::remove_offset(sql);
        assert!(!result.contains("OFFSET"));
        assert!(result.contains("LIMIT 1000"));
    }
}
