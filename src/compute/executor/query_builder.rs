use std::sync::Arc;

use crate::engine::Engine;
use crate::utils::error::CoreResult;

/// SQL 查询构建和解析工具
pub struct QueryBuilder {
    engine: Arc<Engine>,
}

impl QueryBuilder {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    /// 提取 SQL 中的表名
    pub fn extract_table_name(&self, sql: &str) -> CoreResult<String> {
        let query_lower = sql.to_lowercase();
        let table_names = self.engine.list_tables();

        // 按表名长度降序排列，优先匹配更长的表名
        // 这样 label_event_v1 会在 label_event_v 之前被检查
        let mut sorted_tables = table_names.clone();
        sorted_tables.sort_by_key(|b| std::cmp::Reverse(b.len()));

        for table_name in &sorted_tables {
            let table_lower = table_name.to_lowercase();

            // 使用正则表达式或简单的边界检查来确保精确匹配
            // 检查 "from table_name" 模式，确保后面跟着空格、换行或结束
            if let Some(pos) = query_lower.find(&format!("from {}", table_lower)) {
                let after_pos = pos + "from ".len() + table_lower.len();
                if after_pos >= query_lower.len() {
                    // 表名在查询末尾
                    return Ok(table_name.clone());
                }
                // 检查后面的字符是否是分隔符
                let next_char = query_lower.chars().nth(after_pos);
                if matches!(
                    next_char,
                    Some(' ') | Some('\t') | Some('\n') | Some('\r') | Some(';') | Some(',')
                ) {
                    return Ok(table_name.clone());
                }
            }

            // 检查带反引号的情况
            if query_lower.contains(&format!("from `{}`", table_lower)) {
                return Ok(table_name.clone());
            }
        }

        Err(crate::utils::error::CoreError::InvalidParam(format!(
            "Table not found in query. Available tables: {}",
            table_names.join(", ")
        )))
    }
}
