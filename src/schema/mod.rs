pub mod field;

use std::time::Duration;

/// Segment 持久化策略配置
#[derive(Clone, Debug)]
pub struct PersistPolicy {
    /// 文档数阈值（达到此数量触发持久化）
    pub max_docs_per_segment: u32,

    /// 时间阈值（segment 存活超过此时间触发持久化）
    pub max_segment_age: Duration,

    /// 是否在 flush 时立即检查持久化
    pub check_on_flush: bool,
}

impl Default for PersistPolicy {
    fn default() -> Self {
        Self {
            max_docs_per_segment: 100_000,             // 10万条文档
            max_segment_age: Duration::from_secs(300), // 5分钟
            check_on_flush: true,
        }
    }
}

#[derive(Clone)]
pub struct Schema {
    pub name: String,
    pub primary_key: Option<String>,
    pub store_source: bool,
    pub fields: Vec<field::FieldOption>,
    pub persist_policy: PersistPolicy,
}

impl Schema {
    pub(crate) fn add_field(&mut self, field: field::FieldOption) {
        self.fields.push(field);
    }
}
