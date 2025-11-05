pub mod compute;
pub mod field;

use std::time::Duration;

/// Segment 持久化策略配置
#[derive(Clone, Debug)]
pub struct PersistPolicy {
    /// 文档数阈值（达到此数量触发持久化）
    pub max_docs_per_segment: u32,

    /// 时间阈值（segment 存活超过此时间触发持久化）
    pub max_segment_age: Duration,
}

impl Default for PersistPolicy {
    fn default() -> Self {
        Self {
            max_docs_per_segment: 100_000,             // 10万条文档
            max_segment_age: Duration::from_secs(300), // 5分钟
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

    /// 将当前 Schema 转换为 Arrow Schema
    pub fn to_arrow_schema(&self) -> std::sync::Arc<datafusion::arrow::datatypes::Schema> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;

        let mut arrow_fields = Vec::new();
        arrow_fields.push(Field::new(field.name(), data_type, nullable));

        for field in &self.fields {
            let (data_type, nullable) = match field {
                field::FieldOption::Keyword { is_array, .. } => {
                    if *is_array {
                        (
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                            true,
                        )
                    } else {
                        (DataType::Utf8, true)
                    }
                }
                field::FieldOption::I64 { .. } => (DataType::Int64, true),
                field::FieldOption::F64 { .. } => (DataType::Float64, true),
            };

            arrow_fields.push(Field::new(field.name(), data_type, nullable));
        }

        Arc::new(ArrowSchema::new(arrow_fields))
    }
}
