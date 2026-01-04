pub mod field;

use serde::{Deserialize, Serialize};
use std::time::Duration;

mod intrnal_field {
    pub const DOC_ID_FIELD: &str = "_internal_id";
    pub const PARTITION_FIELD: &str = "_partition";
}

pub use intrnal_field::{DOC_ID_FIELD, PARTITION_FIELD};

/// Segment 持久化策略配置
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistPolicy {
    /// 文档数阈值（达到此数量触发持久化）
    pub max_docs_per_segment: u32,

    /// 时间阈值（segment 存活超过此时间触发持久化）
    #[serde(with = "serde_duration")]
    pub max_segment_age: Duration,
}

mod serde_duration {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        duration.as_secs().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

impl Default for PersistPolicy {
    fn default() -> Self {
        Self {
            max_docs_per_segment: 100_000,             // 10万条文档
            max_segment_age: Duration::from_secs(300), // 5分钟
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub name: String,
    pub primary_key: Option<String>,
    pub store_source: bool,
    pub fields: Vec<field::FieldOption>,
    pub persist_policy: PersistPolicy,
    /// 表描述/注释
    pub description: Option<String>,
}

impl Schema {
    /// 创建新的 Schema，并自动规范化所有字段名为小写
    pub fn new(
        name: String,
        primary_key: Option<String>,
        store_source: bool,
        mut fields: Vec<field::FieldOption>,
        persist_policy: PersistPolicy,
        description: Option<String>,
    ) -> Self {
        // 规范化所有字段名为小写
        for field in &mut fields {
            field.normalize_name();
        }
        Schema {
            name,
            primary_key,
            store_source,
            fields,
            persist_policy,
            description,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn add_field(&mut self, mut field: field::FieldOption) {
        field.normalize_name(); // 统一转换为小写
        self.fields.push(field);
    }

    /// 将当前 Schema 转换为 Arrow Schema
    pub fn to_arrow_schema(&self) -> std::sync::Arc<datafusion::arrow::datatypes::Schema> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;

        let mut arrow_fields = Vec::new();

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
                field::FieldOption::Fulltext { is_array, .. } => {
                    if *is_array {
                        (
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                            true,
                        )
                    } else {
                        (DataType::Utf8, true)
                    }
                }
                field::FieldOption::I8 { .. } => (DataType::Int8, true),
                field::FieldOption::I16 { .. } => (DataType::Int16, true),
                field::FieldOption::I32 { .. } => (DataType::Int32, true),
                field::FieldOption::I64 { .. } => (DataType::Int64, true),
                field::FieldOption::U8 { .. } => (DataType::UInt8, true),
                field::FieldOption::U16 { .. } => (DataType::UInt16, true),
                field::FieldOption::U32 { .. } => (DataType::UInt32, true),
                field::FieldOption::U64 { .. } => (DataType::UInt64, true),
                field::FieldOption::F32 { .. } => (DataType::Float32, true),
                field::FieldOption::F64 { .. } => (DataType::Float64, true),
                field::FieldOption::Boolean { .. } => (DataType::Boolean, true),
                field::FieldOption::Timestamp { .. } => (
                    DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                    true,
                ),
            };

            // Convert field name to lowercase for case-insensitive SQL queries
            let field_name_lower = field.name().to_lowercase();
            arrow_fields.push(Field::new(field_name_lower, data_type, nullable));
        }

        Arc::new(ArrowSchema::new(arrow_fields))
    }
}
