use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    Keyword,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    Boolean,
    Timestamp, // Unix 时间戳(毫秒),底层存储为 i64
}

/// 持久化配置选项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistOption {
    /// Zstd 压缩级别 (1-22, 默认 3)
    pub zstd_level: i32,
    /// B-tree chunk 大小 (默认 256)
    pub chunk_size: usize,
}

impl PersistOption {
    /// 创建默认配置
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Self {
        Self {
            zstd_level: 3,
            chunk_size: 256,
        }
    }

    /// 创建自定义配置
    pub fn new(zstd_level: i32, chunk_size: usize) -> Self {
        Self {
            zstd_level,
            chunk_size,
        }
    }
}

impl Default for PersistOption {
    fn default() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldOption {
    Keyword {
        name: String,
        index: bool,
        is_array: bool,
        persist_option: Option<PersistOption>,
        /// 是否区分大小写，默认 true（区分）。如果为 false，所有值和查询都会转为小写
        case_sensitive: bool,
        /// 字段描述/注释
        description: Option<String>,
        /// 默认值（JSON 字符串格式）
        default_value: Option<String>,
        /// 是否可为空，默认 true
        nullable: bool,
    },
    I8 {
        name: String,
        index: bool,
        description: Option<String>,
        default_value: Option<String>,
        nullable: bool,
    },
    I16 {
        name: String,
        index: bool,
        description: Option<String>,
        default_value: Option<String>,
        nullable: bool,
    },
    I32 {
        name: String,
        index: bool,
        description: Option<String>,
        default_value: Option<String>,
        nullable: bool,
    },
    I64 {
        name: String,
        index: bool,
        description: Option<String>,
        default_value: Option<String>,
        nullable: bool,
    },
    U8 {
        name: String,
        index: bool,
        description: Option<String>,
        default_value: Option<String>,
        nullable: bool,
    },
    U16 {
        name: String,
        index: bool,
        description: Option<String>,
        default_value: Option<String>,
        nullable: bool,
    },
    U32 {
        name: String,
        index: bool,
        description: Option<String>,
        default_value: Option<String>,
        nullable: bool,
    },
    U64 {
        name: String,
        index: bool,
        description: Option<String>,
        default_value: Option<String>,
        nullable: bool,
    },
    F32 {
        name: String,
        index: bool,
        description: Option<String>,
        default_value: Option<String>,
        nullable: bool,
    },
    F64 {
        name: String,
        index: bool,
        description: Option<String>,
        default_value: Option<String>,
        nullable: bool,
    },
    Boolean {
        name: String,
        index: bool,
        description: Option<String>,
        default_value: Option<String>,
        nullable: bool,
    },
    Timestamp {
        name: String,
        index: bool,
        /// 输入/输出时间格式,支持:
        /// - "iso8601": ISO 8601 格式 (2024-01-01T10:00:00Z)
        /// - "rfc3339": RFC 3339 格式
        /// - 自定义格式如 "yyyy-MM-dd HH:mm:ss"
        /// None 表示只接受数值型时间戳
        format: Option<String>,
        description: Option<String>,
        default_value: Option<String>,
        nullable: bool,
    },
}

impl FieldOption {
    pub fn name(&self) -> &str {
        match self {
            FieldOption::Keyword { name, .. } => name,
            FieldOption::I8 { name, .. } => name,
            FieldOption::I16 { name, .. } => name,
            FieldOption::I32 { name, .. } => name,
            FieldOption::I64 { name, .. } => name,
            FieldOption::U8 { name, .. } => name,
            FieldOption::U16 { name, .. } => name,
            FieldOption::U32 { name, .. } => name,
            FieldOption::U64 { name, .. } => name,
            FieldOption::F32 { name, .. } => name,
            FieldOption::F64 { name, .. } => name,
            FieldOption::Boolean { name, .. } => name,
            FieldOption::Timestamp { name, .. } => name,
        }
    }

    /// 将字段名规范化为小写（用于内部统一处理）
    pub fn normalize_name(&mut self) {
        match self {
            FieldOption::Keyword { name, .. } => *name = name.to_lowercase(),
            FieldOption::I8 { name, .. } => *name = name.to_lowercase(),
            FieldOption::I16 { name, .. } => *name = name.to_lowercase(),
            FieldOption::I32 { name, .. } => *name = name.to_lowercase(),
            FieldOption::I64 { name, .. } => *name = name.to_lowercase(),
            FieldOption::U8 { name, .. } => *name = name.to_lowercase(),
            FieldOption::U16 { name, .. } => *name = name.to_lowercase(),
            FieldOption::U32 { name, .. } => *name = name.to_lowercase(),
            FieldOption::U64 { name, .. } => *name = name.to_lowercase(),
            FieldOption::F32 { name, .. } => *name = name.to_lowercase(),
            FieldOption::F64 { name, .. } => *name = name.to_lowercase(),
            FieldOption::Boolean { name, .. } => *name = name.to_lowercase(),
            FieldOption::Timestamp { name, .. } => *name = name.to_lowercase(),
        }
    }

    pub fn is_index(&self) -> bool {
        match self {
            FieldOption::Keyword { index, .. } => *index,
            FieldOption::I8 { index, .. } => *index,
            FieldOption::I16 { index, .. } => *index,
            FieldOption::I32 { index, .. } => *index,
            FieldOption::I64 { index, .. } => *index,
            FieldOption::U8 { index, .. } => *index,
            FieldOption::U16 { index, .. } => *index,
            FieldOption::U32 { index, .. } => *index,
            FieldOption::U64 { index, .. } => *index,
            FieldOption::F32 { index, .. } => *index,
            FieldOption::F64 { index, .. } => *index,
            FieldOption::Boolean { index, .. } => *index,
            FieldOption::Timestamp { index, .. } => *index,
        }
    }

    pub fn is_array(&self) -> bool {
        match self {
            FieldOption::Keyword { is_array, .. } => *is_array,
            FieldOption::I8 { .. } => false,
            FieldOption::I16 { .. } => false,
            FieldOption::I32 { .. } => false,
            FieldOption::I64 { .. } => false,
            FieldOption::U8 { .. } => false,
            FieldOption::U16 { .. } => false,
            FieldOption::U32 { .. } => false,
            FieldOption::U64 { .. } => false,
            FieldOption::F32 { .. } => false,
            FieldOption::F64 { .. } => false,
            FieldOption::Boolean { .. } => false,
            FieldOption::Timestamp { .. } => false,
        }
    }

    /// 获取持久化配置，如果未设置则返回默认值
    pub fn persist_option(&self) -> PersistOption {
        match self {
            FieldOption::Keyword { persist_option, .. } => persist_option
                .clone()
                .unwrap_or_else(PersistOption::default),
            _ => PersistOption::default(),
        }
    }

    /// 获取 zstd 压缩级别
    pub fn zstd_level(&self) -> i32 {
        self.persist_option().zstd_level
    }

    /// 获取 chunk size
    pub fn chunk_size(&self) -> usize {
        self.persist_option().chunk_size
    }

    /// 获取是否区分大小写（仅 Keyword 类型有效）
    pub fn case_sensitive(&self) -> bool {
        match self {
            FieldOption::Keyword { case_sensitive, .. } => *case_sensitive,
            _ => true, // 其他类型默认区分大小写
        }
    }

    /// 获取字段类型
    pub fn field_type(&self) -> FieldType {
        match self {
            FieldOption::Keyword { .. } => FieldType::Keyword,
            FieldOption::I8 { .. } => FieldType::I8,
            FieldOption::I16 { .. } => FieldType::I16,
            FieldOption::I32 { .. } => FieldType::I32,
            FieldOption::I64 { .. } => FieldType::I64,
            FieldOption::U8 { .. } => FieldType::U8,
            FieldOption::U16 { .. } => FieldType::U16,
            FieldOption::U32 { .. } => FieldType::U32,
            FieldOption::U64 { .. } => FieldType::U64,
            FieldOption::F32 { .. } => FieldType::F32,
            FieldOption::F64 { .. } => FieldType::F64,
            FieldOption::Boolean { .. } => FieldType::Boolean,
            FieldOption::Timestamp { .. } => FieldType::Timestamp,
        }
    }

    /// 获取时间格式(仅 Timestamp 类型有效)
    pub fn timestamp_format(&self) -> Option<&str> {
        match self {
            FieldOption::Timestamp { format, .. } => format.as_deref(),
            _ => None,
        }
    }

    /// 获取字段描述
    pub fn description(&self) -> Option<&str> {
        match self {
            FieldOption::Keyword { description, .. } => description.as_deref(),
            FieldOption::I8 { description, .. } => description.as_deref(),
            FieldOption::I16 { description, .. } => description.as_deref(),
            FieldOption::I32 { description, .. } => description.as_deref(),
            FieldOption::I64 { description, .. } => description.as_deref(),
            FieldOption::U8 { description, .. } => description.as_deref(),
            FieldOption::U16 { description, .. } => description.as_deref(),
            FieldOption::U32 { description, .. } => description.as_deref(),
            FieldOption::U64 { description, .. } => description.as_deref(),
            FieldOption::F32 { description, .. } => description.as_deref(),
            FieldOption::F64 { description, .. } => description.as_deref(),
            FieldOption::Boolean { description, .. } => description.as_deref(),
            FieldOption::Timestamp { description, .. } => description.as_deref(),
        }
    }

    /// 获取默认值
    pub fn default_value(&self) -> Option<&str> {
        match self {
            FieldOption::Keyword { default_value, .. } => default_value.as_deref(),
            FieldOption::I8 { default_value, .. } => default_value.as_deref(),
            FieldOption::I16 { default_value, .. } => default_value.as_deref(),
            FieldOption::I32 { default_value, .. } => default_value.as_deref(),
            FieldOption::I64 { default_value, .. } => default_value.as_deref(),
            FieldOption::U8 { default_value, .. } => default_value.as_deref(),
            FieldOption::U16 { default_value, .. } => default_value.as_deref(),
            FieldOption::U32 { default_value, .. } => default_value.as_deref(),
            FieldOption::U64 { default_value, .. } => default_value.as_deref(),
            FieldOption::F32 { default_value, .. } => default_value.as_deref(),
            FieldOption::F64 { default_value, .. } => default_value.as_deref(),
            FieldOption::Boolean { default_value, .. } => default_value.as_deref(),
            FieldOption::Timestamp { default_value, .. } => default_value.as_deref(),
        }
    }

    /// 获取是否可为空
    pub fn nullable(&self) -> bool {
        match self {
            FieldOption::Keyword { nullable, .. } => *nullable,
            FieldOption::I8 { nullable, .. } => *nullable,
            FieldOption::I16 { nullable, .. } => *nullable,
            FieldOption::I32 { nullable, .. } => *nullable,
            FieldOption::I64 { nullable, .. } => *nullable,
            FieldOption::U8 { nullable, .. } => *nullable,
            FieldOption::U16 { nullable, .. } => *nullable,
            FieldOption::U32 { nullable, .. } => *nullable,
            FieldOption::U64 { nullable, .. } => *nullable,
            FieldOption::F32 { nullable, .. } => *nullable,
            FieldOption::F64 { nullable, .. } => *nullable,
            FieldOption::Boolean { nullable, .. } => *nullable,
            FieldOption::Timestamp { nullable, .. } => *nullable,
        }
    }
}
