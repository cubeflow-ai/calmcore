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
    },
    I8 {
        name: String,
        index: bool,
    },
    I16 {
        name: String,
        index: bool,
    },
    I32 {
        name: String,
        index: bool,
    },
    I64 {
        name: String,
        index: bool,
    },
    U8 {
        name: String,
        index: bool,
    },
    U16 {
        name: String,
        index: bool,
    },
    U32 {
        name: String,
        index: bool,
    },
    U64 {
        name: String,
        index: bool,
    },
    F32 {
        name: String,
        index: bool,
    },
    F64 {
        name: String,
        index: bool,
    },
    Boolean {
        name: String,
        index: bool,
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
        }
    }
}
