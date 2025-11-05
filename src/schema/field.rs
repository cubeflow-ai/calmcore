#[derive(Debug, Clone)]
pub enum FieldType {
    Keyword,
    F32,
    F64,
    I32,
    I64,
    U32,
    U64,
    // Text,
    // Boolean,
    // Date,
    // GeoPoint,
}

/// 持久化配置选项
#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub enum FieldOption {
    Keyword {
        name: String,
        index: bool,
        is_array: bool,
        persist_option: Option<PersistOption>,
        /// 是否区分大小写，默认 true（区分）。如果为 false，所有值和查询都会转为小写
        case_sensitive: bool,
    },
    I64 {
        name: String,
        index: bool,
    },
    F64 {
        name: String,
        index: bool,
    },
}

impl FieldOption {
    pub fn name(&self) -> &str {
        match self {
            FieldOption::Keyword { name, .. } => name,
            FieldOption::I64 { name, .. } => name,
            FieldOption::F64 { name, .. } => name,
        }
    }

    pub fn is_index(&self) -> bool {
        match self {
            FieldOption::Keyword { index, .. } => *index,
            FieldOption::I64 { index, .. } => *index,
            FieldOption::F64 { index, .. } => *index,
        }
    }

    pub fn is_array(&self) -> bool {
        match self {
            FieldOption::Keyword { is_array, .. } => *is_array,
            FieldOption::I64 { .. } => false,
            FieldOption::F64 { .. } => false,
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
            FieldOption::I64 { .. } => FieldType::I64,
            FieldOption::F64 { .. } => FieldType::F64,
        }
    }
}
