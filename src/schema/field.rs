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

#[derive(Debug, Clone)]
pub enum FieldOption {
    Keyword {
        name: String,
        index: bool,
        is_array: bool,
    },
    I32 {
        name: String,
        index: bool,
    },
    I64 {
        name: String,
        index: bool,
    },
    U32 {
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
}

impl FieldOption {
    pub fn name(&self) -> &str {
        match self {
            FieldOption::Keyword { name, .. } => name,
            FieldOption::I32 { name, .. } => name,
            FieldOption::I64 { name, .. } => name,
            FieldOption::U32 { name, .. } => name,
            FieldOption::F32 { name, .. } => name,
            FieldOption::F64 { name, .. } => name,
        }
    }

    pub fn is_index(&self) -> bool {
        match self {
            FieldOption::Keyword { index, .. } => *index,
            FieldOption::I32 { index, .. } => *index,
            FieldOption::I64 { index, .. } => *index,
            FieldOption::U32 { index, .. } => *index,
            FieldOption::F32 { index, .. } => *index,
            FieldOption::F64 { index, .. } => *index,
        }
    }

    pub fn is_array(&self) -> bool {
        match self {
            FieldOption::Keyword { is_array, .. } => *is_array,
            FieldOption::I32 { .. } => false, // 数值类型不支持数组
            FieldOption::I64 { .. } => false,
            FieldOption::U32 { .. } => false,
            FieldOption::F32 { .. } => false,
            FieldOption::F64 { .. } => false,
        }
    }
}
