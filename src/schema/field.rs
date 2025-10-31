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
    // Text(Arc<TextField>),
    // Integer(Arc<IntegerField>),
    // Float(Arc<FloatField>),
    // Boolean(Arc<BooleanField>),
    // Date(Arc<DateField>),
    // GeoPoint(Arc<GeoPointField>),
}

impl FieldOption {
    pub fn name(&self) -> &str {
        match self {
            FieldOption::Keyword { name, .. } => name,
            // FieldOption::Text(field) => &field.name,
            // FieldOption::Integer(field) => &field.name,
            // FieldOption::Float(field) => &field.name,
            // FieldOption::Boolean(field) => &field.name,
            // FieldOption::Date(field) => &field.name,
            // FieldOption::GeoPoint(field) => &field.name,
        }
    }

    pub fn is_index(&self) -> bool {
        match self {
            FieldOption::Keyword { index, .. } => *index,
            // FieldOption::Text(field) => field.index,
            // FieldOption::Integer(field) => field.index,
            // FieldOption::Float(field) => field.index,
            // FieldOption::Boolean(field) => field.index,
            // FieldOption::Date(field) => field.index,
            // FieldOption::GeoPoint(field) => field.index,
        }
    }

    pub fn is_array(&self) -> bool {
        match self {
            FieldOption::Keyword { is_array, .. } => *is_array,
            // FieldOption::Text(field) => field.is_array,
            // FieldOption::Integer(field) => field.is_array,
            // FieldOption::Float(field) => field.is_array,
            // FieldOption::Boolean(field) => field.is_array,
            // FieldOption::Date(field) => field.is_array,
            // FieldOption::GeoPoint(field) => field.is_array,
        }
    }
}
