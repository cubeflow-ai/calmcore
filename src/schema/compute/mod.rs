mod partition_table_provider;

pub enum QueryScalarValue {
    Int64(i64),
    Float64(f64),
    Utf8(String),
}

impl From<&datafusion::scalar::ScalarValue> for QueryScalarValue {
    fn from(value: &datafusion::scalar::ScalarValue) -> Self {
        match value {
            datafusion::scalar::ScalarValue::Int64(Some(v)) => QueryScalarValue::Int64(*v),
            datafusion::scalar::ScalarValue::Float64(Some(v)) => QueryScalarValue::Float64(*v),
            datafusion::scalar::ScalarValue::Utf8(Some(v)) => QueryScalarValue::Utf8(v.clone()),
            _ => panic!("Unsupported ScalarValue type or null value"),
        }
    }
}
