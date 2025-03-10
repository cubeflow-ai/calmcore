/// Extract a column from RecordBatch and convert it to a specific Arrow array type by index
///
/// # Parameters
/// * `batch` - The RecordBatch to extract data from
/// * `col_index` - The column index
/// * `array_type` - The target Arrow array type
///
/// # Example
/// ```
/// let ids: CoreResult<UInt64Array> = data_column_index!(batch, 0, UInt64Array);
/// ```
#[macro_export]
macro_rules! data_column_index {
    ($batch:expr, $col_index:expr, $array_type:ty) => {{
        let column = $batch.column($col_index);
        match column.as_any().downcast_ref::<$array_type>() {
            Some(array) => Ok(array.clone()),
            None => Err(CoreError::Type(format!(
                "Failed to downcast column {} to {}",
                $col_index,
                stringify!($array_type)
            ))),
        }
    }};
}

/// Extract a column from RecordBatch and convert it to a specific Arrow array type by name
///
/// # Parameters
/// * `batch` - The RecordBatch to extract data from
/// * `col_name` - The column name
/// * `array_type` - The target Arrow array type
///
/// # Example
/// ```
/// let ids: CoreResult<UInt64Array> = data_column_name!(batch, "id", UInt64Array);
/// ```
#[macro_export]
macro_rules! data_column_name {
    ($batch:expr, $col_name:expr, $array_type:ty) => {{
        let schema = $batch.schema();
        let col_index = match schema.index_of($col_name) {
            Some(index) => index,
            None => {
                return Err(CoreError::Type(format!(
                    "Column {} not found in schema",
                    $col_name
                )))
            }
        };
        let column = $batch.column(col_index);
        match column.as_any().downcast_ref::<$array_type>() {
            Some(array) => Ok(array.clone()),
            None => Err(CoreError::Type(format!(
                "Failed to downcast column {} to {}",
                $col_name,
                stringify!($array_type)
            ))),
        }
    }};
}
