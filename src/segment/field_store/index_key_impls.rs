/// IndexKey trait 的各种类型实现
///
/// 这个文件实现了 String, i64, u64, u32, i32, f64 等类型的 IndexKey trait
use super::generic_index::IndexKey;
use super::*;
use datafusion::arrow::array::*;

// ============================================================================
// String (Keyword) 实现
// ============================================================================

impl IndexKey for String {
    type Serializer = StringRoaringSerializer;

    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        use datafusion::arrow::array::Array;

        if let Some(string_array) = arr.as_any().downcast_ref::<StringArray>() {
            Box::new(
                string_array
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, opt)| opt.map(|s| (idx, s.to_string()))),
            )
        } else if let Some(list_array) = arr.as_any().downcast_ref::<ListArray>() {
            // 数组字段支持
            Box::new(
                list_array
                    .iter()
                    .enumerate()
                    .flat_map(|(row_idx, opt_list)| {
                        opt_list
                            .and_then(|list| {
                                list.as_any().downcast_ref::<StringArray>().map(|arr| {
                                    arr.iter()
                                        .filter_map(|s| s.map(|s| s.to_string()))
                                        .collect::<Vec<_>>()
                                })
                            })
                            .into_iter()
                            .flatten()
                            .map(move |s| (row_idx, s))
                    }),
            )
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn from_scalar(value: &datafusion::scalar::ScalarValue) -> Option<Self> {
        use datafusion::scalar::ScalarValue;

        match value {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
            _ => None,
        }
    }

    fn new_serializer(zstd_level: i32) -> Self::Serializer {
        StringRoaringSerializer::new(zstd_level)
    }

    fn key_len() -> usize {
        0 // 可变长度
    }

    fn normalize(&self, case_sensitive: bool) -> Self {
        if case_sensitive {
            self.clone()
        } else {
            self.to_lowercase()
        }
    }

    fn supports_range() -> bool {
        true // 支持字符串范围查询（字典序）
    }
}

// ============================================================================
// i64 实现
// ============================================================================

impl IndexKey for i64 {
    type Serializer = I64RoaringSerializer;

    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        use crate::arrow_downcast;

        Box::new(
            arrow_downcast!(arr, Int64Array)
                .iter()
                .enumerate()
                .filter_map(|(idx, opt)| opt.map(|v| (idx, v))),
        )
    }

    fn from_scalar(value: &datafusion::scalar::ScalarValue) -> Option<Self> {
        use datafusion::scalar::ScalarValue;

        match value {
            ScalarValue::Int64(Some(v)) => Some(*v),
            _ => None,
        }
    }

    fn new_serializer(zstd_level: i32) -> Self::Serializer {
        I64RoaringSerializer::new(zstd_level)
    }

    fn key_len() -> usize {
        8
    }

    fn normalize(&self, _case_sensitive: bool) -> Self {
        *self
    }
}

// ============================================================================
// u64 实现
// ============================================================================

impl IndexKey for u64 {
    type Serializer = U64RoaringSerializer;

    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        use crate::arrow_downcast;

        Box::new(
            arrow_downcast!(arr, UInt64Array)
                .iter()
                .enumerate()
                .filter_map(|(idx, opt)| opt.map(|v| (idx, v))),
        )
    }

    fn from_scalar(value: &datafusion::scalar::ScalarValue) -> Option<Self> {
        use datafusion::scalar::ScalarValue;

        match value {
            ScalarValue::UInt64(Some(v)) => Some(*v),
            _ => None,
        }
    }

    fn new_serializer(zstd_level: i32) -> Self::Serializer {
        U64RoaringSerializer::new(zstd_level)
    }

    fn key_len() -> usize {
        8
    }

    fn normalize(&self, _case_sensitive: bool) -> Self {
        *self
    }
}

// ============================================================================
// u32 实现
// ============================================================================

impl IndexKey for u32 {
    type Serializer = U32RoaringSerializer;

    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        use crate::arrow_downcast;

        Box::new(
            arrow_downcast!(arr, UInt32Array)
                .iter()
                .enumerate()
                .filter_map(|(idx, opt)| opt.map(|v| (idx, v))),
        )
    }

    fn from_scalar(value: &datafusion::scalar::ScalarValue) -> Option<Self> {
        use datafusion::scalar::ScalarValue;

        match value {
            ScalarValue::UInt32(Some(v)) => Some(*v),
            _ => None,
        }
    }

    fn new_serializer(zstd_level: i32) -> Self::Serializer {
        U32RoaringSerializer::new(zstd_level)
    }

    fn key_len() -> usize {
        4
    }

    fn normalize(&self, _case_sensitive: bool) -> Self {
        *self
    }
}

// ============================================================================
// i32 实现
// ============================================================================

impl IndexKey for i32 {
    type Serializer = I32RoaringSerializer;

    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        use crate::arrow_downcast;

        Box::new(
            arrow_downcast!(arr, Int32Array)
                .iter()
                .enumerate()
                .filter_map(|(idx, opt)| opt.map(|v| (idx, v))),
        )
    }

    fn from_scalar(value: &datafusion::scalar::ScalarValue) -> Option<Self> {
        use datafusion::scalar::ScalarValue;

        match value {
            ScalarValue::Int32(Some(v)) => Some(*v),
            _ => None,
        }
    }

    fn new_serializer(zstd_level: i32) -> Self::Serializer {
        I32RoaringSerializer::new(zstd_level)
    }

    fn key_len() -> usize {
        4
    }

    fn normalize(&self, _case_sensitive: bool) -> Self {
        *self
    }
}

// ============================================================================
// f64 实现 (使用 OrderedF64 包装)
// ============================================================================

// 使用 OrderedF64 类型（从 ordered_f64 模块导入）
pub use super::ordered_f64::OrderedF64;

impl IndexKey for OrderedF64 {
    type Serializer = F64RoaringSerializer;

    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        use crate::arrow_downcast;

        Box::new(
            arrow_downcast!(arr, Float64Array)
                .iter()
                .enumerate()
                .filter_map(|(idx, opt)| opt.map(|v| (idx, OrderedF64::from(v)))),
        )
    }

    fn from_scalar(value: &datafusion::scalar::ScalarValue) -> Option<Self> {
        use datafusion::scalar::ScalarValue;

        match value {
            ScalarValue::Float64(Some(v)) => Some(OrderedF64::from(*v)),
            _ => None,
        }
    }

    fn new_serializer(zstd_level: i32) -> Self::Serializer {
        F64RoaringSerializer::new(zstd_level)
    }

    fn key_len() -> usize {
        8
    }

    fn normalize(&self, _case_sensitive: bool) -> Self {
        *self
    }

    fn supports_range() -> bool {
        true
    }
}

// ============================================================================
// f32 实现 (使用 OrderedF32 包装)
// ============================================================================

pub use super::ordered_f32::OrderedF32;

impl IndexKey for OrderedF32 {
    type Serializer = F32RoaringSerializer;

    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        use crate::arrow_downcast;

        Box::new(
            arrow_downcast!(arr, Float32Array)
                .iter()
                .enumerate()
                .filter_map(|(idx, opt)| opt.map(|v| (idx, OrderedF32::from(v)))),
        )
    }

    fn from_scalar(scalar: &ScalarValue) -> Option<Self> {
        use datafusion::scalar::ScalarValue;

        match scalar {
            ScalarValue::Float32(Some(v)) => Some(OrderedF32::from(*v)),
            _ => None,
        }
    }

    fn new_serializer(zstd_level: i32) -> Self::Serializer {
        F32RoaringSerializer::new(zstd_level)
    }

    fn key_len() -> usize {
        4
    }

    fn normalize(&self, _case_sensitive: bool) -> Self {
        *self
    }

    fn supports_range() -> bool {
        true
    }
}

// ============================================================================
// u8 实现
// ============================================================================

impl IndexKey for u8 {
    type Serializer = U8RoaringSerializer;

    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        use crate::arrow_downcast;

        Box::new(
            arrow_downcast!(arr, UInt8Array)
                .iter()
                .enumerate()
                .filter_map(|(idx, opt)| opt.map(|v| (idx, v))),
        )
    }

    fn from_scalar(scalar: &ScalarValue) -> Option<Self> {
        use datafusion::scalar::ScalarValue;

        match scalar {
            ScalarValue::UInt8(Some(v)) => Some(*v),
            _ => None,
        }
    }

    fn new_serializer(zstd_level: i32) -> Self::Serializer {
        U8RoaringSerializer::new(zstd_level)
    }

    fn key_len() -> usize {
        1
    }

    fn normalize(&self, _case_sensitive: bool) -> Self {
        *self
    }
}

// ============================================================================
// i8 实现
// ============================================================================

impl IndexKey for i8 {
    type Serializer = I8RoaringSerializer;

    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        use crate::arrow_downcast;

        Box::new(
            arrow_downcast!(arr, Int8Array)
                .iter()
                .enumerate()
                .filter_map(|(idx, opt)| opt.map(|v| (idx, v))),
        )
    }

    fn from_scalar(scalar: &ScalarValue) -> Option<Self> {
        use datafusion::scalar::ScalarValue;

        match scalar {
            ScalarValue::Int8(Some(v)) => Some(*v),
            _ => None,
        }
    }

    fn new_serializer(zstd_level: i32) -> Self::Serializer {
        I8RoaringSerializer::new(zstd_level)
    }

    fn key_len() -> usize {
        1
    }

    fn normalize(&self, _case_sensitive: bool) -> Self {
        *self
    }
}

// ============================================================================
// u16 实现
// ============================================================================

impl IndexKey for u16 {
    type Serializer = U16RoaringSerializer;

    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        use crate::arrow_downcast;

        Box::new(
            arrow_downcast!(arr, UInt16Array)
                .iter()
                .enumerate()
                .filter_map(|(idx, opt)| opt.map(|v| (idx, v))),
        )
    }

    fn from_scalar(scalar: &ScalarValue) -> Option<Self> {
        use datafusion::scalar::ScalarValue;

        match scalar {
            ScalarValue::UInt16(Some(v)) => Some(*v),
            _ => None,
        }
    }

    fn new_serializer(zstd_level: i32) -> Self::Serializer {
        U16RoaringSerializer::new(zstd_level)
    }

    fn key_len() -> usize {
        2
    }

    fn normalize(&self, _case_sensitive: bool) -> Self {
        *self
    }
}

// ============================================================================
// i16 实现
// ============================================================================

impl IndexKey for i16 {
    type Serializer = I16RoaringSerializer;

    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        use crate::arrow_downcast;

        Box::new(
            arrow_downcast!(arr, Int16Array)
                .iter()
                .enumerate()
                .filter_map(|(idx, opt)| opt.map(|v| (idx, v))),
        )
    }

    fn from_scalar(scalar: &ScalarValue) -> Option<Self> {
        use datafusion::scalar::ScalarValue;

        match scalar {
            ScalarValue::Int16(Some(v)) => Some(*v),
            _ => None,
        }
    }

    fn new_serializer(zstd_level: i32) -> Self::Serializer {
        I16RoaringSerializer::new(zstd_level)
    }

    fn key_len() -> usize {
        2
    }

    fn normalize(&self, _case_sensitive: bool) -> Self {
        *self
    }
}

// ============================================================================
// bool 实现
// ============================================================================

impl IndexKey for bool {
    type Serializer = BooleanRoaringSerializer;

    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        use crate::arrow_downcast;
        use datafusion::arrow::array::BooleanArray;

        Box::new(
            arrow_downcast!(arr, BooleanArray)
                .iter()
                .enumerate()
                .filter_map(|(idx, opt)| opt.map(|v| (idx, v))),
        )
    }

    fn from_scalar(scalar: &ScalarValue) -> Option<Self> {
        use datafusion::scalar::ScalarValue;

        match scalar {
            ScalarValue::Boolean(Some(v)) => Some(*v),
            _ => None,
        }
    }

    fn new_serializer(zstd_level: i32) -> Self::Serializer {
        BooleanRoaringSerializer::new(zstd_level)
    }

    fn key_len() -> usize {
        1
    }

    fn normalize(&self, _case_sensitive: bool) -> Self {
        *self
    }

    fn supports_range() -> bool {
        false // Boolean 不支持范围查询
    }
}
