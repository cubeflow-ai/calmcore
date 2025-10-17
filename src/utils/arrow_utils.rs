use std::hash::{Hash, Hasher};

use ahash::AHasher;
use arrow::array::{Array, ArrayRef, StringArray, UInt32Array};

use crate::arrow_downcast;

pub fn array_to_hash(arr: &ArrayRef) -> Vec<u32> {
    match arr.data_type() {
        arrow::datatypes::DataType::Utf8 => {
            let string_array = arrow_downcast!(arr, StringArray);
            let mut result = Vec::with_capacity(string_array.len());
            for v in string_array.iter() {
                let mut hasher = AHasher::default();
                v.hash(&mut hasher);
                result.push(hasher.finish() as u32);
            }
            result
        }
        arrow::datatypes::DataType::UInt32 => arrow_downcast!(arr, UInt32Array)
            .iter()
            .map(|v| v.unwrap_or(0))
            .collect(),
        _ => panic!("unsupported pk type"),
    }
}
