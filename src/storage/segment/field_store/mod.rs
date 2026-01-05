use std::{any::Any, borrow::Cow, collections::HashSet, error::Error, sync::Arc};

use parking_lot::{Mutex, RwLock};

use datafusion::{
    arrow::array::{ArrayRef, RecordBatch},
    scalar::ScalarValue,
};
use mem_btree::{
    persist::{self, num_ser},
    BTree,
};

use roaring::RoaringBitmap;

use crate::{partition::WriteInfo, schema::field::FieldType, utils::error::CoreResult};

// 新的泛型实现
pub mod generic_index;
pub mod index_key_impls;
pub mod ordered_f32;
pub mod ordered_f64; // OrderedF64 类型定义 // OrderedF32 类型定义
pub mod timestamp_key; // Timestamp 类型支持

// 其他模块
pub mod row_data;
pub mod text;

pub use row_data::RowDataStore;
pub use text::{FieldStats, FullTextField, PostingEntry, TermStats};

// 导出泛型类型别名
pub use generic_index::GenericIndexedField;
pub use ordered_f32::OrderedF32;
pub use ordered_f64::OrderedF64;

// 类型别名，方便使用
pub type KeywordField = GenericIndexedField<String>;
pub type I64Field = GenericIndexedField<i64>;
pub type U64Field = GenericIndexedField<u64>;
pub type U32Field = GenericIndexedField<u32>;
pub type I32Field = GenericIndexedField<i32>;
pub type F64Field = GenericIndexedField<OrderedF64>;
pub type F32Field = GenericIndexedField<OrderedF32>;
pub type U8Field = GenericIndexedField<u8>;
pub type I8Field = GenericIndexedField<i8>;
pub type U16Field = GenericIndexedField<u16>;
pub type I16Field = GenericIndexedField<i16>;
pub type BooleanField = GenericIndexedField<bool>;

// Timestamp 类型: 底层使用 TimestampKey (包装 i64) 存储毫秒级 Unix 时间戳
// 查询时支持字符串时间格式自动转换
pub type TimestampField = GenericIndexedField<timestamp_key::TimestampKey>;

/// Serializer for i64 keys with RoaringBitmap values
#[derive(Clone)]
pub struct I64RoaringSerializer {
    zstd_level: i32,
}

impl I64RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }

    #[allow(dead_code, clippy::should_implement_trait)]
    pub fn default() -> Self {
        Self { zstd_level: 3 }
    }
}

impl persist::ReadSerializer<i64, RoaringBitmap> for I64RoaringSerializer {
    fn deserialize_value(
        &self,
        data: &[u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys(&self, data: &[u8]) -> Vec<i64> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };

        if data_to_parse.len() < 2 {
            return Vec::new();
        }

        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        let mut result = Vec::with_capacity(key_count);
        let mut pos = 2;

        for _ in 0..key_count {
            if pos + 8 > data_to_parse.len() {
                break;
            }
            let key = i64::from_be_bytes([
                data_to_parse[pos],
                data_to_parse[pos + 1],
                data_to_parse[pos + 2],
                data_to_parse[pos + 3],
                data_to_parse[pos + 4],
                data_to_parse[pos + 5],
                data_to_parse[pos + 6],
                data_to_parse[pos + 7],
            ]);
            result.push(key);
            pos += 8;
        }
        result
    }
}

impl persist::WriteSerializer<i64, RoaringBitmap> for I64RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<i64>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;

        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");

        for key in keys {
            uncompressed.extend_from_slice(&key.to_be_bytes());
        }

        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for u64 keys with RoaringBitmap values
#[derive(Clone)]
pub struct U64RoaringSerializer {
    zstd_level: i32,
}

impl U64RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }
}

impl persist::ReadSerializer<u64, RoaringBitmap> for U64RoaringSerializer {
    fn deserialize_value(
        &self,
        data: &[u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys(&self, data: &[u8]) -> Vec<u64> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };
        if data_to_parse.len() < 2 {
            return Vec::new();
        }
        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        let mut result = Vec::with_capacity(key_count);
        let mut pos = 2;
        for _ in 0..key_count {
            if pos + 8 > data_to_parse.len() {
                break;
            }
            let key = u64::from_be_bytes([
                data_to_parse[pos],
                data_to_parse[pos + 1],
                data_to_parse[pos + 2],
                data_to_parse[pos + 3],
                data_to_parse[pos + 4],
                data_to_parse[pos + 5],
                data_to_parse[pos + 6],
                data_to_parse[pos + 7],
            ]);
            result.push(key);
            pos += 8;
        }
        result
    }
}

impl persist::WriteSerializer<u64, RoaringBitmap> for U64RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<u64>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;
        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");
        for key in keys {
            uncompressed.extend_from_slice(&key.to_be_bytes());
        }
        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for u32 keys with RoaringBitmap values
#[derive(Clone)]
pub struct U32RoaringSerializer {
    zstd_level: i32,
}

impl U32RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }
}

impl persist::ReadSerializer<u32, RoaringBitmap> for U32RoaringSerializer {
    fn deserialize_value(
        &self,
        data: &[u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys(&self, data: &[u8]) -> Vec<u32> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };
        if data_to_parse.len() < 2 {
            return Vec::new();
        }
        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        let mut result = Vec::with_capacity(key_count);
        let mut pos = 2;
        for _ in 0..key_count {
            if pos + 4 > data_to_parse.len() {
                break;
            }
            let key = u32::from_be_bytes([
                data_to_parse[pos],
                data_to_parse[pos + 1],
                data_to_parse[pos + 2],
                data_to_parse[pos + 3],
            ]);
            result.push(key);
            pos += 4;
        }
        result
    }
}

impl persist::WriteSerializer<u32, RoaringBitmap> for U32RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<u32>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;
        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");
        for key in keys {
            uncompressed.extend_from_slice(&key.to_be_bytes());
        }
        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for u8 keys with RoaringBitmap values
#[derive(Clone)]
pub struct U8RoaringSerializer {
    zstd_level: i32,
}

impl U8RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }
}

impl persist::ReadSerializer<u8, RoaringBitmap> for U8RoaringSerializer {
    fn deserialize_value(
        &self,
        data: &[u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys(&self, data: &[u8]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };
        if data_to_parse.len() < 2 {
            return Vec::new();
        }
        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        let mut result = Vec::with_capacity(key_count);
        let mut pos = 2;
        for _ in 0..key_count {
            if pos + 1 > data_to_parse.len() {
                break;
            }
            let key = data_to_parse[pos];
            result.push(key);
            pos += 1;
        }
        result
    }
}

impl persist::WriteSerializer<u8, RoaringBitmap> for U8RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<u8>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;
        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");
        for key in keys {
            uncompressed.push(*key);
        }
        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for bool keys with RoaringBitmap values
/// Boolean values are stored as u8 (0 or 1)
#[derive(Clone)]
pub struct BooleanRoaringSerializer {
    zstd_level: i32,
}

impl BooleanRoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }
}

impl persist::ReadSerializer<bool, RoaringBitmap> for BooleanRoaringSerializer {
    fn deserialize_value(
        &self,
        data: &[u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys(&self, data: &[u8]) -> Vec<bool> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };
        if data_to_parse.len() < 2 {
            return Vec::new();
        }
        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        let mut result = Vec::with_capacity(key_count);
        let mut pos = 2;
        for _ in 0..key_count {
            if pos + 1 > data_to_parse.len() {
                break;
            }
            let key = data_to_parse[pos] != 0;
            result.push(key);
            pos += 1;
        }
        result
    }
}

impl persist::WriteSerializer<bool, RoaringBitmap> for BooleanRoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<bool>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;
        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");
        for key in keys {
            uncompressed.push(if *key { 1u8 } else { 0u8 });
        }
        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for u16 keys with RoaringBitmap values
#[derive(Clone)]
pub struct U16RoaringSerializer {
    zstd_level: i32,
}

impl U16RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }
}

impl persist::ReadSerializer<u16, RoaringBitmap> for U16RoaringSerializer {
    fn deserialize_value(
        &self,
        data: &[u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys(&self, data: &[u8]) -> Vec<u16> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };
        if data_to_parse.len() < 2 {
            return Vec::new();
        }
        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        let mut result = Vec::with_capacity(key_count);
        let mut pos = 2;
        for _ in 0..key_count {
            if pos + 2 > data_to_parse.len() {
                break;
            }
            let key = u16::from_be_bytes([data_to_parse[pos], data_to_parse[pos + 1]]);
            result.push(key);
            pos += 2;
        }
        result
    }
}

impl persist::WriteSerializer<u16, RoaringBitmap> for U16RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<u16>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;
        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");
        for key in keys {
            uncompressed.extend_from_slice(&key.to_be_bytes());
        }
        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for i32 keys with RoaringBitmap values
#[derive(Clone)]
pub struct I32RoaringSerializer {
    zstd_level: i32,
}

impl I32RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }
}

impl persist::ReadSerializer<i32, RoaringBitmap> for I32RoaringSerializer {
    fn deserialize_value(
        &self,
        data: &[u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys(&self, data: &[u8]) -> Vec<i32> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };
        if data_to_parse.len() < 2 {
            return Vec::new();
        }
        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        let mut result = Vec::with_capacity(key_count);
        let mut pos = 2;
        for _ in 0..key_count {
            if pos + 4 > data_to_parse.len() {
                break;
            }
            let key = i32::from_be_bytes([
                data_to_parse[pos],
                data_to_parse[pos + 1],
                data_to_parse[pos + 2],
                data_to_parse[pos + 3],
            ]);
            result.push(key);
            pos += 4;
        }
        result
    }
}

impl persist::WriteSerializer<i32, RoaringBitmap> for I32RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<i32>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;
        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");
        for key in keys {
            uncompressed.extend_from_slice(&key.to_be_bytes());
        }
        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for i8 keys with RoaringBitmap values
#[derive(Clone)]
pub struct I8RoaringSerializer {
    zstd_level: i32,
}

impl I8RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }
}

impl persist::ReadSerializer<i8, RoaringBitmap> for I8RoaringSerializer {
    fn deserialize_value(
        &self,
        data: &[u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys(&self, data: &[u8]) -> Vec<i8> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };
        if data_to_parse.len() < 2 {
            return Vec::new();
        }
        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        let mut result = Vec::with_capacity(key_count);
        let mut pos = 2;
        for _ in 0..key_count {
            if pos + 1 > data_to_parse.len() {
                break;
            }
            let key = data_to_parse[pos] as i8;
            result.push(key);
            pos += 1;
        }
        result
    }
}

impl persist::WriteSerializer<i8, RoaringBitmap> for I8RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<i8>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;
        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");
        for key in keys {
            uncompressed.push(*key as u8);
        }
        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for i16 keys with RoaringBitmap values
#[derive(Clone)]
pub struct I16RoaringSerializer {
    zstd_level: i32,
}

impl I16RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }
}

impl persist::ReadSerializer<i16, RoaringBitmap> for I16RoaringSerializer {
    fn deserialize_value(
        &self,
        data: &[u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys(&self, data: &[u8]) -> Vec<i16> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };
        if data_to_parse.len() < 2 {
            return Vec::new();
        }
        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        let mut result = Vec::with_capacity(key_count);
        let mut pos = 2;
        for _ in 0..key_count {
            if pos + 2 > data_to_parse.len() {
                break;
            }
            let key = i16::from_be_bytes([data_to_parse[pos], data_to_parse[pos + 1]]);
            result.push(key);
            pos += 2;
        }
        result
    }
}

impl persist::WriteSerializer<i16, RoaringBitmap> for I16RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<i16>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;
        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");
        for key in keys {
            uncompressed.extend_from_slice(&key.to_be_bytes());
        }
        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for f32 keys with RoaringBitmap values
#[derive(Clone)]
pub struct F32RoaringSerializer {
    zstd_level: i32,
}

impl F32RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }
}

impl persist::ReadSerializer<OrderedF32, RoaringBitmap> for F32RoaringSerializer {
    fn deserialize_value(
        &self,
        data: &[u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys(&self, data: &[u8]) -> Vec<OrderedF32> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };
        if data_to_parse.len() < 2 {
            return Vec::new();
        }
        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        let mut result = Vec::with_capacity(key_count);
        let mut pos = 2;
        for _ in 0..key_count {
            if pos + 4 > data_to_parse.len() {
                break;
            }
            let key = f32::from_be_bytes([
                data_to_parse[pos],
                data_to_parse[pos + 1],
                data_to_parse[pos + 2],
                data_to_parse[pos + 3],
            ]);
            result.push(OrderedF32(key));
            pos += 4;
        }
        result
    }
}

impl persist::WriteSerializer<OrderedF32, RoaringBitmap> for F32RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<OrderedF32>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;
        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");
        for key in keys {
            uncompressed.extend_from_slice(&key.0.to_be_bytes());
        }
        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for f64 keys with RoaringBitmap values
/// Note: This serializer works with raw f64 values for disk storage
#[derive(Clone)]
pub struct F64RoaringSerializer {
    zstd_level: i32,
}

impl F64RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }

    #[allow(dead_code, clippy::should_implement_trait)]
    pub fn default() -> Self {
        Self { zstd_level: 3 }
    }
}

// OrderedF64 is now available from the ordered_f64 module
// (already imported at the top of this file)

impl persist::ReadSerializer<OrderedF64, RoaringBitmap> for F64RoaringSerializer {
    fn deserialize_value(
        &self,
        data: &[u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys(&self, data: &[u8]) -> Vec<OrderedF64> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };

        if data_to_parse.len() < 2 {
            return Vec::new();
        }

        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        let mut result = Vec::with_capacity(key_count);
        let mut pos = 2;

        for _ in 0..key_count {
            if pos + 8 > data_to_parse.len() {
                break;
            }
            let key = f64::from_be_bytes([
                data_to_parse[pos],
                data_to_parse[pos + 1],
                data_to_parse[pos + 2],
                data_to_parse[pos + 3],
                data_to_parse[pos + 4],
                data_to_parse[pos + 5],
                data_to_parse[pos + 6],
                data_to_parse[pos + 7],
            ]);
            result.push(OrderedF64(key));
            pos += 8;
        }
        result
    }
}

impl persist::WriteSerializer<OrderedF64, RoaringBitmap> for F64RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<OrderedF64>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;

        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");

        for key in keys {
            uncompressed.extend_from_slice(&key.0.to_be_bytes());
        }

        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for String keys with RoaringBitmap values
#[derive(Clone)]
pub struct StringRoaringSerializer {
    zstd_level: i32,
}

impl StringRoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }

    #[allow(dead_code, clippy::should_implement_trait)]
    pub fn default() -> Self {
        Self { zstd_level: 3 }
    }
}

impl persist::ReadSerializer<String, RoaringBitmap> for StringRoaringSerializer {
    fn deserialize_value(
        &self,
        data: &[u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys(&self, data: &[u8]) -> Vec<String> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };
        let mut result = Vec::new();
        let mut pos = 0;
        if data_to_parse.len() < 2 {
            return result;
        }
        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        pos += 2;
        for _ in 0..key_count {
            if pos >= data_to_parse.len() {
                break;
            }
            let len = persist::zigzag::read_u32(&data_to_parse, &mut pos) as usize;
            if pos + len > data_to_parse.len() {
                break;
            }
            match String::from_utf8(data_to_parse[pos..pos + len].to_vec()) {
                Ok(key) => result.push(key),
                Err(_) => break,
            }
            pos += len;
        }
        result
    }
}

impl persist::WriteSerializer<String, RoaringBitmap> for StringRoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<String>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;

        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");

        for key in keys {
            let key_bytes = key.as_bytes();
            persist::zigzag::write_u32(key_bytes.len() as u32, &mut uncompressed)
                .expect("write zigzag u32 failed");
            uncompressed.extend_from_slice(key_bytes);
        }

        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for u32 keys with RecordBatch values
/// Stores RecordBatch in Parquet format with built-in compression
#[derive(Clone)]
pub struct U32RecordBatchSerializer;

impl Default for U32RecordBatchSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl U32RecordBatchSerializer {
    #[allow(dead_code, clippy::should_implement_trait)]
    pub fn new() -> Self {
        Self
    }

    pub fn default() -> Self {
        Self
    }
}

impl persist::WriteSerializer<u32, RecordBatch> for U32RecordBatchSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<u32>) -> Cow<'a, [u8]> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());

        for key in keys {
            buf.extend_from_slice(&key.to_be_bytes());
        }

        Cow::Owned(buf)
    }

    fn serialize_value<'a>(&self, batch: &'a RecordBatch) -> Cow<'a, [u8]> {
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::basic::Compression;
        use datafusion::parquet::file::properties::WriterProperties;

        let mut buf = Vec::new();

        // 使用 Parquet format,默认 ZSTD 压缩
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .expect("Failed to create ArrowWriter");
        writer.write(batch).expect("Failed to write batch");
        writer.close().expect("Failed to close writer");

        Cow::Owned(buf)
    }
}

impl persist::ReadSerializer<u32, RecordBatch> for U32RecordBatchSerializer {
    fn deserialize_keys(&self, data: &[u8]) -> Vec<u32> {
        let mut pos = 0;
        if data.len() < 4 {
            return Vec::new();
        }

        let count =
            u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            if pos + 4 > data.len() {
                break;
            }
            let key = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            keys.push(key);
            pos += 4;
        }
        keys
    }

    fn deserialize_value(&self, data: &[u8]) -> Result<RecordBatch, Box<dyn Error>> {
        use bytes::Bytes;
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        // 使用 Parquet format 反序列化
        let bytes = Bytes::copy_from_slice(data);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| format!("Failed to create ParquetRecordBatchReaderBuilder: {}", e))?;

        let mut reader = builder
            .build()
            .map_err(|e| format!("Failed to build reader: {}", e))?;

        // 读取第一个 batch
        if let Some(result) = reader.next() {
            result.map_err(|e| format!("Failed to read batch: {}", e).into())
        } else {
            Err("No batch found".into())
        }
    }
}

pub trait IndexReader: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn field_type(&self) -> FieldType;
    fn query(&self, value: &ScalarValue) -> Option<RoaringBitmap>;

    /// Range query with support for open/closed intervals
    ///
    /// # Arguments
    /// * `start` - Start bound value
    /// * `start_inclusive` - Whether start bound is inclusive (>=) or exclusive (>)
    /// * `end` - End bound value
    /// * `end_inclusive` - Whether end bound is inclusive (<=) or exclusive (<)
    fn range(
        &self,
        start: &ScalarValue,
        start_inclusive: bool,
        end: &ScalarValue,
        end_inclusive: bool,
    ) -> Option<RoaringBitmap>;

    /// Range union query - 优化版本，直接返回聚合后的 bitmap
    ///
    /// 对于支持 union_leaf 的索引(数值类型等)，使用底层 B-Tree 的 range_union()
    /// 可以获得 100-200x 的性能提升
    ///
    /// # Arguments
    /// * `start` - Start bound value
    /// * `start_inclusive` - Whether start bound is inclusive (>=) or exclusive (>)
    /// * `end` - End bound value
    /// * `end_inclusive` - Whether end bound is inclusive (<=) or exclusive (<)
    ///
    /// # Returns
    /// - Some(bitmap): 支持 range_union 优化，返回聚合后的 bitmap
    /// - None: 不支持，调用方应该回退到 range() 方法
    fn range_union(
        &self,
        _start: &ScalarValue,
        _start_inclusive: bool,
        _end: &ScalarValue,
        _end_inclusive: bool,
    ) -> Option<RoaringBitmap> {
        None // 默认不支持，回退到 range()
    }

    /// 估算字段的基数（不同值的数量）
    /// 用于查询优化和成本估算
    /// 默认实现返回一个保守的估计值
    fn estimate_cardinality(&self) -> usize {
        1000 // 默认假设 1000 个不同的值
    }

    /// 按顺序扫描索引，返回有序的 doc_ids（ORDER BY 优化）
    ///
    /// # Arguments
    /// * `ascending` - true 表示升序，false 表示降序
    /// * `filter_bitmap` - 可选的过滤 bitmap（只返回在这个 bitmap 中的 doc_ids）
    /// * `limit` - 可选的限制返回的文档数量
    ///
    /// # Returns
    /// 返回按顺序的 doc_ids，如果索引不支持有序扫描则返回 None
    ///
    /// # Note
    /// 默认实现返回 None，表示不支持有序扫描
    /// 只有支持有序索引的类型（如数值、时间戳）才应该实现此方法
    fn scan_ordered(
        &self,
        _ascending: bool,
        _filter_bitmap: Option<&RoaringBitmap>,
        _limit: Option<usize>,
    ) -> Option<Vec<u32>> {
        None // 默认不支持
    }
}

pub trait IndexWriter: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn field_type(&self) -> FieldType;
    fn write(&self, data: &RecordBatch, start_id: u32) -> CoreResult<()>;
    fn mget_internal_id(&self, column: &ArrayRef) -> Vec<u32>;
    fn as_any(&self) -> &dyn Any;
}

pub trait PkWriter: Send + Sync + 'static {
    // write primary key and return the need delete ids
    fn write_pk(
        &self,
        data: &RecordBatch,
        start_id: u32,
        info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<HashSet<u32>>;
}

#[derive(Clone)]
pub(crate) enum InvertedIndex<K>
where
    K: Clone + PartialOrd,
{
    Disk(Arc<persist::TreeReader<K, RoaringBitmap>>),
    Memory(BTree<K, Arc<RwLock<Vec<u32>>>>),
}

impl<K: Clone + PartialOrd + Ord> InvertedIndex<K> {
    pub fn new_memory(size: usize) -> InvertedIndex<K> {
        InvertedIndex::Memory(BTree::new(size))
    }

    pub fn new_disk<S>(path: &str, serializer: S) -> CoreResult<InvertedIndex<K>>
    where
        S: persist::ReadSerializer<K, RoaringBitmap> + 'static,
    {
        let reader = persist::TreeReader::new(std::path::Path::new(path), Box::new(serializer))
            .map_err(|e| crate::utils::error::CoreError::IOError(e.to_string()))?;
        Ok(InvertedIndex::Disk(Arc::new(reader)))
    }
}

/// write functions
impl<K: Clone + PartialOrd + Ord> InvertedIndex<K> {
    pub(crate) fn extend(&mut self, k: K, ids: Vec<u32>) {
        if let InvertedIndex::Memory(tree) = self {
            match tree.get(&k) {
                Some(v) => v.write().extend(ids),
                None => _ = tree.put(k, Arc::new(RwLock::new(ids))),
            }
        } else {
            panic!("cannot insert to disk index");
        }
    }

    #[allow(dead_code, clippy::should_implement_trait)]
    pub(crate) fn append_ids(&mut self, k: K, ids: Vec<u32>) {
        if let InvertedIndex::Memory(tree) = self {
            match tree.get(&k) {
                Some(v) => v.write().extend(ids),
                None => _ = tree.put(k, Arc::new(RwLock::new(ids))),
            }
        } else {
            panic!("cannot insert to disk index");
        }
    }

    pub(crate) fn insert(
        &mut self,
        k: K,
        ids: Vec<u32>,
    ) -> Option<mem_btree::Item<K, Arc<RwLock<Vec<u32>>>>> {
        if let InvertedIndex::Memory(tree) = self {
            tree.put(k, Arc::new(RwLock::new(ids)))
        } else {
            panic!("cannot insert to disk index");
        }
    }
}

/// read functions
impl<K: Clone + PartialOrd + Ord> InvertedIndex<K> {
    pub(crate) fn get_bitmap(&self, k: &K) -> Option<RoaringBitmap> {
        match self {
            InvertedIndex::Disk(r) => r.get(k),
            InvertedIndex::Memory(btree) => btree.get(k).map(|v| {
                // 🔧 修复: 不能假设 Vec 是排序的,使用通用的 from_iter
                // 并发写入时 extend() 可能导致 Vec 无序
                let ids = v.read();
                RoaringBitmap::from_iter(ids.iter().copied())
            }),
        }
    }

    /// Range query with support for open/closed intervals
    ///
    /// # Arguments
    /// * `start` - Optional start bound (None means no lower bound)
    /// * `start_inclusive` - Whether start bound is inclusive (>=) or exclusive (>)
    /// * `end` - Optional end bound (None means no upper bound)
    /// * `end_inclusive` - Whether end bound is inclusive (<=) or exclusive (<)
    ///
    /// # Examples
    /// - `range_query(Some(10), true, Some(20), true)` -> [10, 20]
    /// - `range_query(Some(10), false, Some(20), true)` -> (10, 20]
    /// - `range_query(Some(10), true, Some(20), false)` -> [10, 20)
    /// - `range_query(Some(10), false, Some(20), false)` -> (10, 20)
    pub(crate) fn range_query(
        &self,
        start: Option<&K>,
        start_inclusive: bool,
        end: Option<&K>,
        end_inclusive: bool,
    ) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        match self {
            InvertedIndex::Disk(reader) => {
                // Use TreeIterator with seek optimization for efficient range query
                let mut iter = reader.iter();

                // Seek to start position if specified
                if let Some(s) = start {
                    iter.seek(s);
                }

                // Iterate through keys in range
                for item in iter {
                    let (key, bitmap, _ttl) = &*item;

                    // Check start bound
                    let start_ok = match start {
                        Some(s) => {
                            if start_inclusive {
                                key >= s // [s, ...)
                            } else {
                                key > s // (s, ...)
                            }
                        }
                        None => true, // No lower bound
                    };

                    // Check end bound
                    let end_ok = match end {
                        Some(e) => {
                            if end_inclusive {
                                key <= e // (..., e]
                            } else {
                                key < e // (..., e)
                            }
                        }
                        None => true, // No upper bound
                    };

                    // Early termination: if key exceeds end bound, stop iterating
                    if !end_ok {
                        break;
                    }

                    if start_ok {
                        result |= bitmap;
                    }
                }
                result
            }
            InvertedIndex::Memory(btree) => {
                log::debug!(
                    "🔍 [InvertedIndex::Memory::range_query] btree.len()={}",
                    btree.len()
                );

                // For memory index, use seek to efficiently position iterator at start
                // This avoids scanning from the beginning of the tree
                let mut iter = btree.iter();

                // Seek to start position if specified
                if let Some(s) = start {
                    iter.seek(s);
                }

                let mut matched_keys = 0;
                // Iterate through keys in range
                while let Some(item) = iter.next() {
                    let (key, ids_lock, _ttl) = &*item;

                    // Check start bound
                    let start_ok = match start {
                        Some(s) => {
                            if start_inclusive {
                                key >= s // [s, ...)
                            } else {
                                key > s // (s, ...)
                            }
                        }
                        None => true, // No lower bound
                    };

                    // Check end bound
                    let end_ok = match end {
                        Some(e) => {
                            if end_inclusive {
                                key <= e // (..., e]
                            } else {
                                key < e // (..., e)
                            }
                        }
                        None => true, // No upper bound
                    };

                    // Early termination: if key exceeds end bound, stop iterating
                    // This is important for performance as keys are sorted
                    if !end_ok {
                        break;
                    }

                    if start_ok {
                        let ids = ids_lock.read();
                        // 🔧 修复: 不能假设 Vec 是排序的,使用通用的 from_iter
                        result |= RoaringBitmap::from_iter(ids.iter().copied());
                        matched_keys += 1;
                    }
                }
                log::debug!(
                    "🔍 [InvertedIndex::Memory::range_query] matched_keys={}, result.len()={}",
                    matched_keys,
                    result.len()
                );
                result
            }
        }
    }

    /// Range union query - 使用底层 mem_btree 的 range_union() 优化
    ///
    /// 仅支持磁盘索引(Disk variant)，内存索引返回 None
    ///
    /// # Performance
    /// 对于大范围查询，可以获得 100-200x 的性能提升
    pub(crate) fn range_union(
        &self,
        start: Option<&K>,
        start_inclusive: bool,
        end: Option<&K>,
        end_inclusive: bool,
    ) -> Option<RoaringBitmap> {
        match self {
            InvertedIndex::Disk(reader) => {
                // 使用底层 mem_btree 的 range_union() - 100-200x faster!
                reader
                    .range_union(start, start_inclusive, end, end_inclusive)
                    .ok()
            }
            InvertedIndex::Memory(_) => {
                // 内存索引不支持 range_union，返回 None 让调用方回退到 range()
                None
            }
        }
    }

    #[allow(dead_code, clippy::should_implement_trait)]
    pub(crate) fn memory_get_ref(&self, k: &K) -> Option<&Arc<RwLock<Vec<u32>>>> {
        match self {
            InvertedIndex::Disk(_) => unreachable!(),
            InvertedIndex::Memory(btree) => btree.get(k),
        }
    }

    #[allow(dead_code, clippy::should_implement_trait)]
    pub fn len(&self) -> usize {
        match self {
            InvertedIndex::Disk(r) => r.len(),
            InvertedIndex::Memory(btree) => btree.len(),
        }
    }
}

/// Encode a list of u32 doc IDs into bytes with a leading marker byte.
/// - 0: delta-encoded u32 sequence (big-endian, mem_btree::persist::num_ser::u32_coder)
/// - 1: RoaringBitmap native serialization
/// Returns Vec<u8> on success.
#[allow(dead_code, clippy::should_implement_trait)]
pub fn encode_roaring_from_u32s(ids: &[u32]) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    if ids.len() < 1000 {
        // marker 0 => delta encoding
        out.push(0u8);
        num_ser::u32_coder::write_delta(&mut out, ids)?;
    } else {
        // marker 1 => roaring native
        out.push(1u8);
        let rb = RoaringBitmap::from_iter(ids.iter().copied());
        // roaring serialize_into returns io::Result
        rb.serialize_into(&mut out)?;
    }
    Ok(out)
}

/// Decode bytes encoded by `encode_roaring_from_u32s` back into a RoaringBitmap.
/// Accepts empty slice as empty bitmap.
pub fn decode_roaring_from_bytes(
    data: &[u8],
) -> std::result::Result<RoaringBitmap, Box<dyn Error>> {
    if data.is_empty() {
        return Ok(RoaringBitmap::new());
    }

    let marker = data[0];
    let mut data_slice = &data[1..];
    match marker {
        0 => {
            let ids = num_ser::u32_coder::read_delta(&data_slice);
            Ok(RoaringBitmap::from_iter(ids))
        }
        1 => RoaringBitmap::deserialize_from(&mut data_slice)
            .map_err(|e| Box::new(e) as Box<dyn Error>),
        _ => Err(format!("Unknown marker byte: {}", marker).into()),
    }
}

/// Encode a RoaringBitmap into bytes with a leading marker, choosing the smaller form
/// between: 0 + delta-encoded u32s, or 1 + roaring native format.
pub fn encode_roaring_from_bitmap(bitmap: &RoaringBitmap) -> Vec<u8> {
    // Estimate sizes - need to check actual delta-encoded size
    let ids: Vec<u32> = bitmap.iter().collect();

    // Try delta encoding
    let mut delta_buf = Vec::new();
    delta_buf.push(0u8);
    if num_ser::u32_coder::write_delta(&mut delta_buf, &ids).is_ok() {
        // Try roaring native
        let mut roaring_buf = Vec::new();
        roaring_buf.push(1u8);
        if bitmap.serialize_into(&mut roaring_buf).is_ok() {
            // Choose the smaller one
            if delta_buf.len() <= roaring_buf.len() {
                delta_buf
            } else {
                roaring_buf
            }
        } else {
            // Roaring serialization failed, use delta
            delta_buf
        }
    } else {
        // Delta encoding failed (shouldn't happen), fallback to roaring
        let mut buf = Vec::new();
        buf.push(1u8);
        let _ = bitmap.serialize_into(&mut buf);
        buf
    }
}
