use std::{
    any::Any,
    borrow::Cow,
    collections::HashSet,
    error::Error,
    sync::{Arc, RwLock},
};

use arrow::array::{ArrayRef, RecordBatch};
use mem_btree::{persist, BTree};
use roaring::RoaringBitmap;

use crate::{partition::WriteInfo, schema::field::FieldType, utils::error::CoreResult};

pub mod keyword;
pub mod num_f32;
pub mod num_f64;
pub mod num_i32;
pub mod num_i64;
pub mod num_u32;
pub mod num_u64;

/// Serializer for u32 keys with RecordBatch values
/// Stores RecordBatch in Arrow IPC format with zstd compression
#[derive(Clone)]
pub struct U32RecordBatchSerializer {
    zstd_level: i32,
}

impl U32RecordBatchSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }

    pub fn default() -> Self {
        Self { zstd_level: 3 }
    }
}

impl persist::KeySerializer<u32, RecordBatch> for U32RecordBatchSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<u32>) -> Cow<'a, [u8]> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());

        for key in keys {
            buf.extend_from_slice(&key.to_be_bytes());
        }

        // 使用 zstd 压缩
        match zstd::encode_all(&buf[..], self.zstd_level) {
            Ok(compressed) => Cow::Owned(compressed),
            Err(_) => Cow::Owned(buf),
        }
    }

    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<u32> {
        // 解压
        let decompressed = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };

        let mut pos = 0;
        if decompressed.len() < 4 {
            return Vec::new();
        }

        let count = u32::from_be_bytes([
            decompressed[pos],
            decompressed[pos + 1],
            decompressed[pos + 2],
            decompressed[pos + 3],
        ]) as usize;
        pos += 4;

        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            if pos + 4 > decompressed.len() {
                break;
            }
            let key = u32::from_be_bytes([
                decompressed[pos],
                decompressed[pos + 1],
                decompressed[pos + 2],
                decompressed[pos + 3],
            ]);
            keys.push(key);
            pos += 4;
        }
        keys
    }

    fn serialize_value<'a>(&self, batch: &'a RecordBatch) -> Cow<'a, [u8]> {
        use arrow::ipc::writer::StreamWriter;

        let mut buf = Vec::new();

        // 使用 Arrow IPC format
        {
            let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
                .expect("Failed to create StreamWriter");
            writer.write(batch).expect("Failed to write batch");
            writer.finish().expect("Failed to finish writer");
        }

        // 使用 zstd 压缩
        match zstd::encode_all(&buf[..], self.zstd_level) {
            Ok(compressed) => Cow::Owned(compressed),
            Err(_) => Cow::Owned(buf),
        }
    }

    fn deserialize_value<'a>(&self, data: &'a [u8]) -> Result<RecordBatch, Box<dyn Error>> {
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;

        // 解压
        let decompressed =
            zstd::decode_all(data).map_err(|e| format!("Failed to decompress: {}", e))?;

        // 使用 Arrow IPC format 反序列化
        let cursor = Cursor::new(decompressed);
        let mut reader = StreamReader::try_new(cursor, None)
            .map_err(|e| format!("Failed to create StreamReader: {}", e))?;

        // 读取第一个 batch
        if let Some(result) = reader.next() {
            result.map_err(|e| format!("Failed to read batch: {}", e).into())
        } else {
            Err("No batch found".into())
        }
    }
}

/// RowDataStore: 用于存储文档的 row data (u32 -> RecordBatch)
/// 支持内存模式和磁盘模式
#[derive(Clone)]
pub enum RowDataStore {
    Disk(Arc<persist::TreeReader<u32, RecordBatch>>),
    Memory(BTree<u32, RecordBatch>),
}

impl RowDataStore {
    pub fn new_memory(size: usize) -> Self {
        RowDataStore::Memory(BTree::new(size))
    }

    pub fn new_disk<S>(path: &str, serializer: S) -> CoreResult<Self>
    where
        S: persist::KeySerializer<u32, RecordBatch> + 'static,
    {
        let reader = persist::TreeReader::new(std::path::Path::new(path), Box::new(serializer))
            .map_err(|e| crate::utils::error::CoreError::IOError(e.to_string()))?;
        Ok(RowDataStore::Disk(Arc::new(reader)))
    }

    /// 插入一个 RecordBatch
    pub fn put(&mut self, key: u32, batch: RecordBatch) {
        if let RowDataStore::Memory(tree) = self {
            tree.put(key, batch);
        } else {
            panic!("cannot write to disk store");
        }
    }

    /// 获取一个 RecordBatch
    pub fn get(&self, key: &u32) -> Option<RecordBatch> {
        match self {
            RowDataStore::Disk(reader) => reader.get(key),
            RowDataStore::Memory(tree) => tree.get(key).cloned(),
        }
    }

    /// Find the largest key-value pair where key <= given key (floor lookup)
    /// This is used to find which RecordBatch contains a specific doc_id
    ///
    /// Example: BTree has keys [1, 100, 200], query for doc_id 50 returns batch at key 1
    pub fn floor(&self, key: &u32) -> Option<(u32, RecordBatch)> {
        match self {
            RowDataStore::Disk(reader) => {
                // For disk, we need to iterate (TreeReader doesn't have floor yet)
                // This is a simple implementation - can be optimized later
                reader.floor(key).map(|item| {
                    let (k, v, _ttl) = &*item;
                    (*k, v.clone())
                })
            }
            RowDataStore::Memory(tree) => tree.floor(key).map(|item| {
                let (k, v, _ttl) = &*item;
                (*k, v.clone())
            }),
        }
    }

    /// 获取所有数据的迭代器(仅用于持久化)
    pub fn iter(&self) -> Option<impl Iterator<Item = (u32, RecordBatch)> + '_> {
        match self {
            RowDataStore::Memory(tree) => Some(tree.iter().map(|item| {
                let (key, value, _ttl) = &*item;
                (*key, value.clone())
            })),
            RowDataStore::Disk(_) => None,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            RowDataStore::Disk(reader) => reader.len() as usize,
            RowDataStore::Memory(tree) => tree.len(),
        }
    }
}

/// Serializer for String keys with zstd compression and RoaringBitmap values
/// with intelligent Vec<u32> vs RoaringBitmap selection
#[derive(Clone)]
pub struct StringRoaringSerializer {
    zstd_level: i32,
}

impl StringRoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }

    pub fn default() -> Self {
        Self { zstd_level: 3 } // 默认压缩级别 3
    }
}
impl persist::KeySerializer<String, RoaringBitmap> for StringRoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<String>) -> Cow<'a, [u8]> {
        // 1. 序列化为简单格式
        let mut buf = Vec::new();
        buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());

        for key in keys {
            let key_bytes = key.as_bytes();
            buf.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(key_bytes);
        }

        // 2. 用 zstd 压缩
        match zstd::encode_all(&buf[..], self.zstd_level) {
            Ok(compressed) => Cow::Owned(compressed),
            Err(_) => Cow::Owned(buf), // 压缩失败则返回原始数据
        }
    }

    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<String> {
        // 1. 解压
        let decompressed = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };

        // 2. 反序列化
        let mut pos = 0;
        if decompressed.len() < 4 {
            return Vec::new();
        }

        let count = u32::from_be_bytes([
            decompressed[pos],
            decompressed[pos + 1],
            decompressed[pos + 2],
            decompressed[pos + 3],
        ]) as usize;
        pos += 4;

        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            if pos + 4 > decompressed.len() {
                break;
            }

            let len = u32::from_be_bytes([
                decompressed[pos],
                decompressed[pos + 1],
                decompressed[pos + 2],
                decompressed[pos + 3],
            ]) as usize;
            pos += 4;

            if pos + len > decompressed.len() {
                break;
            }

            let key = String::from_utf8_lossy(&decompressed[pos..pos + len]).to_string();
            keys.push(key);
            pos += len;
        }
        keys
    }

    fn serialize_value<'a>(&self, bitmap: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        let ids: Vec<u32> = bitmap.iter().collect();

        // 计算两种格式的大小
        let vec_size = 1 + ids.len() * 4; // type(1) + u32s
        let bitmap_size = 1 + bitmap.serialized_size(); // type(1) + bitmap

        let mut buf = Vec::new();
        if vec_size <= bitmap_size {
            // 使用 Vec<u32> 格式
            buf.push(0u8); // type = 0
            for id in ids {
                buf.extend_from_slice(&id.to_be_bytes());
            }
        } else {
            // 使用 RoaringBitmap 格式
            buf.push(1u8); // type = 1
            if let Err(_) = bitmap.serialize_into(&mut buf) {
                // 序列化失败，回退到 Vec 格式
                buf.clear();
                buf.push(0u8);
                for id in bitmap.iter() {
                    buf.extend_from_slice(&id.to_be_bytes());
                }
            }
        }
        Cow::Owned(buf)
    }

    fn deserialize_value<'a>(&self, data: &'a [u8]) -> Result<RoaringBitmap, Box<dyn Error>> {
        if data.is_empty() {
            return Err("Empty data".into());
        }

        let type_flag = data[0];

        match type_flag {
            0 => {
                // Vec<u32> 格式
                let count = (data.len() - 1) / 4;
                let mut ids = Vec::with_capacity(count);
                for i in 0..count {
                    let offset = 1 + i * 4;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let id = u32::from_be_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]);
                    ids.push(id);
                }
                Ok(RoaringBitmap::from_sorted_iter(ids.into_iter())?)
            }
            1 => {
                // RoaringBitmap 格式
                Ok(RoaringBitmap::deserialize_from(&data[1..])?)
            }
            _ => Err("Unknown type flag".into()),
        }
    }
}

/// Serializer for F32Key (memcomparable f32) with RoaringBitmap values
#[derive(Clone)]
pub struct F32RoaringSerializer {
    zstd_level: i32,
}

impl F32RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }

    pub fn default() -> Self {
        Self { zstd_level: 3 }
    }
}

impl persist::KeySerializer<num_f32::F32Key, RoaringBitmap> for F32RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<num_f32::F32Key>) -> Cow<'a, [u8]> {
        let mut buf = Vec::with_capacity(4 + keys.len() * 4);
        buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());

        for key in keys {
            buf.extend_from_slice(&key.to_memcomparable());
        }

        Cow::Owned(buf)
    }

    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<num_f32::F32Key> {
        if data.len() < 4 {
            return Vec::new();
        }

        let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut keys = Vec::with_capacity(count);
        let mut pos = 4;

        for _ in 0..count {
            if pos + 4 > data.len() {
                break;
            }
            let bytes = [data[pos], data[pos + 1], data[pos + 2], data[pos + 3]];
            keys.push(num_f32::F32Key::from_memcomparable(bytes));
            pos += 4;
        }

        keys
    }

    fn serialize_value<'a>(&self, bitmap: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        // Reuse the same logic as StringRoaringSerializer
        let ids: Vec<u32> = bitmap.iter().collect();
        let vec_size = 1 + ids.len() * 4;
        let bitmap_size = 1 + bitmap.serialized_size();

        let mut buf = Vec::new();
        if vec_size <= bitmap_size {
            buf.push(0u8);
            for id in ids {
                buf.extend_from_slice(&id.to_be_bytes());
            }
        } else {
            buf.push(1u8);
            if let Err(_) = bitmap.serialize_into(&mut buf) {
                buf.clear();
                buf.push(0u8);
                for id in bitmap.iter() {
                    buf.extend_from_slice(&id.to_be_bytes());
                }
            }
        }
        Cow::Owned(buf)
    }

    fn deserialize_value<'a>(&self, data: &'a [u8]) -> Result<RoaringBitmap, Box<dyn Error>> {
        if data.is_empty() {
            return Err("Empty data".into());
        }

        match data[0] {
            0 => {
                let count = (data.len() - 1) / 4;
                let mut ids = Vec::with_capacity(count);
                for i in 0..count {
                    let offset = 1 + i * 4;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let id = u32::from_be_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]);
                    ids.push(id);
                }
                Ok(RoaringBitmap::from_sorted_iter(ids.into_iter())?)
            }
            1 => Ok(RoaringBitmap::deserialize_from(&data[1..])?),
            _ => Err("Unknown type flag".into()),
        }
    }
}

/// Serializer for I32Key (memcomparable i32) with RoaringBitmap values
#[derive(Clone)]
pub struct I32RoaringSerializer {
    _zstd_level: i32,
}

impl I32RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self {
            _zstd_level: zstd_level,
        }
    }

    pub fn default() -> Self {
        Self { _zstd_level: 3 }
    }
}

impl persist::KeySerializer<num_i32::I32Key, RoaringBitmap> for I32RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<num_i32::I32Key>) -> Cow<'a, [u8]> {
        let mut buf = Vec::with_capacity(4 + keys.len() * 4);
        buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());

        for key in keys {
            buf.extend_from_slice(&key.to_memcomparable());
        }

        Cow::Owned(buf)
    }

    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<num_i32::I32Key> {
        if data.len() < 4 {
            return Vec::new();
        }

        let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut keys = Vec::with_capacity(count);
        let mut pos = 4;

        for _ in 0..count {
            if pos + 4 > data.len() {
                break;
            }
            let bytes = [data[pos], data[pos + 1], data[pos + 2], data[pos + 3]];
            keys.push(num_i32::I32Key::from_memcomparable(bytes));
            pos += 4;
        }

        keys
    }

    fn serialize_value<'a>(&self, bitmap: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        let ids: Vec<u32> = bitmap.iter().collect();
        let vec_size = 1 + ids.len() * 4;
        let bitmap_size = 1 + bitmap.serialized_size();

        let mut buf = Vec::new();
        if vec_size <= bitmap_size {
            buf.push(0u8);
            for id in ids {
                buf.extend_from_slice(&id.to_be_bytes());
            }
        } else {
            buf.push(1u8);
            if let Err(_) = bitmap.serialize_into(&mut buf) {
                buf.clear();
                buf.push(0u8);
                for id in bitmap.iter() {
                    buf.extend_from_slice(&id.to_be_bytes());
                }
            }
        }
        Cow::Owned(buf)
    }

    fn deserialize_value<'a>(&self, data: &'a [u8]) -> Result<RoaringBitmap, Box<dyn Error>> {
        if data.is_empty() {
            return Err("Empty data".into());
        }

        match data[0] {
            0 => {
                let count = (data.len() - 1) / 4;
                let mut ids = Vec::with_capacity(count);
                for i in 0..count {
                    let offset = 1 + i * 4;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let id = u32::from_be_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]);
                    ids.push(id);
                }
                Ok(RoaringBitmap::from_sorted_iter(ids.into_iter())?)
            }
            1 => Ok(RoaringBitmap::deserialize_from(&data[1..])?),
            _ => Err("Unknown type flag".into()),
        }
    }
}

/// Serializer for U32Key (memcomparable u32) with RoaringBitmap values
#[derive(Clone)]
pub struct U32RoaringSerializer {
    _zstd_level: i32,
}

impl U32RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self {
            _zstd_level: zstd_level,
        }
    }

    pub fn default() -> Self {
        Self { _zstd_level: 3 }
    }
}

impl persist::KeySerializer<num_u32::U32Key, RoaringBitmap> for U32RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<num_u32::U32Key>) -> Cow<'a, [u8]> {
        let mut buf = Vec::with_capacity(4 + keys.len() * 4);
        buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());

        for key in keys {
            buf.extend_from_slice(&key.to_memcomparable());
        }

        Cow::Owned(buf)
    }

    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<num_u32::U32Key> {
        if data.len() < 4 {
            return Vec::new();
        }

        let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut keys = Vec::with_capacity(count);
        let mut pos = 4;

        for _ in 0..count {
            if pos + 4 > data.len() {
                break;
            }
            let bytes = [data[pos], data[pos + 1], data[pos + 2], data[pos + 3]];
            keys.push(num_u32::U32Key::from_memcomparable(bytes));
            pos += 4;
        }

        keys
    }

    fn serialize_value<'a>(&self, bitmap: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        let ids: Vec<u32> = bitmap.iter().collect();
        let vec_size = 1 + ids.len() * 4;
        let bitmap_size = 1 + bitmap.serialized_size();

        let mut buf = Vec::new();
        if vec_size <= bitmap_size {
            buf.push(0u8);
            for id in ids {
                buf.extend_from_slice(&id.to_be_bytes());
            }
        } else {
            buf.push(1u8);
            if let Err(_) = bitmap.serialize_into(&mut buf) {
                buf.clear();
                buf.push(0u8);
                for id in bitmap.iter() {
                    buf.extend_from_slice(&id.to_be_bytes());
                }
            }
        }
        Cow::Owned(buf)
    }

    fn deserialize_value<'a>(&self, data: &'a [u8]) -> Result<RoaringBitmap, Box<dyn Error>> {
        if data.is_empty() {
            return Err("Empty data".into());
        }

        match data[0] {
            0 => {
                let count = (data.len() - 1) / 4;
                let mut ids = Vec::with_capacity(count);
                for i in 0..count {
                    let offset = 1 + i * 4;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let id = u32::from_be_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]);
                    ids.push(id);
                }
                Ok(RoaringBitmap::from_sorted_iter(ids.into_iter())?)
            }
            1 => Ok(RoaringBitmap::deserialize_from(&data[1..])?),
            _ => Err("Unknown type flag".into()),
        }
    }
}

/// Serializer for I64Key (memcomparable i64) with RoaringBitmap values
#[derive(Clone)]
pub struct I64RoaringSerializer {
    _zstd_level: i32,
}

impl I64RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self {
            _zstd_level: zstd_level,
        }
    }

    pub fn default() -> Self {
        Self { _zstd_level: 3 }
    }
}

impl persist::KeySerializer<num_i64::I64Key, RoaringBitmap> for I64RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<num_i64::I64Key>) -> Cow<'a, [u8]> {
        let mut buf = Vec::with_capacity(4 + keys.len() * 8);
        buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());

        for key in keys {
            buf.extend_from_slice(&key.to_memcomparable());
        }

        Cow::Owned(buf)
    }

    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<num_i64::I64Key> {
        if data.len() < 4 {
            return Vec::new();
        }

        let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut keys = Vec::with_capacity(count);
        let mut pos = 4;

        for _ in 0..count {
            if pos + 8 > data.len() {
                break;
            }
            let bytes = [
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ];
            keys.push(num_i64::I64Key::from_memcomparable(bytes));
            pos += 8;
        }

        keys
    }

    fn serialize_value<'a>(&self, bitmap: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        let ids: Vec<u32> = bitmap.iter().collect();
        let vec_size = 1 + ids.len() * 4;
        let bitmap_size = 1 + bitmap.serialized_size();

        let mut buf = Vec::new();
        if vec_size <= bitmap_size {
            buf.push(0u8);
            for id in ids {
                buf.extend_from_slice(&id.to_be_bytes());
            }
        } else {
            buf.push(1u8);
            if let Err(_) = bitmap.serialize_into(&mut buf) {
                buf.clear();
                buf.push(0u8);
                for id in bitmap.iter() {
                    buf.extend_from_slice(&id.to_be_bytes());
                }
            }
        }
        Cow::Owned(buf)
    }

    fn deserialize_value<'a>(&self, data: &'a [u8]) -> Result<RoaringBitmap, Box<dyn Error>> {
        if data.is_empty() {
            return Err("Empty data".into());
        }

        match data[0] {
            0 => {
                let count = (data.len() - 1) / 4;
                let mut ids = Vec::with_capacity(count);
                for i in 0..count {
                    let offset = 1 + i * 4;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let id = u32::from_be_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]);
                    ids.push(id);
                }
                Ok(RoaringBitmap::from_sorted_iter(ids.into_iter())?)
            }
            1 => Ok(RoaringBitmap::deserialize_from(&data[1..])?),
            _ => Err("Unknown type flag".into()),
        }
    }
}

/// Serializer for U64Key (memcomparable u64) with RoaringBitmap values
#[derive(Clone)]
pub struct U64RoaringSerializer {
    _zstd_level: i32,
}

impl U64RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self {
            _zstd_level: zstd_level,
        }
    }

    pub fn default() -> Self {
        Self { _zstd_level: 3 }
    }
}

impl persist::KeySerializer<num_u64::U64Key, RoaringBitmap> for U64RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<num_u64::U64Key>) -> Cow<'a, [u8]> {
        let mut buf = Vec::with_capacity(4 + keys.len() * 8);
        buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());

        for key in keys {
            buf.extend_from_slice(&key.to_memcomparable());
        }

        Cow::Owned(buf)
    }

    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<num_u64::U64Key> {
        if data.len() < 4 {
            return Vec::new();
        }

        let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut keys = Vec::with_capacity(count);
        let mut pos = 4;

        for _ in 0..count {
            if pos + 8 > data.len() {
                break;
            }
            let bytes = [
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ];
            keys.push(num_u64::U64Key::from_memcomparable(bytes));
            pos += 8;
        }

        keys
    }

    fn serialize_value<'a>(&self, bitmap: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        let ids: Vec<u32> = bitmap.iter().collect();
        let vec_size = 1 + ids.len() * 4;
        let bitmap_size = 1 + bitmap.serialized_size();

        let mut buf = Vec::new();
        if vec_size <= bitmap_size {
            buf.push(0u8);
            for id in ids {
                buf.extend_from_slice(&id.to_be_bytes());
            }
        } else {
            buf.push(1u8);
            if let Err(_) = bitmap.serialize_into(&mut buf) {
                buf.clear();
                buf.push(0u8);
                for id in bitmap.iter() {
                    buf.extend_from_slice(&id.to_be_bytes());
                }
            }
        }
        Cow::Owned(buf)
    }

    fn deserialize_value<'a>(&self, data: &'a [u8]) -> Result<RoaringBitmap, Box<dyn Error>> {
        if data.is_empty() {
            return Err("Empty data".into());
        }

        match data[0] {
            0 => {
                let count = (data.len() - 1) / 4;
                let mut ids = Vec::with_capacity(count);
                for i in 0..count {
                    let offset = 1 + i * 4;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let id = u32::from_be_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]);
                    ids.push(id);
                }
                Ok(RoaringBitmap::from_sorted_iter(ids.into_iter())?)
            }
            1 => Ok(RoaringBitmap::deserialize_from(&data[1..])?),
            _ => Err("Unknown type flag".into()),
        }
    }
}

/// Serializer for F64Key (memcomparable f64) with RoaringBitmap values
#[derive(Clone)]
pub struct F64RoaringSerializer {
    _zstd_level: i32,
}

impl F64RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self {
            _zstd_level: zstd_level,
        }
    }

    pub fn default() -> Self {
        Self { _zstd_level: 3 }
    }
}

impl persist::KeySerializer<num_f64::F64Key, RoaringBitmap> for F64RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<num_f64::F64Key>) -> Cow<'a, [u8]> {
        let mut buf = Vec::with_capacity(4 + keys.len() * 8);
        buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());

        for key in keys {
            buf.extend_from_slice(&key.to_memcomparable());
        }

        Cow::Owned(buf)
    }

    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<num_f64::F64Key> {
        if data.len() < 4 {
            return Vec::new();
        }

        let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut keys = Vec::with_capacity(count);
        let mut pos = 4;

        for _ in 0..count {
            if pos + 8 > data.len() {
                break;
            }
            let bytes = [
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ];
            keys.push(num_f64::F64Key::from_memcomparable(bytes));
            pos += 8;
        }

        keys
    }

    fn serialize_value<'a>(&self, bitmap: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        let ids: Vec<u32> = bitmap.iter().collect();
        let vec_size = 1 + ids.len() * 4;
        let bitmap_size = 1 + bitmap.serialized_size();

        let mut buf = Vec::new();
        if vec_size <= bitmap_size {
            buf.push(0u8);
            for id in ids {
                buf.extend_from_slice(&id.to_be_bytes());
            }
        } else {
            buf.push(1u8);
            if let Err(_) = bitmap.serialize_into(&mut buf) {
                buf.clear();
                buf.push(0u8);
                for id in bitmap.iter() {
                    buf.extend_from_slice(&id.to_be_bytes());
                }
            }
        }
        Cow::Owned(buf)
    }

    fn deserialize_value<'a>(&self, data: &'a [u8]) -> Result<RoaringBitmap, Box<dyn Error>> {
        if data.is_empty() {
            return Err("Empty data".into());
        }

        match data[0] {
            0 => {
                let count = (data.len() - 1) / 4;
                let mut ids = Vec::with_capacity(count);
                for i in 0..count {
                    let offset = 1 + i * 4;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let id = u32::from_be_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]);
                    ids.push(id);
                }
                Ok(RoaringBitmap::from_sorted_iter(ids.into_iter())?)
            }
            1 => Ok(RoaringBitmap::deserialize_from(&data[1..])?),
            _ => Err("Unknown type flag".into()),
        }
    }
}

// pub trait PrimaryKey<K> {
//     fn get(&self, key: &K) -> Option<u32>;
// }

pub trait IndexWriter: Sync + 'static {
    fn name(&self) -> &str;
    fn field_type(&self) -> FieldType;
    fn write(&self, data: &RecordBatch) -> CoreResult<()>;
    fn mget_internal_id(&self, pk_filter: &RwLock<RoaringBitmap>, column: &ArrayRef) -> Vec<u32>;
    fn as_any(&self) -> &dyn Any;
}

pub trait PkWriter: Sync + 'static {
    // write primary key and return the need delete ids
    fn write_pk(
        &self,
        data: &RecordBatch,
        info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<HashSet<u32>>;
}

#[derive(Clone)]
pub(crate) enum InvertedIndex<K> {
    Disk(Arc<persist::TreeReader<K, RoaringBitmap>>),
    Memory(BTree<K, Arc<RwLock<Vec<u32>>>>),
}

impl<K: Clone + Ord> InvertedIndex<K> {
    pub fn new_memory(size: usize) -> InvertedIndex<K> {
        InvertedIndex::Memory(BTree::new(size))
    }

    pub fn new_disk<S>(path: &str, serializer: S) -> CoreResult<InvertedIndex<K>>
    where
        K: Clone,
        S: persist::KeySerializer<K, RoaringBitmap> + 'static,
    {
        let reader = persist::TreeReader::new(std::path::Path::new(path), Box::new(serializer))
            .map_err(|e| crate::utils::error::CoreError::IOError(e.to_string()))?;
        Ok(InvertedIndex::Disk(Arc::new(reader)))
    }
}

/// write functions
impl<K: Clone + Ord> InvertedIndex<K> {
    pub(crate) fn extend(&mut self, k: K, ids: Vec<u32>) {
        if let InvertedIndex::Memory(tree) = self {
            match tree.get(&k) {
                Some(v) => v.write().unwrap().extend(ids),
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
impl<K: Clone + Ord> InvertedIndex<K> {
    pub(crate) fn get_bitmap(&self, k: &K) -> Option<RoaringBitmap> {
        match self {
            InvertedIndex::Disk(r) => r.get(k),
            InvertedIndex::Memory(btree) => btree.get(k).map(|v| {
                RoaringBitmap::from_sorted_iter(v.read().unwrap().iter().map(|v| *v)).unwrap()
            }),
        }
    }

    pub(crate) fn memory_get_ref(&self, k: &K) -> Option<&Arc<RwLock<Vec<u32>>>> {
        match self {
            InvertedIndex::Disk(_) => unreachable!(),
            InvertedIndex::Memory(btree) => btree.get(k),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            InvertedIndex::Disk(r) => r.len() as usize,
            InvertedIndex::Memory(btree) => btree.len(),
        }
    }
}

mod test {
    #[test]
    fn test_index() {
        use super::InvertedIndex;

        let mut index = InvertedIndex::<String>::new_memory(64);

        index.insert("hello".to_string(), vec![1, 2, 3]);
        index.insert("world".to_string(), vec![4, 5, 6]);

        assert_eq!(index.len(), 2);

        index.extend("hello".to_string(), vec![7, 8, 9]);

        assert_eq!(index.len(), 2);

        let bitmap = index.get_bitmap(&"hello".to_string()).unwrap();

        assert!(bitmap.contains(1));
        assert!(bitmap.contains(9));
        assert!(!bitmap.contains(10));

        println!("bitmap: {:?}", bitmap);
    }
}
