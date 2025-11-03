use std::{
    any::Any,
    borrow::Cow,
    collections::HashSet,
    error::Error,
    sync::{Arc, RwLock},
};

use arrow::array::{ArrayRef, RecordBatch};
use mem_btree::{
    persist::{self, num_ser},
    BTree,
};

use roaring::RoaringBitmap;

use crate::{partition::WriteInfo, schema::field::FieldType, utils::error::CoreResult};

pub mod keyword;
pub mod num_f32;
pub mod num_f64;
pub mod num_i32;
pub mod num_i64;
pub mod num_u32;
pub mod serializer;
// pub mod num_u64; // removed per design: u64 field not needed currently

// Re-export StringRoaringSerializer from keyword module
pub use keyword::StringRoaringSerializer;

// Re-export numeric serializers
pub use serializer::{
    F32RoaringSerializer, F64RoaringSerializer, I32RoaringSerializer, I64RoaringSerializer,
    U32RoaringSerializer,
};

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

impl persist::WriteSerializer<u32, RecordBatch> for U32RecordBatchSerializer {
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
}

impl persist::ReadSerializer<u32, RecordBatch> for U32RecordBatchSerializer {
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
        S: persist::ReadSerializer<u32, RecordBatch> + 'static,
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
impl<K: Clone + PartialOrd + Ord> InvertedIndex<K> {
    pub(crate) fn get_bitmap(&self, k: &K) -> Option<RoaringBitmap> {
        match self {
            InvertedIndex::Disk(r) => r.get(k),
            InvertedIndex::Memory(btree) => btree.get(k).map(|v| {
                RoaringBitmap::from_sorted_iter(v.read().unwrap().iter().copied()).unwrap()
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

/// Encode a list of u32 doc IDs into bytes with a leading marker byte.
/// - 0: delta-encoded u32 sequence (big-endian, mem_btree::persist::num_ser::u32_coder)
/// - 1: RoaringBitmap native serialization
/// Returns Vec<u8> on success.
pub fn encode_roaring_from_u32s(ids: &[u32]) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    if ids.len() < 1000 {
        // marker 0 => delta encoding
        out.push(0u8);
        num_ser::u32_coder::write_delta(&mut out, &ids.to_vec())?;
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
    if let Ok(_) = num_ser::u32_coder::write_delta(&mut delta_buf, &ids) {
        // Try roaring native
        let mut roaring_buf = Vec::new();
        roaring_buf.push(1u8);
        if let Ok(_) = bitmap.serialize_into(&mut roaring_buf) {
            // Choose the smaller one
            if delta_buf.len() <= roaring_buf.len() {
                return delta_buf;
            } else {
                return roaring_buf;
            }
        } else {
            // Roaring serialization failed, use delta
            return delta_buf;
        }
    } else {
        // Delta encoding failed (shouldn't happen), fallback to roaring
        let mut buf = Vec::new();
        buf.push(1u8);
        let _ = bitmap.serialize_into(&mut buf);
        return buf;
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
