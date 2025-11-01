use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use arrow::array::{ArrayRef, Int32Array, ListArray, RecordBatch, UInt32Array};
use roaring::RoaringBitmap;

use super::{IndexWriter, InvertedIndex, PkWriter};
use crate::{
    arrow_downcast,
    partition::WriteInfo,
    schema::field::{FieldOption, FieldType},
    utils::error::{CoreError, CoreResult},
};

/// I32 field type with inverted index
/// i32 is naturally Ord in Rust, so no wrapper needed in memory
pub struct NumI32 {
    field: FieldOption,
    indexs: RwLock<InvertedIndex<i32>>,
}

impl NumI32 {
    pub fn new(field: &FieldOption) -> Self {
        Self {
            field: field.clone(),
            indexs: RwLock::new(InvertedIndex::new_memory(64)),
        }
    }

    pub fn from_disk(field: &FieldOption, inverted_index: InvertedIndex<i32>) -> CoreResult<Self> {
        Ok(Self {
            field: field.clone(),
            indexs: RwLock::new(inverted_index),
        })
    }

    pub fn persist(&self, path: &str) -> CoreResult<Self> {
        use mem_btree::persist::TreeWriter;

        let memory_index = self.indexs.read().unwrap();
        let btree = match &*memory_index {
            InvertedIndex::Memory(tree) => tree,
            InvertedIndex::Disk(_) => {
                return Err(CoreError::Internal("Cannot persist disk index".to_string()));
            }
        };

        let len = btree.len();
        let iter = btree.iter().map(|item| {
            let (key, value_lock, _ttl) = &*item;
            let ids = value_lock.read().unwrap();
            let bitmap = RoaringBitmap::from_sorted_iter(ids.iter().copied()).unwrap();
            Arc::new((*key, bitmap, None))
        });

        let serializer = I32RoaringSerializer::default();
        let writer = TreeWriter::new(
            std::path::PathBuf::from(path),
            128,
            4, // i32 = 4 bytes
        );

        writer
            .persist(len, Box::new(serializer), iter)
            .map_err(|e| CoreError::IOError(e.to_string()))?;

        let disk_index = InvertedIndex::new_disk(path, I32RoaringSerializer::default())?;

        Ok(Self {
            field: self.field.clone(),
            indexs: RwLock::new(disk_index),
        })
    }
}

impl IndexWriter for NumI32 {
    fn write(&self, data: &RecordBatch) -> CoreResult<()> {
        let Some(arr) = data.column_by_name(&self.field.name()) else {
            return Ok(());
        };

        let mut mtp: HashMap<i32, Vec<u32>> = HashMap::new();

        if self.field.is_array() {
            for (a, id) in arrow_downcast!(arr, ListArray)
                .iter()
                .zip(arrow_downcast!(data.column(0), UInt32Array).iter())
            {
                if let (Some(a), Some(id)) = (a, id) {
                    for v in arrow_downcast!(a, Int32Array).iter().flatten() {
                        if let Some(list) = mtp.get_mut(&v) {
                            if list.last() != Some(&id) {
                                list.push(id);
                            }
                        } else {
                            mtp.insert(v, vec![id]);
                        }
                    }
                }
            }
        } else {
            for (a, id) in arrow_downcast!(arr, Int32Array)
                .iter()
                .zip(arrow_downcast!(data.column(0), UInt32Array).iter())
            {
                if let (Some(v), Some(id)) = (a, id) {
                    if let Some(list) = mtp.get_mut(&v) {
                        if list.last() != Some(&id) {
                            list.push(id);
                        }
                    } else {
                        mtp.insert(v, vec![id]);
                    }
                }
            }
        }

        let mut indexs = self.indexs.read().unwrap().clone();

        for (k, ids) in mtp {
            indexs.extend(k, ids);
        }

        *self.indexs.write().unwrap() = indexs;

        Ok(())
    }

    fn name(&self) -> &str {
        &self.field.name()
    }

    fn field_type(&self) -> FieldType {
        FieldType::I32
    }

    fn mget_internal_id(&self, _pk_filter: &RwLock<RoaringBitmap>, _column: &ArrayRef) -> Vec<u32> {
        vec![]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PkWriter for NumI32 {
    fn write_pk(
        &self,
        data: &RecordBatch,
        _info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<HashSet<u32>> {
        let mut cur_dels = HashSet::new();

        let pk = data.column_by_name(self.name()).ok_or_else(|| {
            CoreError::Internal(format!(
                "field:{:?} not found in recordbatch",
                self.field.name()
            ))
        })?;

        let mut mtp: HashMap<i32, Vec<u32>> = HashMap::new();
        let internal_id_array = arrow_downcast!(data.column(0), UInt32Array);

        for (a, id) in arrow_downcast!(pk, Int32Array)
            .iter()
            .zip(internal_id_array.iter())
        {
            if let (Some(v), Some(id)) = (a, id) {
                if let Some(list) = mtp.get_mut(&v) {
                    if !list.is_empty() {
                        cur_dels.extend(list.clone());
                    }
                    list[0] = id;
                } else {
                    mtp.insert(v, vec![id]);
                }
            }
        }

        let _lock = lock.write().unwrap();

        let indexs = self.indexs.read().unwrap();

        for (k, _ids) in mtp.iter() {
            if let Some(prev_ids) = indexs.memory_get_ref(k) {
                let prev = prev_ids.read().unwrap();
                cur_dels.extend(prev.iter().copied());
            }
        }

        drop(indexs);

        let mut indexs = self.indexs.write().unwrap();

        for (k, ids) in mtp {
            indexs.insert(k, ids);
        }

        Ok(cur_dels)
    }
}

/// Serializer for I32Key with RoaringBitmap values
/// Handles key serialization using memcomparable encoding
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

impl mem_btree::persist::KeySerializer<i32, RoaringBitmap, RoaringBitmap> for I32RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<i32>) -> std::borrow::Cow<'a, [u8]> {
        use mem_btree::persist::num_ser::i64_coder;

        // Keys are already sorted from BTree iteration
        // Use delta encoding for better compression
        let keys_i64: Vec<i64> = keys.iter().map(|&k| k as i64).collect();

        let mut buf = Vec::new();
        // Keys are strictly sorted; use delta encoding to reduce overhead
        i64_coder::write_delta(&mut buf, &keys_i64).expect("Failed to serialize keys");

        std::borrow::Cow::Owned(buf)
    }

    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<i32> {
        use mem_btree::persist::num_ser::i64_coder;

        let mut pos = 0;
        // Pass a reference to the slice so it matches BufferRead for &[u8]
        // Decode with position-aware delta reader to advance pos correctly
        let keys_i64 = i64_coder::read_delta_pos(&data, &mut pos);
        keys_i64.iter().map(|&k| k as i32).collect()
    }
    fn serialize_value<'a>(&self, bitmap: &'a RoaringBitmap) -> std::borrow::Cow<'a, [u8]> {
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
        std::borrow::Cow::Owned(buf)
    }

    fn deserialize_value<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<RoaringBitmap, Box<dyn std::error::Error>> {
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
