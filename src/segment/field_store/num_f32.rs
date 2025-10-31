use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use arrow::array::{ArrayRef, Float32Array, ListArray, RecordBatch, UInt32Array};
use roaring::RoaringBitmap;

use crate::{
    arrow_downcast,
    partition::WriteInfo,
    schema::field::{FieldOption, FieldType},
    segment::field_store::{IndexWriter, InvertedIndex, PkWriter},
    utils::error::{CoreError, CoreResult},
};

/// F32 field type with inverted index
/// Uses memcomparable encoding for sorted storage
pub struct NumF32 {
    field: FieldOption,
    indexs: RwLock<InvertedIndex<F32Key>>,
}

/// Wrapper for f32 that implements Ord for BTree storage
/// Uses memcomparable encoding to maintain proper sort order
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct F32Key(pub f32);

impl F32Key {
    /// Convert f32 to memcomparable bytes
    /// NaN -> 0x00...
    /// -Inf -> 0x00...01
    /// negative -> flip all bits
    /// positive -> flip sign bit
    /// +Inf -> 0xFF...FE
    pub fn to_memcomparable(&self) -> [u8; 4] {
        let value = self.0;

        // Handle NaN
        if value.is_nan() {
            return [0x00, 0x00, 0x00, 0x00];
        }

        let bits = value.to_bits();
        let encoded = if value >= 0.0 {
            // Positive: flip sign bit
            bits ^ 0x80000000
        } else {
            // Negative: flip all bits
            !bits
        };

        encoded.to_be_bytes()
    }

    /// Convert memcomparable bytes back to f32
    pub fn from_memcomparable(bytes: [u8; 4]) -> Self {
        let encoded = u32::from_be_bytes(bytes);

        let bits = if encoded & 0x80000000 != 0 {
            // Was positive: flip sign bit back
            encoded ^ 0x80000000
        } else {
            // Was negative: flip all bits back
            !encoded
        };

        F32Key(f32::from_bits(bits))
    }
}

impl Eq for F32Key {}

impl PartialOrd for F32Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for F32Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to_memcomparable().cmp(&other.to_memcomparable())
    }
}

impl std::hash::Hash for F32Key {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.to_memcomparable().hash(state);
    }
}

impl std::fmt::Display for F32Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl NumF32 {
    pub fn new(field: &FieldOption) -> Self {
        Self {
            field: field.clone(),
            indexs: RwLock::new(InvertedIndex::new_memory(64)),
        }
    }

    /// Create a NumF32 from disk-based inverted index
    pub fn from_disk(
        field: &FieldOption,
        inverted_index: InvertedIndex<F32Key>,
    ) -> CoreResult<Self> {
        Ok(Self {
            field: field.clone(),
            indexs: RwLock::new(inverted_index),
        })
    }

    /// Persist the in-memory index to disk and return a new NumF32 with disk-based index
    pub fn persist(&self, path: &str) -> CoreResult<Self> {
        use super::F32RoaringSerializer;
        use mem_btree::persist::TreeWriter;

        // 1. Extract the memory index
        let memory_index = self.indexs.read().unwrap();
        let btree = match &*memory_index {
            InvertedIndex::Memory(tree) => tree,
            InvertedIndex::Disk(_) => {
                return Err(CoreError::Internal("Cannot persist disk index".to_string()));
            }
        };

        // 2. Convert BTree<F32Key, Vec<u32>> to Vec of (F32Key, RoaringBitmap)
        let len = btree.len();
        let iter = btree.iter().map(|item| {
            let (key, value_lock, _ttl) = &*item;
            let ids = value_lock.read().unwrap();
            let bitmap = RoaringBitmap::from_sorted_iter(ids.iter().copied()).unwrap();
            Arc::new((*key, bitmap, None))
        });

        // 3. Persist to disk
        let serializer = F32RoaringSerializer::default();
        let writer = TreeWriter::new(
            std::path::PathBuf::from(path),
            128, // chunk_size
            4,   // key_len (f32 = 4 bytes in memcomparable format)
        );

        writer
            .persist(len, Box::new(serializer), iter)
            .map_err(|e| CoreError::IOError(e.to_string()))?;

        // 4. Create new NumF32 with disk index
        let disk_index = InvertedIndex::new_disk(path, F32RoaringSerializer::default())?;

        Ok(Self {
            field: self.field.clone(),
            indexs: RwLock::new(disk_index),
        })
    }
}

impl IndexWriter for NumF32 {
    fn write(&self, data: &RecordBatch) -> CoreResult<()> {
        let Some(arr) = data.column_by_name(&self.field.name()) else {
            return Ok(());
        };

        let mut mtp: HashMap<F32Key, Vec<u32>> = HashMap::new();

        if self.field.is_array() {
            // Handle array of f32
            for (a, id) in arrow_downcast!(arr, ListArray)
                .iter()
                .zip(arrow_downcast!(data.column(0), UInt32Array).iter())
            {
                if let (Some(a), Some(id)) = (a, id) {
                    for v in arrow_downcast!(a, Float32Array).iter().flatten() {
                        let key = F32Key(v);
                        if let Some(list) = mtp.get_mut(&key) {
                            if list.last() != Some(&id) {
                                list.push(id);
                            }
                        } else {
                            mtp.insert(key, vec![id]);
                        }
                    }
                }
            }
        } else {
            // Handle single f32
            for (a, id) in arrow_downcast!(arr, Float32Array)
                .iter()
                .zip(arrow_downcast!(data.column(0), UInt32Array).iter())
            {
                if let (Some(v), Some(id)) = (a, id) {
                    let key = F32Key(v);
                    if let Some(list) = mtp.get_mut(&key) {
                        if list.last() != Some(&id) {
                            list.push(id);
                        }
                    } else {
                        mtp.insert(key, vec![id]);
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
        FieldType::F32
    }

    fn mget_internal_id(&self, _pk_filter: &RwLock<RoaringBitmap>, _column: &ArrayRef) -> Vec<u32> {
        // F32 cannot be primary key
        vec![]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PkWriter for NumF32 {
    fn write_pk(
        &self,
        _data: &RecordBatch,
        _info: Option<WriteInfo>,
        _lock: &RwLock<()>,
    ) -> CoreResult<HashSet<u32>> {
        // F32 cannot be used as primary key
        Err(CoreError::Internal(
            "F32 field cannot be used as primary key".to_string(),
        ))
    }
}
