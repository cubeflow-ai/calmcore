use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use arrow::array::{ArrayRef, ListArray, RecordBatch, UInt32Array};
use roaring::RoaringBitmap;

use crate::{
    arrow_downcast,
    partition::WriteInfo,
    schema::field::{FieldOption, FieldType},
    segment::field_store::{IndexWriter, InvertedIndex, PkWriter},
    utils::error::{CoreError, CoreResult},
};

/// U32 field type with inverted index
/// Uses memcomparable encoding (which is just big-endian for unsigned)
pub struct NumU32 {
    field: FieldOption,
    indexs: RwLock<InvertedIndex<U32Key>>,
}

/// Wrapper for u32 - already has natural ordering
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct U32Key(pub u32);

impl U32Key {
    /// Convert u32 to memcomparable bytes (big-endian)
    pub fn to_memcomparable(&self) -> [u8; 4] {
        self.0.to_be_bytes()
    }

    /// Convert memcomparable bytes back to u32
    pub fn from_memcomparable(bytes: [u8; 4]) -> Self {
        U32Key(u32::from_be_bytes(bytes))
    }
}

impl std::fmt::Display for U32Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl NumU32 {
    pub fn new(field: &FieldOption) -> Self {
        Self {
            field: field.clone(),
            indexs: RwLock::new(InvertedIndex::new_memory(64)),
        }
    }

    pub fn from_disk(
        field: &FieldOption,
        inverted_index: InvertedIndex<U32Key>,
    ) -> CoreResult<Self> {
        Ok(Self {
            field: field.clone(),
            indexs: RwLock::new(inverted_index),
        })
    }

    pub fn persist(&self, path: &str) -> CoreResult<Self> {
        use super::U32RoaringSerializer;
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

        let serializer = U32RoaringSerializer::default();
        let writer = TreeWriter::new(
            std::path::PathBuf::from(path),
            128,
            4, // u32 = 4 bytes
        );

        writer
            .persist(len, Box::new(serializer), iter)
            .map_err(|e| CoreError::IOError(e.to_string()))?;

        let disk_index = InvertedIndex::new_disk(path, U32RoaringSerializer::default())?;

        Ok(Self {
            field: self.field.clone(),
            indexs: RwLock::new(disk_index),
        })
    }
}

impl IndexWriter for NumU32 {
    fn write(&self, data: &RecordBatch) -> CoreResult<()> {
        let Some(arr) = data.column_by_name(&self.field.name()) else {
            return Ok(());
        };

        let mut mtp: HashMap<U32Key, Vec<u32>> = HashMap::new();

        if self.field.is_array() {
            for (a, id) in arrow_downcast!(arr, ListArray)
                .iter()
                .zip(arrow_downcast!(data.column(0), UInt32Array).iter())
            {
                if let (Some(a), Some(id)) = (a, id) {
                    for v in arrow_downcast!(a, UInt32Array).iter().flatten() {
                        let key = U32Key(v);
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
            for (a, id) in arrow_downcast!(arr, UInt32Array)
                .iter()
                .zip(arrow_downcast!(data.column(0), UInt32Array).iter())
            {
                if let (Some(v), Some(id)) = (a, id) {
                    let key = U32Key(v);
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
        FieldType::U32
    }

    fn mget_internal_id(&self, _pk_filter: &RwLock<RoaringBitmap>, _column: &ArrayRef) -> Vec<u32> {
        vec![]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PkWriter for NumU32 {
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

        let mut mtp: HashMap<U32Key, Vec<u32>> = HashMap::new();
        let internal_id_array = arrow_downcast!(data.column(0), UInt32Array);

        for (a, id) in arrow_downcast!(pk, UInt32Array)
            .iter()
            .zip(internal_id_array.iter())
        {
            if let (Some(v), Some(id)) = (a, id) {
                let key = U32Key(v);
                if let Some(list) = mtp.get_mut(&key) {
                    if !list.is_empty() {
                        cur_dels.extend(list.clone());
                    }
                    list[0] = id;
                } else {
                    mtp.insert(key, vec![id]);
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
