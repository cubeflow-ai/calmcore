use std::{any::Any, collections::HashMap, sync::RwLock};

use datafusion::arrow::array::{Float64Array, RecordBatch, UInt32Array};
use roaring::RoaringBitmap;

use crate::{
    arrow_downcast,
    schema::field::{FieldOption, FieldType},
    segment::field_store::{IndexReader, IndexWriter, InvertedIndex},
    utils::error::{CoreError, CoreResult},
};

/// Wrapper for f64 that implements Ord by comparing bit patterns
/// NaN values are treated as equal and greater than all other values
#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub struct OrderedF64(pub f64);

impl Eq for OrderedF64 {}

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or_else(|| {
            // Handle NaN: NaN == NaN, NaN > everything else
            match (self.0.is_nan(), other.0.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => unreachable!(),
            }
        })
    }
}

impl From<f64> for OrderedF64 {
    fn from(f: f64) -> Self {
        OrderedF64(f)
    }
}

impl From<OrderedF64> for f64 {
    fn from(o: OrderedF64) -> Self {
        o.0
    }
}

pub struct NumF64 {
    field: FieldOption,
    indexs: RwLock<InvertedIndex<OrderedF64>>,
}

impl Clone for NumF64 {
    fn clone(&self) -> Self {
        Self {
            field: self.field.clone(),
            indexs: RwLock::new(self.indexs.read().unwrap().clone()),
        }
    }
}

impl NumF64 {
    pub fn new(field: &FieldOption) -> Self {
        Self {
            field: field.clone(),
            indexs: RwLock::new(InvertedIndex::new_memory(64)),
        }
    }

    pub fn from_disk(field: &FieldOption, field_path: &str) -> CoreResult<Self> {
        match field {
            FieldOption::F64 { .. } => {
                let zstd_level = field.zstd_level();
                let inverted_index = InvertedIndex::new_disk(
                    &field_path,
                    super::F64RoaringSerializer::new(zstd_level),
                )?;

                Ok(Self {
                    field: field.clone(),
                    indexs: RwLock::new(inverted_index),
                })
            }
            _ => Err(CoreError::Internal("FieldOption must be F64".to_string())),
        }
    }

    pub fn persist(&self, path: &str) -> CoreResult<Self> {
        use super::F64RoaringSerializer;
        use mem_btree::persist::TreeWriter;
        use std::sync::Arc;

        let persist_opt = self.field.persist_option();
        let zstd_level = persist_opt.zstd_level;
        let chunk_size = persist_opt.chunk_size;

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

        let serializer = F64RoaringSerializer::new(zstd_level);
        let writer = TreeWriter::new(
            std::path::PathBuf::from(path),
            chunk_size,
            8, // f64 = 8 bytes
        );

        writer
            .persist::<OrderedF64, RoaringBitmap, RoaringBitmap>(len, Box::new(serializer), iter)
            .map_err(|e| CoreError::IOError(e.to_string()))?;

        let disk_index = InvertedIndex::new_disk(path, F64RoaringSerializer::default())?;

        Ok(Self {
            field: self.field.clone(),
            indexs: RwLock::new(disk_index),
        })
    }
}

impl IndexWriter for NumF64 {
    fn write(&self, data: &RecordBatch, start_id: u32) -> CoreResult<()> {
        let Some(arr) = data.column_by_name(&self.field.name()) else {
            return Ok(());
        };

        let mut mtp: HashMap<u64, Vec<u32>> = HashMap::new();

        // 使用start_id + row_idx 生成文档ID
        for (row_idx, value) in arrow_downcast!(arr, Float64Array).iter().enumerate() {
            if let Some(value) = value {
                let id = start_id + row_idx as u32;
                // Convert f64 to u64 bits for map key (ensures exact equality)
                let key = value.to_bits();
                if let Some(list) = mtp.get_mut(&key) {
                    if list.last() != Some(&id) {
                        list.push(id);
                    }
                } else {
                    mtp.insert(key, vec![id]);
                }
            }
        }

        let mut indexs = self.indexs.write().unwrap();
        for (bits, ids) in mtp.into_iter() {
            let k = OrderedF64(f64::from_bits(bits));
            indexs.append_ids(k, ids);
        }

        Ok(())
    }

    fn field_type(&self) -> FieldType {
        FieldType::F64
    }

    fn name(&self) -> &str {
        self.field.name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn mget_internal_id(&self, _column: &datafusion::arrow::array::ArrayRef) -> Vec<u32> {
        vec![]
    }
}

impl IndexReader for NumF64 {
    fn query(&self, value: &datafusion::scalar::ScalarValue) -> Option<RoaringBitmap> {
        if let datafusion::scalar::ScalarValue::Float64(Some(v)) = value {
            let indexs = self.indexs.read().unwrap();
            indexs.get_bitmap(&OrderedF64(*v))
        } else {
            None
        }
    }

    fn range(
        &self,
        start: &datafusion::scalar::ScalarValue,
        start_inclusive: bool,
        end: &datafusion::scalar::ScalarValue,
        end_inclusive: bool,
    ) -> Option<RoaringBitmap> {
        use datafusion::scalar::ScalarValue::Float64;

        let indexs = self.indexs.read().unwrap();

        let start_key = match start {
            Float64(Some(s)) => Some(OrderedF64(*s)),
            _ => None,
        };

        let end_key = match end {
            Float64(Some(e)) => Some(OrderedF64(*e)),
            _ => None,
        };

        // Use range_query with open/closed interval support
        Some(indexs.range_query(
            start_key.as_ref(),
            start_inclusive,
            end_key.as_ref(),
            end_inclusive,
        ))
    }

    fn name(&self) -> &str {
        self.field.name()
    }

    fn field_type(&self) -> FieldType {
        self.field.field_type()
    }
}
