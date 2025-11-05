use std::{any::Any, collections::HashMap, sync::RwLock};

use datafusion::arrow::array::{Int64Array, RecordBatch, UInt32Array};
use roaring::RoaringBitmap;

use crate::{
    arrow_downcast,
    schema::field::{FieldOption, FieldType},
    segment::field_store::{IndexReader, IndexWriter, InvertedIndex},
    utils::error::{CoreError, CoreResult},
};

pub struct NumI64 {
    field: FieldOption,
    indexs: RwLock<InvertedIndex<i64>>,
}

impl NumI64 {
    pub fn new(field: &FieldOption) -> Self {
        Self {
            field: field.clone(),
            indexs: RwLock::new(InvertedIndex::new_memory(64)),
        }
    }

    pub fn from_disk(field: &FieldOption, field_path: &str) -> CoreResult<Self> {
        match field {
            FieldOption::I64 { .. } => {
                let zstd_level = field.zstd_level();
                let inverted_index = InvertedIndex::new_disk(
                    &field_path,
                    super::I64RoaringSerializer::new(zstd_level),
                )?;

                Ok(Self {
                    field: field.clone(),
                    indexs: RwLock::new(inverted_index),
                })
            }
            _ => Err(CoreError::Internal("FieldOption must be I64".to_string())),
        }
    }

    pub fn persist(&self, path: &str) -> CoreResult<Self> {
        use super::I64RoaringSerializer;
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

        let serializer = I64RoaringSerializer::new(zstd_level);
        let writer = TreeWriter::new(
            std::path::PathBuf::from(path),
            chunk_size,
            8, // i64 = 8 bytes
        );

        writer
            .persist::<i64, RoaringBitmap, RoaringBitmap>(len, Box::new(serializer), iter)
            .map_err(|e| CoreError::IOError(e.to_string()))?;

        let disk_index = InvertedIndex::new_disk(path, I64RoaringSerializer::default())?;

        Ok(Self {
            field: self.field.clone(),
            indexs: RwLock::new(disk_index),
        })
    }

    pub fn query(&self, value: i64) -> Option<RoaringBitmap> {
        let indexs = self.indexs.read().unwrap();
        indexs.get_bitmap(&value)
    }

    /// Range query with support for open/closed intervals
    ///
    /// # Arguments
    /// * `start` - Optional start bound
    /// * `start_inclusive` - Whether start bound is inclusive (>=) or exclusive (>)
    /// * `end` - Optional end bound
    /// * `end_inclusive` - Whether end bound is inclusive (<=) or exclusive (<)
    pub fn range_query(
        &self,
        start: Option<i64>,
        start_inclusive: bool,
        end: Option<i64>,
        end_inclusive: bool,
    ) -> RoaringBitmap {
        let indexs = self.indexs.read().unwrap();
        indexs.range_query(start.as_ref(), start_inclusive, end.as_ref(), end_inclusive)
    }
}

impl IndexWriter for NumI64 {
    fn write(&self, data: &RecordBatch) -> CoreResult<()> {
        let Some(arr) = data.column_by_name(&self.field.name()) else {
            return Ok(());
        };

        let mut mtp: HashMap<i64, Vec<u32>> = HashMap::new();

        for (value, id) in arrow_downcast!(arr, Int64Array)
            .iter()
            .zip(arrow_downcast!(data.column(0), UInt32Array).iter())
        {
            if let (Some(value), Some(id)) = (value, id) {
                if let Some(list) = mtp.get_mut(&value) {
                    if list.last() != Some(&id) {
                        list.push(id);
                    }
                } else {
                    mtp.insert(value, vec![id]);
                }
            }
        }

        let mut indexs = self.indexs.write().unwrap();
        for (k, ids) in mtp.into_iter() {
            indexs.append_ids(k, ids);
        }

        Ok(())
    }

    fn field_type(&self) -> FieldType {
        FieldType::I64
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

impl IndexReader for NumI64 {
    fn name(&self) -> &str {
        self.field.name()
    }

    fn field_type(&self) -> FieldType {
        FieldType::I64
    }

    fn query(&self, value: &datafusion::scalar::ScalarValue) -> Option<RoaringBitmap> {
        use datafusion::scalar::ScalarValue;

        match value {
            ScalarValue::Int64(Some(v)) => {
                let indexs = self.indexs.read().unwrap();
                indexs.get_bitmap(v)
            }
            _ => None,
        }
    }

    fn range(
        &self,
        start: &datafusion::scalar::ScalarValue,
        start_inclusive: bool,
        end: &datafusion::scalar::ScalarValue,
        end_inclusive: bool,
    ) -> Option<RoaringBitmap> {
        use datafusion::scalar::ScalarValue;

        let indexs = self.indexs.read().unwrap();

        let start_val = match start {
            ScalarValue::Int64(Some(v)) => Some(*v),
            _ => None,
        };

        let end_val = match end {
            ScalarValue::Int64(Some(v)) => Some(*v),
            _ => None,
        };

        Some(indexs.range_query(
            start_val.as_ref(),
            start_inclusive,
            end_val.as_ref(),
            end_inclusive,
        ))
    }
}
