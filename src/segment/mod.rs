mod field_store;

use crate::{
    partition::WriteInfo,
    schema::{field::FieldOption, Schema},
    segment::field_store::{keyword::Keyword, IndexWriter, PkWriter},
    utils::error::{CoreError, CoreResult},
};
use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, UInt32Array},
    datatypes::{DataType, Field, SchemaRef},
};
use core::num;
use itertools::Itertools;
use log::error;
use mem_btree::BTree;
use parquet::schema;
use rand::seq::index;
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::{
    any::Any,
    collections::{hash_map::DefaultHasher, HashMap, LinkedList},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicU32, Ordering},
        mpsc, Arc, RwLock,
    },
};

enum SegmentStatus {
    Forzen,
    Active,
}

pub struct Segment {
    pub start: u64,
    doc_id_gen: AtomicU32,
    status: SegmentStatus,
    pk_bloomfilter: RwLock<RoaringBitmap>,
    deleted: RwLock<RoaringBitmap>,
    fields: Vec<Box<dyn IndexWriter>>,
    field_index: HashMap<String, usize>,
    schema: Arc<Schema>,
    row_data: RwLock<BTree<u32, RecordBatch>>,
}

impl Segment {
    pub fn new(start: u64, schema: Arc<Schema>) -> Self {
        let fields = schema
            .fields
            .iter()
            .map(|f| match f {
                FieldOption::Keyword { .. } => Box::new(Keyword::new(f)) as Box<dyn IndexWriter>,
            })
            .collect();

        let field_index = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().to_string(), i))
            .collect();

        Self {
            start,
            doc_id_gen: AtomicU32::new(0),
            status: SegmentStatus::Active,
            pk_bloomfilter: RwLock::default(),
            deleted: RwLock::default(),
            fields,
            field_index: field_index,
            schema: schema,
            row_data: RwLock::new(BTree::new(32)),
        }
    }

    fn index_write(&mut self, schema: SchemaRef, data: RecordBatch) {
        let num_rows = data.num_rows() as u32;

        // generate auto-increment id column
        let start_id = self.doc_id_gen.load(Ordering::Relaxed);

        // concatenate auto-increment id column to RecordBatch (insert into the first column)
        let old_columns = data.columns();
        let mut columns = Vec::with_capacity(old_columns.len() + 1);
        columns.push(
            Arc::new(UInt32Array::from_iter_values(start_id..start_id + num_rows)) as ArrayRef,
        );
        columns.extend_from_slice(old_columns);

        let new_batch = RecordBatch::try_new(schema, columns).unwrap();

        // update doc_id_gen
        self.doc_id_gen
            .store(start_id + num_rows, Ordering::Relaxed);

        // self.fields
        //     .par_iter()
        //     //filter pk ,because already index
        //     .map(|f| {
        //         self.schema
        //             .primary_key
        //             .as_ref()
        //             .map(|pk| pk.eq(f.name()))
        //             .unwrap_or_default()
        //     })
        //     .for_each(|f| {
        //         if let Err(e) = f.write(&new_batch) {
        //             log::error!("write field:{:?} failed: {:?}", f.name(), e);
        //         }
        //     });
    }

    pub fn write(
        &self,
        data: &RecordBatch,
        pk_hash: Option<Vec<u32>>,
        info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<Vec<u32>> {
        // add id column
        let mut columns = Vec::with_capacity(data.schema().fields().len() + 1);
        columns.push(Field::new("_internal_id", DataType::UInt32, false));
        columns.extend(data.schema().flattened_fields().into_iter().cloned());
        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(columns));

        let num_rows = data.num_rows() as u32;
        // generate auto-increment id column
        let start_id = self.doc_id_gen.load(Ordering::Relaxed);
        // concatenate auto-increment id column to RecordBatch (insert into the first column)
        let old_columns = data.columns();
        let mut columns = Vec::with_capacity(old_columns.len() + 1);

        let result = (start_id..start_id + num_rows).collect_vec();

        columns.push(Arc::new(UInt32Array::from_iter_values(result.iter().cloned())) as ArrayRef);
        columns.extend_from_slice(old_columns);

        let new_data = RecordBatch::try_new(arrow_schema, columns).unwrap();

        self.doc_id_gen
            .store(start_id + num_rows, Ordering::Relaxed);

        if let Some(pk_field) = self
            .schema
            .primary_key
            .as_ref()
            .and_then(|k| self.field_index.get(k))
            .and_then(|i| Some(&self.fields[*i]))
        {
            self.write_pk(pk_field, &new_data, pk_hash, info, lock)?;
        }

        self.row_data.write().unwrap().put(start_id, new_data);

        Ok(result)
    }

    fn write_pk(
        &self,
        pk_field: &Box<dyn IndexWriter>,
        data: &RecordBatch,
        pk_hash: Option<Vec<u32>>,
        info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<()> {
        use crate::schema::field::FieldType;

        match pk_field.field_type() {
            FieldType::Keyword => {
                let pk_writer = pk_field.as_any().downcast_ref::<Keyword>().ok_or_else(|| {
                    CoreError::Internal(format!(
                        "field:{:?} field_type:{:?} does not implement PkWriter",
                        pk_field.name(),
                        pk_field.field_type()
                    ))
                })?;

                let del = pk_writer.write_pk(data, info, lock)?;
                if !del.is_empty() {
                    self.deleted.write().unwrap().extend(del.iter());
                }

                if let Some(hashes) = pk_hash {
                    self.pk_bloomfilter.write().unwrap().extend(hashes);
                }
            }
            _ => {
                return Err(CoreError::InvalidParam(format!(
                    "field:{:?} type not support pk: {:?}",
                    pk_field.name(),
                    pk_field.field_type()
                )))
            }
        }

        Ok(())
    }

    /// Get internal ids by primary key hash and primary key column
    /// if not found, return None, never return empty vector
    /// internal id is the row number in the segment + start
    pub(crate) fn mget_internal_id(
        &self,
        pk_hash: Option<&Vec<u32>>,
        column: &ArrayRef,
    ) -> Option<Vec<u32>> {
        let pk_hash = pk_hash.unwrap();

        println!("==========={}", self.pk_bloomfilter.read().unwrap().len());

        //id filter first
        let active = pk_hash
            .iter()
            .any(|v| self.pk_bloomfilter.read().unwrap().contains(*v));

        // not found any id in this segment
        if !active {
            return None;
        }

        let index = *self
            .field_index
            .get(self.schema.primary_key.as_ref().unwrap())
            .unwrap();

        let ids = {
            let ids = self.fields[index].mget_internal_id(&self.pk_bloomfilter, column);

            let del_guard = self.deleted.read().unwrap();

            if del_guard.is_empty() {
                ids
            } else {
                ids.into_iter()
                    .filter_map(|v| if del_guard.contains(v) { None } else { Some(v) })
                    .collect_vec()
            }
        };

        if ids.is_empty() {
            None
        } else {
            Some(ids)
        }
    }

    fn mark_del(&self, ids: Vec<u32>) {
        self.deleted.write().unwrap().extend(ids);
    }

    pub(crate) fn total_count(&self) -> u64 {
        let data = self.row_data.read().unwrap().clone();
        let mut sum = 0;
        for v in data.iter() {
            sum += v.1.num_rows() as u64;
        }
        sum - self.deleted.read().unwrap().len() as u64
    }
}
