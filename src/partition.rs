use std::{
    collections::LinkedList,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};

use arrow::array::{ArrayRef, RecordBatch};
use parquet::{column, schema};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{
    schema::Schema,
    segment::{self, Segment},
    utils::{
        arrow_utils,
        error::{CoreError, CoreResult},
    },
};

// pk_index, segments , dele_info
pub(crate) struct WriteInfo(pub Arc<Vec<Segment>>, pub Vec<(usize, Vec<u32>)>);

pub struct Partition {
    pub id: u32,
    base_dir: PathBuf,
    schema: Arc<Schema>,
    current_segment: RwLock<Segment>,
    segments: RwLock<Arc<Vec<Segment>>>,
    read_lock: RwLock<()>,
    write_lock: Mutex<()>,
}

impl Partition {
    pub fn new(id: u32, base_dir: PathBuf, schema: Schema) -> Self {
        let schema = Arc::new(schema);
        Partition {
            id,
            base_dir,
            schema: schema.clone(),
            current_segment: RwLock::new(Segment::new(0, schema)),
            segments: RwLock::new(Arc::new(vec![])),
            read_lock: RwLock::new(()),
            write_lock: Mutex::new(()),
        }
    }

    pub fn upsert_json(&self, data: &[serde_json::Value]) -> CoreResult<Vec<u64>> {
        todo!()
    }

    pub fn upsert_parquet(&self, path: &Path) {
        todo!()
    }

    pub fn upsert(&self, data: RecordBatch) -> CoreResult<Vec<u64>> {
        let _write_guard = self.write_lock.lock().unwrap();

        let mut pk_hash = None;

        let info = if let Some(pk) = self.schema.primary_key.as_ref() {
            let column = data.column_by_name(pk).ok_or_else(|| {
                CoreError::InvalidParam(format!("primary key:{:?} doesn't exist in data", pk))
            })?;

            let segments = self.segments.read().unwrap().clone();

            pk_hash = Some(arrow_utils::array_to_hash(column));

            let dels: Vec<(usize, Vec<u32>)> = segments
                .par_iter()
                .enumerate()
                .filter_map(|(i, segment)| {
                    segment
                        .mget_internal_id(pk_hash.as_ref(), column)
                        .map(|ids| (i, ids))
                })
                .collect();
            if dels.is_empty() {
                None
            } else {
                Some(WriteInfo(segments, dels))
            }
        } else {
            None
        };

        self.write(&data, pk_hash, info)
    }

    pub fn persist(&self) {
        todo!()
    }

    fn write(
        &self,
        data: &RecordBatch,
        pk_hash: Option<Vec<u32>>,
        info: Option<WriteInfo>,
    ) -> CoreResult<Vec<u64>> {
        let current_segment = self.current_segment.read().unwrap();
        current_segment
            .write(data, pk_hash, info, &self.read_lock)
            .map(|ids| {
                ids.into_iter()
                    .map(|v| v as u64 + current_segment.start)
                    .collect()
            })
    }

    pub fn total_count(&self) -> u64 {
        let count: u64 = self
            .segments
            .read()
            .unwrap()
            .iter()
            .map(|s| s.total_count())
            .sum();
        count + self.current_segment.read().unwrap().total_count()
    }
}
