pub mod reader;
mod writer;
use crate::{
    util::{CoreError, CoreResult},
    RecordWrapper,
};
use croaring::Bitmap;
use faiss::MetricType;
use hora::{
    core::{ann_index::ANNIndex, metrics::Metric},
    index::hnsw_idx::HNSWIndex,
};
use itertools::Itertools;
use mem_btree::{BTree, BatchWrite};
use parquet::file::page_index::index;
use proto::core::{value::Kind, Hit, Value};
use rayon::iter::IntoParallelRefIterator;
use rayon::prelude::*;
use std::{
    collections::BinaryHeap,
    path::PathBuf,
    sync::{Arc, RwLock},
};

pub struct VectorIndex {
    inner: Arc<proto::core::Field>,
    start: u64,
    dimension: usize,
    metric: MetricType,
    index: RwLock<BTree<u64, Vec<f32>>>,
}

impl VectorIndex {
    pub fn new_mem(start: u64, inner: Arc<proto::core::Field>) -> CoreResult<Self> {
        todo!()
    }

    pub fn new_disk(start: u64, inner: Arc<proto::core::Field>, path: PathBuf) -> CoreResult<Self> {
        todo!()
    }

    pub fn reader() -> VectorIndexReader {
        todo!()
    }

    pub fn field_type(&self) -> proto::core::field::Type {
        self.inner.r#type()
    }
}

impl VectorIndex {
    pub fn write(&self, records: &[RecordWrapper]) {
        if records.is_empty() {
            return;
        }

        let mut bw = BatchWrite::default();

        for r in records {
            if let Some(val) = &r.value {
                if let Some(value) = val.obj().fields.get(&self.inner.name) {
                    match value {
                        Value {
                            kind: Some(Kind::VectorValue(v)),
                            ..
                        } => {
                            if v.vector.len() != self.dimension {
                                log::error!(
                                    "field value:{:?} embedding has err:dimension not match expected:{} actual:{}",
                                    v.vector,
                                    self.dimension,
                                    v.vector.len()
                                );
                                continue;
                            }
                            bw.put(r.id(), v.vector.clone());
                        }
                        _ => {
                            log::error!(
                                "field value:{:?} embedding has err:kind not match expected:VectorValue",
                                value
                            );
                        }
                    }
                }
            }
        }

        let mut index = self.index.read().unwrap().clone();
        index.write(bw);

        //replace maptree with new one
        *self.index.write().unwrap() = index;
    }
}
