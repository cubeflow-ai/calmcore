pub mod reader;
use crate::{
    util::{CoreError, CoreResult},
    RecordWrapper,
};
use croaring::Bitmap;
use faiss::{index_factory, MetricType};
use hora::{
    core::{ann_index::ANNIndex, metrics::Metric},
    index::hnsw_idx::HNSWIndex,
};
use itertools::Itertools;
use mem_btree::{BTree, BatchWrite};
use parquet::file::page_index::index;
use proto::core::{field, value::Kind, Hit, Value};
use rayon::iter::IntoParallelRefIterator;
use rayon::prelude::*;
use std::{
    collections::BinaryHeap,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
};

use super::store::VectorIndexReader;

pub struct VectorIndex {
    inner: Arc<proto::core::Field>,
    start: u64,
    dimension: usize,
    metric: MetricType,
    index: RwLock<BTree<u64, Vec<f32>>>,
}

impl VectorIndex {
    pub fn new(start: u64, inner: Arc<proto::core::Field>) -> CoreResult<Self> {
        let embedding = if let Some(field::Option::Embedding(e)) = &inner.option {
            e
        } else {
            return Err(CoreError::Internal(format!(
                "field:{:?} not set embedding",
                inner
            )));
        };

        let dimension = embedding.dimension as usize;

        let metric = match embedding.metric() {
            field::embedding_option::Metric::InnerProduct => MetricType::InnerProduct,
            field::embedding_option::Metric::L2 => MetricType::L2,
        };

        // let index = index_factory(dimension, &embedding.index_params, metric)?;

        Ok(VectorIndex {
            inner,
            start,
            dimension,
            metric,
            index: RwLock::new(BTree::new(32)),
        })
    }

    pub fn reader(&self) -> VectorIndexReader {
        VectorIndexReader::new_memory(
            self.start,
            self.metric,
            self.dimension,
            self.index.read().unwrap().clone(),
        )
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
