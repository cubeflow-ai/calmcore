pub mod reader;
use crate::{
    util::{CoreError, CoreResult},
    RecordWrapper,
};
use hora::core::metrics::Metric;
use mem_btree::{BTree, BatchWrite};
use proto::core::{field, value::Kind, ObjectValue, Value};
use std::sync::{Arc, RwLock};

use super::store::VectorIndexReader;

pub struct VectorIndex {
    inner: Arc<proto::core::Field>,
    start: u64,
    dimension: usize,
    metric: Metric,
    index: RwLock<BTree<u64, Vec<f32>>>,
}

impl VectorIndex {
    pub fn new(start: u64, inner: Arc<proto::core::Field>) -> CoreResult<Self> {
        let option = inner.vector_option().map_err(CoreError::InvalidParam)?;

        let dimension = option.dimension as usize;

        let metric = match option.metric() {
            field::vector_option::Metric::DotProduct => Metric::DotProduct,
            field::vector_option::Metric::Euclidean => Metric::Euclidean,
            field::vector_option::Metric::Manhattan => Metric::Manhattan,
            field::vector_option::Metric::CosineSimilarity => Metric::CosineSimilarity,
            field::vector_option::Metric::Angular => Metric::Angular,
        };

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
            self.inner.clone(),
            self.start,
            self.metric,
            self.dimension,
            self.index.read().unwrap().clone(),
        )
    }
}

impl VectorIndex {
    pub fn write(&self, records: &[RecordWrapper]) {
        if records.is_empty() {
            return;
        }

        let mut bw = BatchWrite::default();

        for r in records.iter().filter(|r| r.result.is_ok()) {
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
