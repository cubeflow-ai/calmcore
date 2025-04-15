use std::{cmp::Ordering, collections::BinaryHeap, sync::Arc};

use croaring::Bitmap;
use hora::{
    core::{ann_index::ANNIndex, metrics::*},
    index::{hnsw_idx::HNSWIndex, hnsw_params::HNSWParams},
};
use itertools::Itertools;
use mem_btree::BTree;

use crate::util::{CoreError, CoreResult};

pub struct MemoryVectorIndexReader {
    inner: Arc<proto::core::Field>,
    start: u64,
    metric: Metric,
    dimension: usize,
    index: BTree<u64, Vec<f32>>,
}

impl MemoryVectorIndexReader {
    pub fn new(
        inner: Arc<proto::core::Field>,
        start: u64,
        metric: Metric,
        dimension: usize,
        index: BTree<u64, Vec<f32>>,
    ) -> Self {
        Self {
            inner,
            start,
            metric,
            dimension,
            index,
        }
    }

    pub fn search(
        &self,
        query: &[f32],
        size: usize,
        filter: &Bitmap,
    ) -> CoreResult<Vec<(f32, u64)>> {
        let fn_err = |e| {
            CoreError::Internal(format!(
                "field:{:?}  metric:{:?} has err:{}",
                self.inner.name, self.metric, e
            ))
        };
        let mut heap = BinaryHeap::new();
        for item in self.index.iter() {
            if !filter.contains((item.0 - self.start) as u32) {
                continue;
            }

            let distance = match self.metric {
                Metric::Euclidean => euclidean_distance(&item.1, query).map_err(fn_err)?,
                Metric::Manhattan => manhattan_distance(&item.1, query).map_err(fn_err)?,
                Metric::DotProduct => dot_product(&item.1, query).map_err(fn_err)?,
                Metric::CosineSimilarity => cosine_similarity(&item.1, query).map_err(fn_err)?,
                Metric::Angular => angular_distance(&item.1, query).map_err(fn_err)?,
                Metric::Unknown => unreachable!(),
            };

            heap.push((ComparableF32(distance), item.0));

            if heap.len() > size {
                heap.pop();
            }
        }

        Ok(heap.into_iter().map(|(s, i)| (s.0, i)).collect_vec())
    }

    pub(crate) fn build_index(&self) -> CoreResult<HNSWIndex<f32, u64>> {
        let mut index = HNSWIndex::new(
            self.dimension,
            &HNSWParams::default().max_item(self.index.len()),
        );

        for v in self.index.iter() {
            let id = v.0;
            let vector = &v.1;
            index.add(vector, id).map_err(|e| {
                CoreError::Internal(format!(
                    "add vector index field:{:?}  metric:{:?} has err:{}",
                    self.inner.name, self.metric, e
                ))
            })?;
        }

        index.build(self.metric.clone()).map_err(|e| {
            CoreError::Internal(format!(
                "build vector index field:{:?}  metric:{:?} has err:{}",
                self.inner.name, self.metric, e
            ))
        })?;

        Ok(index)
    }
}

#[derive(PartialEq, PartialOrd)]
struct ComparableF32(f32);

impl Eq for ComparableF32 {} // 需要同时实现 Eq

impl Ord for ComparableF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Less)
    }
}
