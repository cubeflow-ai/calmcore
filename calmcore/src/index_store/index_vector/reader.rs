use std::{
    collections::BinaryHeap,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use croaring::Bitmap;
use faiss::{index::IndexImpl, MetricType};
use itertools::Itertools;
use mem_btree::BTree;

use crate::util::{CoreError, CoreResult};

use faiss::{index_factory, Index, MetricType};

pub struct VectorIndexReader {
    index: crate::store::VectorIndex,
}

impl VectorIndexReader {
    pub fn search(&self, size: usize, query: &[f32], filter: &Bitmap) -> Option<Vec<(f32, u64)>> {
        self.index.search(size, query, filter)
    }
}

pub struct DiskVectorIndexReader {
    start: u64,
    metric: MetricType,
    path: PathBuf,
    index: RwLock<IndexImpl>,
    inner: Arc<proto::core::Field>,
}

impl DiskVectorIndexReader {
    pub fn search(&self, size: usize, query: &[f32], filter: &Bitmap) -> Option<Vec<(f32, u64)>> {
        let result = self.index.read().unwrap().search(query, size)?;

        Ok(result
            .labels
            .iter()
            .zip(result.distances.iter())
            .filter(|(id, _)| id.is_some())
            .map(|(id, d)| (*d, id.get().unwrap()))
            .collect_vec())
    }
}

struct MemVectorIndexReader {
    pub start: u64,
    metric: MetricType,
    index: BTree<u64, Vec<f32>>,
    inner: Arc<proto::core::Field>,
}

impl MemVectorIndexReader {
    pub fn search(&self, size: usize, query: &[f32], filter: &Bitmap) -> Option<Vec<(f32, u64)>> {
        let mut heap = BinaryHeap::new();
        for item in self.index.iter() {
            if !filter.contains((item.0 - self.start) as u32) {
                continue;
            }
            let distance = match self.metric {
                MetricType::InnerProduct => dot_product(&item.1, query)?,
                MetricType::L2 => euclidean_distance(&item.1, query)?,
                _ => return None,
            };
            heap.push((distance, item.0));
        }

        Ok(heap.into_iter().take(size).collect_vec())
    }
}
