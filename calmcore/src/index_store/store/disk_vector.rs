use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use croaring::Bitmap;

use itertools::Itertools;
use serde::Serialize;

use crate::{
    index_store::index_fulltext::serializer::VECTOR_INDEX,
    util::{CoreError, CoreResult},
};

use hora::{
    core::{
        ann_index::{ANNIndex, SerializableIndex},
        metrics::Metric,
    },
    index::hnsw_idx::HNSWIndex,
};

pub struct DiskVectorIndex {
    start: u64,
    path: PathBuf,
    index: RwLock<HNSWIndex<f32, u64>>,
    inner: Arc<proto::core::Field>,
    dimension: usize,
}

impl DiskVectorIndex {
    pub fn new(start: u64, inner: Arc<proto::core::Field>, path: PathBuf) -> CoreResult<Self> {
        let index = HNSWIndex::load(path.join(VECTOR_INDEX).as_os_str().to_str().unwrap())
            .map_err(|s| CoreError::Internal(s.to_string()))?;

        let dimension = index.dimension();

        Ok(Self {
            start,
            path,
            index: RwLock::new(index),
            inner,
            dimension,
        })
    }

    pub fn search(&self, size: usize, query: &[f32], ids: &Bitmap) -> CoreResult<Vec<(f32, u64)>> {
        let result = self
            .index
            .read()
            .unwrap()
            .search_with_filter(query, size, ids);

        Ok(result
            .iter()
            .filter(|(id, _)| id.idx().is_some())
            .map(|(id, d)| (*d, id.idx().unwrap()))
            .collect_vec())
    }
}
