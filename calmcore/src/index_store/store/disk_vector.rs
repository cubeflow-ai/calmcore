use faiss::Index;

use crate::util::CoreResult;

pub struct DiskVectorIndex {
    start: u64,
    path: PathBuf,
    index: RwLock<IndexImpl>,
    inner: Arc<proto::core::Field>,
}

impl DiskVectorIndex {
    pub fn new(start: u64, inner: Arc<proto::core::Field>, path: PathBuf) -> CoreResult<Self> {
        let index = Index::new_hnsw(
            inner.dimension() as usize,
            inner.metric_type() as MetricType,
            16,
            16,
        )?;
        Ok(Self {
            start,
            path,
            index: RwLock::new(index),
            inner,
        })
    }

    pub fn search(
        &self,
        size: usize,
        query: &[f32],
        filter: &Bitmap,
    ) -> CoreResult<Vec<(f32, u64)>> {
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
