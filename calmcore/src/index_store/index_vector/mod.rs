// mod stream;
// use crate::{
//     embedding::{self, Embedding},
//     util::{CoreError, CoreResult},
//     RecordWrapper,
// };
// use hora::{
//     core::{ann_index::ANNIndex, metrics::Metric},
//     index::hnsw_idx::HNSWIndex,
// };
// use itertools::Itertools;
// use mem_btree::BTree;
// use proto::core::{value::Kind, Hit, Value};
// use rayon::iter::IntoParallelRefIterator;
// use rayon::prelude::*;
// use croaring::Bitmap;
// use std::{
//     collections::BinaryHeap,
//     sync::{Arc, RwLock},
// };

// use super::{HitStream, QueryParam};

// pub struct Vector {
//     inner: Arc<proto::core::Field>,
//     dimension: usize,
//     embedding: Arc<dyn Embedding + Send + Sync + 'static>,
//     metric: Metric,
//     index: Arc<RwLock<Index>>,
// }

// impl Vector {
//     pub fn new(inner: Arc<proto::core::Field>) -> CoreResult<Self> {
//         let option = match inner
//             .option
//             .as_ref()
//             .ok_or(CoreError::InvalidParam(format!(
//                 "field:{:?} not have option",
//                 inner.name
//             )))? {
//             proto::core::field::Option::Embedding(e) => e.clone(),
//             proto::core::field::Option::Fulltext(_) => {
//                 return Err(CoreError::InvalidParam(format!(
//                     "field:{:?} not support fulltext option",
//                     inner.name
//                 )))
//             }
//         };

//         if option.embedding.as_str() == "" && option.dimension == 0 {
//             return Err(CoreError::InvalidParam(format!(
//                 "field:{:?} not have embedding and dimension",
//                 inner.name
//             )));
//         }

//         let embedding = embedding::new_embedding(&option.embedding)?;

//         let dimension = if option.dimension == 0 {
//             embedding.dimension()
//         } else {
//             option.dimension as usize
//         };

//         let metric = match option.metric() {
//             proto::core::field::embedding_option::Metric::DotProduct => Metric::DotProduct,
//             proto::core::field::embedding_option::Metric::Manhattan => Metric::Manhattan,
//             proto::core::field::embedding_option::Metric::Euclidean => Metric::Euclidean,
//             proto::core::field::embedding_option::Metric::CosineSimilarity => {
//                 Metric::CosineSimilarity
//             }
//             proto::core::field::embedding_option::Metric::Angular => Metric::Angular,
//         };

//         let max_buffer_size = if option.batch_size > 0 {
//             option.batch_size as usize
//         } else {
//             100000
//         };

//         Ok(Self {
//             inner,
//             dimension,
//             embedding,
//             metric,
//             index: Arc::new(RwLock::new(Index {
//                 metric,
//                 dimension,
//                 buffer: Default::default(),
//                 indexs: Default::default(),
//                 buffer_size: 0,
//                 max_buffer_size,
//                 is_indexing: false,
//             })),
//         })
//     }
// }

// impl IndexField for Vector {
//     fn write(&self, records: &[RecordWrapper]) {
//         if records.is_empty() {
//             return;
//         }

//         let mut sub_buffer = Vec::with_capacity(records.len());

//         for r in records {
//             if let Some(val) = &r.value {
//                 if let Some(value) = val.obj().fields.get(&self.inner.name) {
//                     match self.embedding.embedding(value) {
//                         Ok(Some(vs)) => {
//                             if vs.len() != self.dimension {
//                                 log::error!(
//                                     "field value:{:?} embedding has err:dimension not match expected:{} actual:{}",
//                                     value,
//                                     self.dimension,
//                                     vs.len()
//                                 );
//                                 continue;
//                             }

//                             sub_buffer.push((r.id(), vs.into_owned()));
//                         }
//                         Ok(None) => {}
//                         Err(e) => {
//                             log::trace!("field value:{:?} embedding has err:{:?}", value, e);
//                         }
//                     }
//                 }
//             }

//             if let Some(vs) = &r.vectors {
//                 vs.iter().filter(|vt|vt.field_name.eq(&self.inner.name)).for_each(|vt| {
//                     if vt.vector.len() != self.dimension {
//                         log::error!(
//                             "field value:{:?} embedding has err:dimension not match expected:{} actual:{}",
//                             vt.vector,
//                             self.dimension,
//                             vt.vector.len()
//                         );
//                         return;
//                     }
//                     sub_buffer.push((r.id(), vt.vector.clone()));
//                 });
//             }
//         }

//         Index::add_data(&self.index, sub_buffer);
//     }

//     fn delete(&self, _records: &[RecordWrapper]) {
//         //TODO : not need delete anything
//     }

//     fn term(&self, _v: &String, _: Option<QueryParam>) -> CoreResult<Bitmap> {
//         unimplemented!("vector term query")
//     }

//     fn query(
//         &self,
//         filter: &Bitmap,
//         value: &str,
//         param: QueryParam,
//     ) -> CoreResult<Box<dyn HitStream>> {
//         let fetch = 10000;

//         let value = Value {
//             kind: Some(Kind::StringValue(value.to_string())),
//         };

//         let item = self.embedding.embedding(&value)?.ok_or_else(|| {
//             CoreError::InvalidParam(format!("field value:{:?} not support for embedding", value))
//         })?;
//         let result = self.inner_search(item.as_ref(), fetch, filter)?;

//         Ok(Box::new(stream::VectorHitStream::new(result, param.boost)))
//     }

//     fn field_type(&self) -> proto::core::field::Type {
//         proto::core::field::Type::Vector
//     }

//     fn forzen(&self) -> super::IndexData {
//         //TODO: IMPLEMENT ME
//         super::IndexData::Variable(self.inner.clone(), BTree::new(32))
//     }
// }

// impl Vector {
//     fn inner_search(
//         &self,
//         value: &[f32],
//         fetch: usize,
//         filter: &Bitmap,
//     ) -> CoreResult<Vec<Hit>> {
//         if value.len() != self.dimension {
//             return Err(CoreError::InvalidParam(format!(
//                 "field value:{:?} not match dimension expected:{} actual:{}",
//                 value,
//                 self.dimension,
//                 value.len()
//             )));
//         }

//         let (indexs, buffer) = {
//             let index = self.index.read().unwrap();
//             let (i, b) = (index.indexs.clone(), index.buffer.clone());
//             (i.clone(), b.clone())
//         };

//         let mut hits = self.search_indexs(indexs, value, fetch, filter)?;
//         hits.extend(self.search_buffers(buffer, value, fetch, filter)?);

//         Ok(hits)
//     }

//     fn search_buffers(
//         &self,
//         buffer: BufVec,
//         query: &[f32],
//         fetch: usize,
//         filter: &Bitmap,
//     ) -> CoreResult<Vec<Hit>> {
//         let mut heap = BinaryHeap::with_capacity(fetch + 2);

//         for batch in buffer {
//             for (id, v) in batch.iter() {
//                 let id = *id;

//                 if !filter.contains(id) {
//                     continue;
//                 }

//                 let score = hora::core::metrics::metric(query, v, self.metric).map_err(|v| {
//                     CoreError::InvalidParam(format!("metric for query has err:{:?}", v))
//                 })?;

//                 heap.push(Hit {
//                     id,
//                     score,
//                     record: None,
//                 });

//                 if heap.len() > fetch {
//                     heap.pop();
//                 }
//             }
//         }
//         Ok(heap.into_iter().collect_vec())
//     }

//     fn search_indexs(
//         &self,
//         indexs: Vec<Arc<HNSWIndex<f32, u32>>>,
//         query: &[f32],
//         fetch: usize,
//         filter: &Bitmap,
//     ) -> CoreResult<Vec<Hit>> {
//         let hits = indexs
//             .par_iter()
//             .map(|index| {
//                 let result = index.search_with_filter(query, fetch, filter);
//                 result
//                     .into_iter()
//                     .map(|(n, s)| Hit {
//                         id: n.idx().unwrap(),
//                         score: s,
//                         record: None,
//                     })
//                     .collect_vec()
//             })
//             .flatten()
//             .collect();

//         Ok(hits)
//     }
// }

// type BufVec = Vec<Arc<Vec<(u32, Vec<f32>)>>>;

// #[allow(clippy::type_complexity)]
// struct Index {
//     metric: Metric,
//     dimension: usize,
//     buffer: BufVec,
//     indexs: Vec<Arc<HNSWIndex<f32, u32>>>,
//     buffer_size: usize,
//     max_buffer_size: usize,
//     is_indexing: bool,
// }

// impl Index {
//     fn add_data(index: &Arc<RwLock<Index>>, data: Vec<(u32, Vec<f32>)>) {
//         let mut index_lock = index.write().unwrap();

//         index_lock.buffer_size += data.len();
//         index_lock.buffer.push(Arc::new(data));

//         if index_lock.buffer_size >= index_lock.max_buffer_size && !index_lock.is_indexing {
//             index_lock.is_indexing = true;
//             let index = index.clone();
//             std::thread::spawn(|| Self::do_index(index));
//         }
//     }

//     fn do_index(index: Arc<RwLock<Index>>) {
//         let (dimension, metric, buffer, mut indexs) = {
//             let index = index.read().unwrap();
//             (
//                 index.dimension,
//                 index.metric,
//                 index.buffer.clone(),
//                 index.indexs.clone(),
//             )
//         };

//         let mut vector_index = hora::index::hnsw_idx::HNSWIndex::<f32, u32>::new(
//             dimension,
//             &hora::index::hnsw_params::HNSWParams::<f32>::default(),
//         );

//         for sub_buffer in buffer.iter() {
//             let (indices, vss): (Vec<_>, Vec<_>) =
//                 sub_buffer.iter().map(|(id, v)| (id, &v[..])).unzip();
//             if let Err(e) = vector_index.madd(&vss, &indices) {
//                 log::error!("field write index has err:{:?} records:{:?}", e, indices);
//             }
//         }
//         if let Err(e) = vector_index.build(metric) {
//             log::error!("field write index has err:{:?}", e);
//         };

//         indexs.push(Arc::new(vector_index));

//         let mut index = index.write().unwrap();
//         index.buffer.drain(0..buffer.len());
//         index.indexs = indexs;
//         index.is_indexing = false;
//     }
// }
