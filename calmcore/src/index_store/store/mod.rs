mod disk_invert;
mod disk_vector;
mod memory_invert;
mod memory_vector;
use std::{
    hash::Hash,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use croaring::Bitmap;
use disk_invert::DiskInvertIndex;
use hora::{core::metrics::Metric, index::hnsw_idx::HNSWIndex};
use mem_btree::{
    persist::{KVDeserializer, KVSerializer},
    BTree,
};
use memory_invert::MemoryInvertIndex;

use crate::util::{CoreError, CoreResult};

pub(crate) enum VectorIndexReader {
    Memory(memory_vector::MemoryVectorIndexReader),
    Disk(Arc<disk_vector::DiskVectorIndex>),
}

impl VectorIndexReader {
    pub fn new_memory(
        inner: Arc<proto::core::Field>,
        start: u64,
        metric: Metric,
        dimension: usize,
        index: BTree<u64, Vec<f32>>,
    ) -> Self {
        Self::Memory(memory_vector::MemoryVectorIndexReader::new(
            inner, start, metric, dimension, index,
        ))
    }

    pub fn new_disk(start: u64, inner: Arc<proto::core::Field>, path: PathBuf) -> CoreResult<Self> {
        Ok(Self::Disk(Arc::new(disk_vector::DiskVectorIndex::new(
            start, inner, path,
        )?)))
    }

    pub fn build_index(&self) -> CoreResult<HNSWIndex<f32, u64>> {
        match self {
            Self::Memory(m) => m.build_index(),
            Self::Disk(_) => {
                return Err(CoreError::Internal(format!(
                    "disk index not support build_index"
                )))
            }
        }
    }

    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<&Bitmap>,
    ) -> CoreResult<Vec<(f32, u64)>> {
        match self {
            Self::Memory(m) => m.search(query, k, filter),
            Self::Disk(d) => d.search(query, k, filter),
        }
    }
}

pub(crate) enum InvertIndex<K, V> {
    Memory(RwLock<MemoryInvertIndex<K, V>>),
    Disk(Arc<DiskInvertIndex<K, V>>),
}

impl<K, V> InvertIndex<K, V>
where
    K: Ord + Clone + Hash,
    V: Clone,
{
    pub(crate) fn new_memory() -> Self {
        Self::Memory(RwLock::new(MemoryInvertIndex::new()))
    }

    pub(crate) fn new_disk(
        path: PathBuf,
        deserializer: Box<dyn KVDeserializer<K, V>>,
    ) -> CoreResult<Self> {
        Ok(Self::Disk(Arc::new(DiskInvertIndex::new(
            path,
            deserializer,
        )?)))
    }

    pub(crate) fn replace(&self, release: mem_btree::BTree<K, V>) {
        match self {
            Self::Memory(m) => {
                m.write().unwrap().replace(release);
            }
            Self::Disk(_) => {
                panic!("not support update for disk index")
            }
        }
    }

    pub(crate) fn index_reader(&self) -> InvertIndexReader<K, V> {
        match self {
            Self::Memory(m) => InvertIndexReader::Memory(m.read().unwrap().clone()),
            Self::Disk(d) => InvertIndexReader::Disk(d.clone()),
        }
    }

    pub(crate) fn clone_map(&self) -> BTree<K, V> {
        match self {
            Self::Memory(m) => m.read().unwrap().clone_map(),
            Self::Disk(_) => panic!("not support mem_clone for disk index"),
        }
    }
}

pub(crate) enum InvertIndexReader<K, V> {
    Memory(MemoryInvertIndex<K, V>),
    Disk(Arc<DiskInvertIndex<K, V>>),
}

impl<K, V> InvertIndexReader<K, V>
where
    K: Ord + Hash + Clone,
    V: Clone,
{
    pub fn get(&self, key: &K) -> Option<V> {
        match self {
            Self::Memory(m) => m.get(key),
            Self::Disk(d) => d.get(key),
        }
    }

    pub fn mget(&self, key: &[K]) -> Vec<Option<V>> {
        match self {
            Self::Memory(m) => m.mget(key),
            Self::Disk(d) => d.mget(key),
        }
    }

    pub fn range<F>(&self, start: Option<&K>, f: F) -> CoreResult<()>
    where
        F: FnMut(IterKey<K>, &V) -> bool,
    {
        match self {
            Self::Memory(m) => m.range(start, f),
            Self::Disk(d) => d.range(start, f)?,
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        match self {
            Self::Memory(m) => m.len(),
            Self::Disk(d) => d.len(),
        }
    }

    pub fn clone_map(&self) -> BTree<K, V> {
        match self {
            Self::Memory(m) => m.clone_map(),
            Self::Disk(_) => panic!("not support mem_clone for disk index"),
        }
    }
}

pub enum IterKey<'a, K> {
    Memory(&'a K),
    Disk(&'a [u8]),
}

impl<K> IterKey<'_, K>
where
    K: Ord,
{
    pub fn cmp_key<V>(&self, b: &K, ser: &dyn KVSerializer<K, V>) -> std::cmp::Ordering {
        match self {
            Self::Memory(a) => a.cmp(&b),
            Self::Disk(a) => a.cmp(&ser.serialize_key(b).as_ref()),
        }
    }

    pub fn cmp_bytes<V>(&self, b: &[u8], ser: &dyn KVSerializer<K, V>) -> std::cmp::Ordering {
        match self {
            Self::Memory(a) => ser.serialize_key(a).as_ref().cmp(b),
            Self::Disk(a) => a.cmp(&b),
        }
    }

    pub fn to_vec<V>(&self, ser: &dyn KVSerializer<K, V>) -> Vec<u8> {
        match self {
            IterKey::Memory(k) => ser.serialize_key(k).as_ref().to_vec(),
            IterKey::Disk(b) => b.to_vec(),
        }
    }

    pub fn mem_value(&self) -> &K {
        match self {
            IterKey::Memory(k) => k,
            _ => panic!("not support disk key"),
        }
    }
}
