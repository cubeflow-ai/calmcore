mod disk;
pub(crate) mod memory;
use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use disk::DiskInvertIndex;
use mem_btree::{
    persist::{KVDeserializer, KVSerializer},
    BTree,
};
use memory::MemoryInvertIndex;

use crate::util::CoreResult;

pub(crate) enum InvertIndex<K, V> {
    Memory(RwLock<MemoryInvertIndex<K, V>>),
    Disk(Arc<DiskInvertIndex<K, V>>),
}

impl<K, V> InvertIndex<K, V>
where
    K: Ord + Clone,
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
    K: Ord + Clone,
    V: Clone,
{
    pub fn get(&self, key: &K) -> Option<V> {
        match self {
            Self::Memory(m) => m.get(key),
            Self::Disk(d) => d.get(key),
        }
    }

    pub fn range<F>(&self, start: Option<&K>, f: F)
    where
        F: FnMut(IterKey<K>, &V) -> bool,
    {
        match self {
            Self::Memory(m) => m.range(start, f),
            Self::Disk(d) => d.range(start, f),
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        match self {
            Self::Memory(m) => m.len(),
            Self::Disk(d) => d.len(),
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
