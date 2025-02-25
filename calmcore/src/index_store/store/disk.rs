use std::path::PathBuf;

use mem_btree::persist::{self, KVDeserializer};

use crate::util::CoreResult;

use super::IterKey;

pub struct DiskInvertIndex<K, V> {
    term_record_index: persist::TreeReader<K, V>,
}

impl<K, V> DiskInvertIndex<K, V> {
    pub fn new(path: PathBuf, deserializer: Box<dyn KVDeserializer<K, V>>) -> CoreResult<Self> {
        let term_record_index = persist::TreeReader::new(&path, deserializer)?;
        Ok(Self { term_record_index })
    }

    pub fn len(&self) -> usize {
        self.term_record_index.len() as usize
    }

    pub(crate) fn get(&self, key: &K) -> Option<V> {
        self.term_record_index.get(key)
    }

    pub(crate) fn mget(&self, key: &[K]) -> Vec<Option<V>> {
        self.term_record_index.mget(key)
    }

    pub(crate) fn range<F>(&self, start: Option<&K>, mut f: F)
    where
        F: FnMut(IterKey<K>, &V) -> bool,
    {
        let mut iter = self.term_record_index.iter();
        if let Some(start) = start {
            iter.seek(start);
        }
        while let Some(item) = iter.next() {
            if !f(IterKey::Disk(item.0), &item.1) {
                break;
            }
        }
    }
}
