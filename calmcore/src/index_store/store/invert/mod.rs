mod disk;
pub(crate) mod memory;
use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use croaring::Bitmap;
use disk::DiskInvertIndex;
use memory::MemoryInvertIndex;

use crate::util::{CoreError, CoreResult};

pub(crate) enum InvertIndex {
    Memory(RwLock<MemoryInvertIndex>),
    Disk(Arc<DiskInvertIndex>),
}

impl InvertIndex {
    pub(crate) fn new_memory() -> Self {
        Self::Memory(RwLock::new(MemoryInvertIndex::new()))
    }

    pub(crate) fn new_disk(path: PathBuf) -> CoreResult<Self> {
        Ok(Self::Disk(Arc::new(DiskInvertIndex::new(path)?)))
    }

    pub(crate) fn mem(&self) -> &RwLock<MemoryInvertIndex> {
        match self {
            Self::Memory(m) => m,
            Self::Disk(_) => panic!("not support mem for disk index"),
        }
    }

    //TODO remove this function
    pub fn merge(&self, key: &[u8], map: Bitmap) {
        match self {
            Self::Memory(m) => {
                m.write().unwrap().merge(key, map);
            }
            Self::Disk(_d) => {
                panic!("not support insert for disk index")
            }
        }
    }

    pub(crate) fn replace(&self, release: mem_btree::BTree<Vec<u8>, RoaringBitmap>) {
        match self {
            Self::Memory(m) => {
                m.write().unwrap().replace(release);
            }
            Self::Disk(d) => {
                panic!("not support update for disk index")
            }
        }
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<RoaringBitmap> {
        match self {
            Self::Memory(m) => m.read().unwrap().get(key.as_slice()),
            Self::Disk(d) => d.get(key),
        }
    }

    pub(crate) fn index_reader(&self) -> InvertIndexReader {
        match self {
            Self::Memory(m) => InvertIndexReader::Memory(m.read().unwrap().clone()),
            Self::Disk(d) => InvertIndexReader::Disk(d.clone()),
        }
    }
}

pub(crate) enum InvertIndexReader {
    Memory(MemoryInvertIndex),
    Disk(Arc<DiskInvertIndex>),
}

impl InvertIndexReader {
    pub fn get(&self, key: &Vec<u8>) -> Option<Bitmap> {
        match self {
            Self::Memory(m) => m.get(key),
            Self::Disk(d) => d.get(key),
        }
    }

    pub fn pre_range<F>(&self, start: Option<&Vec<u8>>, f: F)
    where
        F: FnMut(&[u8], &Bitmap) -> bool,
    {
        match self {
            Self::Memory(m) => m.pre_range(start, f),
            Self::Disk(d) => d.pre_range(start, f),
        }
    }
}

#[cfg(test)]
mod test {
    use croaring::Bitmap;

    #[test]
    pub fn test_invert_index() {
        let index = super::InvertIndex::new_memory();
        let mut map = Bitmap::new();
        for i in 0..10 {
            map.add(i);
        }
        index.merge(b"hello", map);
        let r = index.get(&b"hello".to_vec());
        assert!(r.is_some());
    }
}
