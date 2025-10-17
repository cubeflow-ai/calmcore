use std::{
    any::{Any, TypeId},
    collections::{HashSet, LinkedList},
    sync::{Arc, RwLock},
};

use arrow::array::{ArrayRef, RecordBatch};
use mem_btree::{persist, BTree};
use roaring::RoaringBitmap;

use crate::{
    partition::WriteInfo, schema::field::FieldType, segment::Segment, utils::error::CoreResult,
};

pub mod keyword;

// pub trait PrimaryKey<K> {
//     fn get(&self, key: &K) -> Option<u32>;
// }

pub trait IndexWriter: Sync + 'static {
    fn name(&self) -> &str;
    fn field_type(&self) -> FieldType;
    fn write(&self, data: &RecordBatch) -> CoreResult<()>;
    fn mget_internal_id(&self, pk_filter: &RwLock<RoaringBitmap>, column: &ArrayRef) -> Vec<u32>;
    fn as_any(&self) -> &dyn Any;
}

pub trait PkWriter: Sync + 'static {
    // write primary key and return the need delete ids
    fn write_pk(
        &self,
        data: &RecordBatch,
        info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<HashSet<u32>>;
}

#[derive(Clone)]
pub(crate) enum InvertedIndex<K> {
    Disk(Arc<persist::TreeReader<K, RoaringBitmap>>),
    Memory(BTree<K, Arc<RwLock<Vec<u32>>>>),
}

impl<K: Clone + Ord> InvertedIndex<K> {
    pub fn new_memory(size: usize) -> InvertedIndex<K> {
        InvertedIndex::Memory(BTree::new(size))
    }

    pub fn new_disk(path: &str) -> CoreResult<InvertedIndex<K>> {
        // let reader = persist::TreeReader::open(path)?;
        // Ok(InvertedIndex::Disk(Arc::new(reader)))
        todo!("implement disk index")
    }
}

/// write functions
impl<K: Clone + Ord> InvertedIndex<K> {
    pub(crate) fn extend(&mut self, k: K, ids: Vec<u32>) {
        if let InvertedIndex::Memory(tree) = self {
            match tree.get(&k) {
                Some(v) => v.write().unwrap().extend(ids),
                None => _ = tree.put(k, Arc::new(RwLock::new(ids))),
            }
        } else {
            panic!("cannot insert to disk index");
        }
    }

    pub(crate) fn insert(
        &mut self,
        k: K,
        ids: Vec<u32>,
    ) -> Option<mem_btree::Item<K, Arc<RwLock<Vec<u32>>>>> {
        if let InvertedIndex::Memory(tree) = self {
            tree.put(k, Arc::new(RwLock::new(ids)))
        } else {
            panic!("cannot insert to disk index");
        }
    }
}

/// read functions
impl<K: Clone + Ord> InvertedIndex<K> {
    pub(crate) fn get_bitmap(&self, k: &K) -> Option<RoaringBitmap> {
        match self {
            InvertedIndex::Disk(r) => r.get(k),
            InvertedIndex::Memory(btree) => btree.get(k).map(|v| {
                RoaringBitmap::from_sorted_iter(v.read().unwrap().iter().map(|v| *v)).unwrap()
            }),
        }
    }

    pub(crate) fn memory_get_ref(&self, k: &K) -> Option<&Arc<RwLock<Vec<u32>>>> {
        match self {
            InvertedIndex::Disk(r) => unreachable!(),
            InvertedIndex::Memory(btree) => btree.get(k),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            InvertedIndex::Disk(r) => r.len() as usize,
            InvertedIndex::Memory(btree) => btree.len(),
        }
    }
}

mod test {
    #[test]
    fn test_index() {
        use super::InvertedIndex;

        let mut index = InvertedIndex::<String>::new_memory(64);

        index.insert("hello".to_string(), vec![1, 2, 3]);
        index.insert("world".to_string(), vec![4, 5, 6]);

        assert_eq!(index.len(), 2);

        index.extend("hello".to_string(), vec![7, 8, 9]);

        assert_eq!(index.len(), 2);

        let bitmap = index.get_bitmap(&"hello".to_string()).unwrap();

        assert!(bitmap.contains(1));
        assert!(bitmap.contains(9));
        assert!(!bitmap.contains(10));

        println!("bitmap: {:?}", bitmap);
    }
}
