use mem_btree::BTree;

use super::IterKey;

#[derive(Clone)]
pub(crate) struct MemoryInvertIndex<K, V> {
    term_record_index: BTree<K, V>,
}

impl<K, V> MemoryInvertIndex<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            term_record_index: BTree::new(32),
        }
    }

    pub fn len(&self) -> usize {
        self.term_record_index.len()
    }

    pub fn replace(&mut self, release: BTree<K, V>) {
        self.term_record_index = release;
    }

    pub(crate) fn get(&self, key: &K) -> Option<V> {
        self.term_record_index.get(key).cloned()
    }

    pub fn clone_map(&self) -> BTree<K, V> {
        self.term_record_index.clone()
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
            if !f(IterKey::Memory(&item.0), &item.1) {
                break;
            }
        }
    }
}
