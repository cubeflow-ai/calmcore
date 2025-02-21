use croaring::Bitmap;
use mem_btree::BTree;

#[derive(Debug, Clone)]
pub(crate) struct MemoryInvertIndex {
    term_record_index: BTree<Vec<u8>, Bitmap>,
}

impl MemoryInvertIndex {
    pub fn new() -> Self {
        Self {
            term_record_index: BTree::new(32),
        }
    }

    pub fn merge(&mut self, key: &[u8], map: Bitmap) {
        let map = match self.term_record_index.get(key) {
            Some(m) => map | m,
            None => map,
        };
        if map.is_empty() {
            self.term_record_index.remove(key);
        } else {
            self.term_record_index.put(key.to_vec(), map);
        }
    }

    pub fn replace(&mut self, release: BTree<Vec<u8>, Bitmap>) {
        self.term_record_index = release;
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<Bitmap> {
        self.term_record_index.get(key).cloned()
    }

    pub fn clone_map(&self) -> BTree<Vec<u8>, Bitmap> {
        self.term_record_index.clone()
    }

    pub fn put(&mut self, term: Vec<u8>, map: Bitmap) {
        self.term_record_index.put(term, map);
    }

    pub(crate) fn pre_range<F>(&self, start: Option<&Vec<u8>>, mut f: F)
    where
        F: FnMut(&[u8], &Bitmap) -> bool,
    {
        let mut iter = self.term_record_index.iter();
        if let Some(start) = start {
            iter.seek(start);
        }
        while let Some(item) = iter.next() {
            if !f(&item.0, &item.1) {
                break;
            }
        }
    }
}
