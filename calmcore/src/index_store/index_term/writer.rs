use std::collections::BTreeMap;

use croaring::Bitmap;
use mem_btree::{Action, BTree, BatchWrite};

pub(super) struct Handler {
    term_index: BTree<Vec<u8>, Bitmap>,
    term_index_buffer: BTreeMap<Vec<u8>, Action<Bitmap>>,
}

impl Handler {
    pub(super) fn new(term_index: BTree<Vec<u8>, Bitmap>) -> Self {
        Self {
            term_index,
            term_index_buffer: Default::default(),
        }
    }

    pub(super) fn push_index(&mut self, term: Vec<u8>, id: u32) {
        if let Some(bi) = self.term_index_buffer.get_mut(&term) {
            bi.mut_value().add(id);
            return;
        }

        let mut bi = self
            .term_index
            .get(&term)
            .cloned()
            .unwrap_or_else(Bitmap::new);

        bi.add(id);
        self.term_index_buffer.insert(term, Action::Put(bi, None));
    }

    pub(super) fn release(mut self) -> BTree<Vec<u8>, Bitmap> {
        self.term_index
            .write(BatchWrite::from(self.term_index_buffer));
        self.term_index
    }
}
