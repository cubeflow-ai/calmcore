use std::collections::BTreeMap;

use croaring::Bitmap;
use itertools::Itertools;
use mem_btree::{Action, BTree, BatchWrite};

use crate::analyzer::Token;

type ReleaseResult = (BTree<String, Bitmap>, BTree<(u32, String), Vec<u32>>);

pub struct Handler {
    token_index: BTree<String, Bitmap>,
    doc_index: BTree<(u32, String), Vec<u32>>,
    token_index_buffer: BTreeMap<String, Action<Bitmap>>,
    doc_index_buffer: BTreeMap<(u32, String), Action<Vec<u32>>>,
}

impl Handler {
    pub fn new(
        token_index: BTree<String, Bitmap>,
        doc_index: BTree<(u32, String), Vec<u32>>,
    ) -> Self {
        Self {
            token_index,
            doc_index,
            token_index_buffer: Default::default(),
            doc_index_buffer: Default::default(),
        }
    }

    pub fn push_index(&mut self, tokens: Vec<Token>, id: u32) {
        // Insert document length
        self.doc_index_buffer.insert(
            (id, "".to_string()),
            Action::Put(vec![tokens.len() as u32], None),
        );
        for (term, tokens) in tokens.iter().into_group_map_by(|t| &t.name) {
            self.doc_index_buffer.insert(
                (id, term.to_string()),
                Action::Put(tokens.iter().map(|t| t.index as u32).collect(), None),
            );

            if let Some(bi) = self.token_index_buffer.get_mut(term) {
                bi.mut_value().add(id);
                return;
            }

            let mut bi = self
                .token_index
                .get(term)
                .cloned()
                .unwrap_or_else(Bitmap::new);
            bi.add(id);
            self.token_index_buffer
                .insert(term.to_string(), Action::Put(bi, None));
        }
    }

    pub fn release(mut self) -> ReleaseResult {
        self.token_index
            .write(BatchWrite::from(self.token_index_buffer));
        self.doc_index
            .write(BatchWrite::from(self.doc_index_buffer));
        (self.token_index, self.doc_index)
    }
}
