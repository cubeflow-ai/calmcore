use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use croaring::Bitmap;
use itertools::Itertools;
use mem_btree::{Action, BTree, BatchWrite};

use crate::{analyzer::Token, entity::TermPositionWriter};

type ReleaseResult = BTree<String, Arc<RwLock<TermPositionWriter>>>;

pub struct Handler {
    term_position: BTree<String, Arc<RwLock<TermPositionWriter>>>,
    term_position_buffer: BTreeMap<String, Action<Arc<RwLock<TermPositionWriter>>>>,
}

impl Handler {
    pub fn new(term_position: BTree<String, Arc<RwLock<TermPositionWriter>>>) -> Self {
        Self {
            term_position,
            term_position_buffer: Default::default(),
        }
    }

    pub fn push_index(&mut self, tokens: Vec<Token>, id: u32) {
        // Insert document length

        let group = tokens.iter().into_group_map_by(|t| &t.name);

        for (term, tokens) in group {
            let tp = self.term_position_buffer.get(term);

            match tp {
                Some(tp) => {
                    let offsets = tokens.iter().map(|t| t.index as u32).collect_vec();
                    let mut tp = tp.value_ref().write().unwrap();
                    let index = tp.offsets.len() as u32;
                    tp.ids.push(id);
                    tp.index.push(index);
                    tp.length.push(offsets.len() as u16);
                    tp.offsets.extend(offsets);
                }
                None => {
                    let value = self.term_position.get(term);
                    match value {
                        Some(tp) => {
                            let offsets = tokens.iter().map(|t| t.index as u32).collect_vec();
                            let mut tp = tp.write().unwrap();
                            let index = tp.offsets.len() as u32;
                            tp.ids.push(id);
                            tp.index.push(index);
                            tp.length.push(offsets.len() as u16);
                            tp.offsets.extend(offsets);
                        }
                        None => {
                            let offsets = tokens.iter().map(|t| t.index as u32).collect_vec();
                            let length = vec![offsets.len() as u16];
                            let tp = TermPositionWriter {
                                name: term.to_string(),
                                ids: vec![id],
                                offsets,
                                index: vec![0],
                                length,
                            };

                            self.term_position_buffer.insert(
                                term.to_string(),
                                Action::Put(Arc::new(RwLock::new(tp)), None),
                            );
                        }
                    }
                }
            }
        }
    }

    pub fn release(mut self) -> ReleaseResult {
        self.term_position
            .write(BatchWrite::from(self.term_position_buffer));
        self.term_position
    }
}
