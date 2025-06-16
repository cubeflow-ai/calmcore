pub(crate) mod reader;
pub(crate) mod serializer;
mod writer;

use croaring::Bitmap;
use mem_btree::BTree;
use proto::core::{
    field::{self},
    value::Kind,
    Field, ObjectValue,
};
use reader::{FulltextIndexReader, TermPositionReader};
use serializer::{DocDeserializer, TokenDeserializer, INDEX_INFO, TERM_INDEX, TERM_POSITION};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
};
use writer::Handler;

use crate::{
    analyzer::Analyzer,
    entity::{ArchivedTermPosition, TermPositionWriter},
    util::CoreResult,
    RecordWrapper,
};

use super::store::InvertIndex;

pub struct FulltextIndex {
    start: u64,
    inner: Arc<proto::core::Field>,
    analyzer: Arc<Analyzer>,
    term_position: RwLock<BTree<String, Arc<RwLock<TermPositionWriter>>>>,
    doc_count: AtomicU32,
    total_term: AtomicU64,
}

impl FulltextIndex {
    pub(crate) fn new_mem(start: u64, inner: Arc<Field>) -> CoreResult<Self> {
        let analyzer = Self::make_analyzer(&inner)?;
        Ok(Self {
            start,
            inner,
            analyzer,
            term_position: RwLock::new(BTree::new(2048)),
            doc_count: AtomicU32::new(0),
            total_term: AtomicU64::new(0),
        })
    }

    fn handler(&self) -> Handler {
        Handler::new(self.term_position.read().unwrap().clone())
    }

    pub fn reader(&self) -> FulltextIndexReader {
        let doc_count = self.doc_count.load(Ordering::Relaxed);
        let total_term = self.total_term.load(Ordering::Relaxed);

        FulltextIndexReader {
            start: self.start,
            inner: self.inner.clone(),
            analyzer: self.analyzer.clone(),
            term_position: TermPositionReader::Memory(self.term_position.read().unwrap().clone()),
            doc_count,
            total_term,
        }
    }

    fn make_analyzer(field: &Arc<Field>) -> CoreResult<Arc<Analyzer>> {
        Ok(Arc::new(
            match field.option.as_ref().and_then(|o| match o {
                field::Option::Fulltext(option) => Some(option),
                _ => None,
            }) {
                Some(o) => Analyzer::new(o)?,
                None => Analyzer::default(),
            },
        ))
    }
}

impl FulltextIndex {
    pub fn write(&self, records: &[RecordWrapper]) {
        if records.is_empty() {
            return;
        }

        let start = std::time::Instant::now();

        let mut handler = self.handler();
        for r in records.iter().filter(|r| r.result.is_ok()) {
            if let Some(val) = &r.value {
                if let Some(value) = val.obj().fields.get(&self.inner.name) {
                    if let Some(Kind::StringValue(text)) = value.kind.as_ref() {
                        if text.is_empty() {
                            continue;
                        }
                        let tokens = self.analyzer.analyzer_index(text);
                        self.doc_count.fetch_add(1, Ordering::Relaxed);
                        self.total_term
                            .fetch_add(tokens.len() as u64, Ordering::Relaxed);
                        handler.push_index(tokens, self.abs_id(r.id()));
                    } else {
                        log::trace!("field value:{:?} is not text, ignore it", value);
                    }
                }
            }
        }

        println!("write fulltext index cost: {:?}", start.elapsed());

        let start = std::time::Instant::now();

        //replace maptree with new one
        *self.term_position.write().unwrap() = handler.release();
        println!("write fulltext index release cost: {:?}", start.elapsed());
    }

    fn abs_id(&self, id: u64) -> u32 {
        (id - self.start) as u32
    }
}
