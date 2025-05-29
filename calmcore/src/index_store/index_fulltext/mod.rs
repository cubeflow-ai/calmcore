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
        Arc, RwLock,
    },
};
use writer::Handler;

use crate::{analyzer::Analyzer, entity::TermPosition, util::CoreResult};

use super::store::InvertIndex;

type TermInvertIndex = InvertIndex<String, Bitmap>;
type DocInvertIndex = InvertIndex<String, Arc<RwLock<TermPosition>>>;

pub struct FulltextIndex {
    start: u64,
    inner: Arc<proto::core::Field>,
    analyzer: Arc<Analyzer>,
    token_index: TermInvertIndex,
    term_position: DocInvertIndex,
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
            token_index: TermInvertIndex::new_memory(),
            term_position: DocInvertIndex::new_memory(),
            doc_count: AtomicU32::new(0),
            total_term: AtomicU64::new(0),
        })
    }

    pub(crate) fn new_disk(start: u64, inner: Arc<Field>, path: PathBuf) -> CoreResult<Self> {
        let analyzer = Self::make_analyzer(&inner)?;

        let info: serde_json::Value =
            serde_json::from_reader(std::fs::File::open(path.join(INDEX_INFO))?)?;

        let doc_count = info.get("doc_count").unwrap().as_u64().unwrap() as u32;
        let total_term = info.get("total_term").unwrap().as_u64().unwrap();
        Ok(Self {
            start,
            inner,
            analyzer,
            token_index: TermInvertIndex::new_disk(
                path.join(TERM_INDEX),
                Box::new(TokenDeserializer {}),
            )?,
            term_position: DocInvertIndex::new_disk(
                path.join(TERM_POSITION),
                Box::new(DocDeserializer {}),
            )?,
            doc_count: AtomicU32::new(doc_count),
            total_term: AtomicU64::new(total_term),
        })
    }

    fn handler(&self) -> Handler {
        Handler::new(self.token_index.clone_map(), self.term_position.clone_map())
    }

    pub fn reader(&self) -> FulltextIndexReader {
        let doc_count = self.doc_count.load(Ordering::Relaxed);
        let total_term = self.total_term.load(Ordering::Relaxed);

        FulltextIndexReader {
            start: self.start,
            inner: self.inner.clone(),
            analyzer: self.analyzer.clone(),
            token_index: self.token_index.index_reader(),
            term_position: TermPositionReader::Memory(self.term_position.clone_map()),
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
    pub fn write(&self, source: &BTree<u32, ObjectValue>) {
        if source.is_empty() {
            return;
        }

        let mut handler = self.handler();

        let start = std::time::Instant::now();

        for r in source.iter() {
            let (id, obj) = (r.0, &r.1);
            if let Some(value) = obj.fields.get(&self.inner.name) {
                if let Some(Kind::StringValue(text)) = value.kind.as_ref() {
                    if text.is_empty() {
                        continue;
                    }
                    let tokens = self.analyzer.analyzer_index(text);
                    self.doc_count.fetch_add(1, Ordering::Relaxed);
                    self.total_term
                        .fetch_add(tokens.len() as u64, Ordering::Relaxed);
                    handler.push_index(tokens, id);
                } else {
                    log::trace!("field value:{:?} is not text, ignore it", value);
                }
            }
        }

        let (token_index, doc_index) = handler.release();

        //replace maptree with new one
        self.term_position.replace(doc_index);
        self.token_index.replace(token_index);
    }
}
