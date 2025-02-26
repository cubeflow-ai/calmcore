use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc, RwLock},
    time::Duration,
};

use croaring::{Bitmap, Bitmap64};
use itertools::Itertools;
use mem_btree::{BTree, BatchWrite};
use proto::core::{Field, Record};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    index_store::index_term::TermIndex,
    util::{CoreError, CoreResult},
    RecordWrapper,
};

use super::{
    index_fulltext::{reader::FulltextIndexReader, FulltextIndex},
    index_term::reader::TermIndexReader,
};

pub enum IndexEnum {
    TermIndex(Arc<TermIndex>),
    FulltextIndex(Arc<FulltextIndex>),
}

pub struct MemSegment {
    start: u64,
    end: AtomicU64,
    dels: RwLock<Bitmap>,
    dels_history: RwLock<Bitmap64>,
    source_store: RwLock<BTree<u32, Record>>,
    name_store: RwLock<BTree<String, u32>>,
    index_term: RwLock<HashMap<String, Arc<TermIndex>>>,
    index_fulltext: RwLock<HashMap<String, Arc<FulltextIndex>>>,
    marker: RwLock<Option<String>>,
    indexs_arr: RwLock<Vec<IndexEnum>>,
    created_at: std::time::Instant,
}

impl MemSegment {
    pub fn new(start: u64, fields: HashMap<String, Arc<Field>>) -> CoreResult<Self> {
        let start = start + 1;
        let segment = MemSegment {
            start,
            end: AtomicU64::new(start),
            dels: RwLock::new(Default::default()),
            dels_history: RwLock::new(Default::default()),
            source_store: RwLock::new(BTree::new(32)),
            name_store: RwLock::new(BTree::new(32)),
            index_term: RwLock::new(HashMap::new()),
            index_fulltext: RwLock::new(HashMap::new()),
            marker: RwLock::new(None),
            indexs_arr: RwLock::new(Vec::new()),
            created_at: std::time::Instant::now(),
        };

        for (_, field) in fields {
            segment.add_index_field(start, field)?;
        }

        Ok(segment)
    }

    /// add index field to segment
    /// if field already exists, return error
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn add_index_field(&self, start: u64, field: Arc<Field>) -> CoreResult<()> {
        let name = field.name.clone();
        if self.index_term.read().unwrap().contains_key(&name) {
            return Err(CoreError::Existed(format!(
                "field {} already existed in index store",
                name
            )));
        }

        use proto::core::field::Type::*;
        match field.r#type() {
            Bool | Int | Float | String => {
                let index = Arc::new(TermIndex::new_mem(start, field)?);
                self.indexs_arr
                    .write()
                    .unwrap()
                    .push(IndexEnum::TermIndex(index.clone()));
                self.index_term.write().unwrap().insert(name, index);
            }
            proto::core::field::Type::Text => {
                let index = Arc::new(FulltextIndex::new_mem(start, field)?);
                self.indexs_arr
                    .write()
                    .unwrap()
                    .push(IndexEnum::FulltextIndex(index.clone()));
                self.index_fulltext.write().unwrap().insert(name, index);
            }
            proto::core::field::Type::Geo => todo!(),
            proto::core::field::Type::Vector => todo!(),
        }

        Ok(())
    }

    pub fn write_records(
        &self,
        records: Vec<RecordWrapper>,
        max: u64,
        marker: Option<String>,
    ) -> Vec<CoreError> {
        self.indexs_arr
            .read()
            .unwrap()
            .par_iter()
            .for_each(|index| match index {
                IndexEnum::TermIndex(i) => {
                    i.write(&records);
                }
                IndexEnum::FulltextIndex(i) => {
                    i.write(&records);
                }
            });

        let mut source_bw = BatchWrite::default();
        let mut name_bw = BatchWrite::default();

        let results = records
            .into_iter()
            .map(|r| {
                let id = r.abs_id(self.start);
                if !r.record.name.is_empty() {
                    name_bw.put(r.record.name.clone(), id);
                }
                source_bw.put(id, r.record);

                r.result
            })
            .collect();

        //write name -> id mapping
        let mut name_store = { self.name_store.write().unwrap().clone() };
        name_store.write(name_bw);
        {
            *self.name_store.write().unwrap() = name_store;
        }

        //write id -> source mapping
        let mut source_store = { self.source_store.write().unwrap().clone() };
        source_store.write(source_bw);
        {
            *self.source_store.write().unwrap() = source_store;
        }
        if marker.is_some() {
            *self.marker.write().unwrap() = marker;
        }
        self.end.store(max, std::sync::atomic::Ordering::SeqCst);
        results
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn end(&self) -> u64 {
        self.end.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mark_delete(&self, del: u64) {
        if del < self.start {
            self.dels_history.write().unwrap().add(del);
        } else {
            self.dels.write().unwrap().remove((del - self.start) as u32);
        }
    }

    pub fn find_by_id(&self, id: u64) -> Option<Record> {
        self.source_store
            .read()
            .unwrap()
            .get(&((id - self.start) as u32))
            .cloned()
    }

    pub(crate) fn find_by_name(&self, name: &String) -> Option<u64> {
        self.name_store
            .read()
            .unwrap()
            .get(name)
            .map(|v| (*v as u64) + self.start)
    }
}

// read segment
impl MemSegment {
    pub(crate) fn reader(&self) -> MemSegmentReader {
        let mut index_term = HashMap::new();
        let mut index_fulltext = HashMap::new();

        for index in self.indexs_arr.read().unwrap().iter() {
            match index {
                IndexEnum::TermIndex(i) => {
                    index_term.insert(i.field_name().to_string(), i.reader());
                }
                IndexEnum::FulltextIndex(f) => {
                    index_fulltext.insert(f.field_name().to_string(), Arc::new(f.reader()));
                }
            }
        }

        MemSegmentReader {
            start: self.start(),
            end: self.end(),
            dels: RwLock::new(self.dels.read().unwrap().clone()),
            dels_history: self.dels_history.read().unwrap().clone(),
            source_store: self.source_store.read().unwrap().clone(),
            name_store: self.name_store.read().unwrap().clone(),
            index_term,
            index_fulltext,
            live_time: self.created_at.elapsed(),
            marker: self.marker.read().unwrap().clone(),
        }
    }
}

pub struct MemSegmentReader {
    pub start: u64,
    pub end: u64,
    pub dels: RwLock<Bitmap>,
    pub dels_history: Bitmap64,
    pub source_store: BTree<u32, Record>,
    pub name_store: BTree<String, u32>,
    pub index_term: HashMap<String, TermIndexReader>,
    pub index_fulltext: HashMap<String, Arc<FulltextIndexReader>>,
    pub live_time: Duration,
    pub marker: Option<String>,
}

impl MemSegmentReader {
    pub fn term(&self, field: &Field, value: &Vec<u8>) -> CoreResult<Bitmap> {
        Ok(self
            .term_reader(&field.name)?
            .term(value)
            .unwrap_or_default())
    }

    pub fn all_record(&self) -> Bitmap {
        if self.end <= self.start {
            return Bitmap::new();
        }
        let all_records = Bitmap::from_iter((0..self.end - self.start + 1).map(|v| v as u32));
        all_records - &*self.dels.read().unwrap()
    }

    pub(crate) fn between(
        &self,
        field: &Field,
        low: Option<&Vec<u8>>,
        low_eq: bool,
        high: Option<&Vec<u8>>,
        high_eq: bool,
    ) -> CoreResult<Bitmap> {
        self.term_reader(&field.name)?
            .between(low, low_eq, high, high_eq)
    }

    pub(crate) fn in_terms(&self, field: &Field, list: &[Vec<u8>]) -> CoreResult<Bitmap> {
        Ok(self.term_reader(&field.name)?.in_terms(list))
    }

    fn term_reader(&self, name: &str) -> CoreResult<&TermIndexReader> {
        self.index_term
            .get(name)
            .ok_or_else(|| CoreError::InvalidParam(format!("field:{:?} not found", name)))
    }

    pub(crate) fn doc(&self, id: u64) -> Option<Cow<Record>> {
        self.source_store.get(&self.abs_id(id)).map(Cow::Borrowed)
    }

    pub(crate) fn batch_doc(&self, ids: &[u64]) -> Vec<Option<Cow<Record>>> {
        let ids = ids.iter().map(|id| self.abs_id(*id)).collect_vec();
        self.source_store
            .mget(&ids)
            .iter()
            .map(|v| v.map(Cow::Borrowed))
            .collect()
    }

    pub(crate) fn find_by_name(&self, name: &str) -> Option<u64> {
        self.name_store.get(name).map(|v| (*v as u64) + self.start)
    }

    pub(crate) fn get(&self, name: &str) -> Option<Cow<Record>> {
        self.find_by_name(name).and_then(|id| self.doc(id))
    }

    pub(crate) fn get_field(&self, field: &str) -> Option<Arc<Field>> {
        self.index_term.get(field).map(|v| v.field().clone())
    }

    pub(crate) fn mark_delete(&self, del: u64) {
        self.dels.write().unwrap().add((del - self.start) as u32);
    }

    fn abs_id(&self, id: u64) -> u32 {
        (id - self.start) as u32
    }

    pub(crate) fn get_text_reader(&self, field: &Field) -> CoreResult<Arc<FulltextIndexReader>> {
        self.index_fulltext
            .get(&field.name)
            .cloned()
            .ok_or_else(|| {
                CoreError::InvalidParam(format!("field:{:?} not found in text index", field.name))
            })
    }

    pub(crate) fn info(&self) -> CoreResult<super::SegmentInfo> {
        Ok(super::SegmentInfo {
            start: self.start,
            end: self.end,
            store_type: "hot".to_string(),
            size_bytes: 0, //TODO impl me
            doc_count: self.source_store.len() as u32,
            del_count: self.dels.read().unwrap().cardinality() as u32,
            marker: self.marker.clone(),
        })
    }
}
