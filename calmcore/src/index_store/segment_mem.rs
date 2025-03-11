use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc, RwLock},
    time::Duration,
};

use croaring::{Bitmap, Bitmap64};
use itertools::Itertools;
use mem_btree::{BTree, BatchWrite};
use proto::core::{field::TermOption, value::Kind, Field, ObjectValue, Record, Value};
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
    fields: Arc<Vec<Field>>,
    dels: RwLock<Bitmap>,
    dels_history: RwLock<Bitmap64>,
    source_store: RwLock<BTree<u32, ObjectValue>>,
    name_store: RwLock<BTree<String, u32>>,
    index_term: RwLock<HashMap<String, Arc<TermIndex>>>,
    index_fulltext: RwLock<HashMap<String, Arc<FulltextIndex>>>,
    marker: RwLock<Option<String>>,
    created_at: std::time::Instant,
}

impl MemSegment {
    pub fn new(start: u64, fields: HashMap<String, Arc<Field>>) -> CoreResult<Self> {
        let start = start + 1;

        let mut fields = fields.values().map(|v| (&**v).clone()).collect_vec();

        fields.push(Field {
            name: "_id".to_string(),
            r#type: proto::core::field::Type::Int as i32,
            option: Some(proto::core::field::Option::Term(TermOption {
                no_index: true,
                no_store: false,
            })),
        });

        fields.push(Field {
            name: "_name".to_string(),
            r#type: proto::core::field::Type::String as i32,
            option: Some(proto::core::field::Option::Term(TermOption {
                no_index: true,
                no_store: false,
            })),
        });

        use itertools::Itertools;

        if fields.iter().map(|f| &f.name).dedup().count() != fields.len() {
            return Err(CoreError::InvalidParam(format!(
                "field name must be unique: fields:{:?}",
                fields,
            )));
        }

        let mut segment = MemSegment {
            start,
            end: AtomicU64::new(start),
            fields: Arc::new(fields),
            dels: RwLock::new(Default::default()),
            dels_history: RwLock::new(Default::default()),
            source_store: RwLock::new(BTree::new(32)),
            name_store: RwLock::new(BTree::new(32)),
            index_term: RwLock::new(HashMap::new()),
            index_fulltext: RwLock::new(HashMap::new()),
            marker: RwLock::new(None),
            created_at: std::time::Instant::now(),
        };

        segment.index_field()?;

        Ok(segment)
    }

    /// add index field to segment
    /// if field already exists, return error
    pub fn index_field(&mut self) -> CoreResult<()> {
        let start = self.start;
        for field in self.fields.iter() {
            use proto::core::field::Type::*;

            let field = Arc::new(field.clone());

            let name = field.name.clone();

            match field.r#type() {
                Bool | Int | Float | String => {
                    if match &field.option {
                        Some(proto::core::field::Option::Term(t)) => !t.no_index,
                        Some(_) => {
                            return Err(CoreError::InvalidParam(format!(
                                "invalid option field:{:?}",
                                field
                            )))
                        }
                        None => true,
                    } {
                        self.index_term
                            .write()
                            .unwrap()
                            .insert(name, Arc::new(TermIndex::new_mem(start, field)?));
                    }
                }
                proto::core::field::Type::Text => {
                    self.index_fulltext
                        .write()
                        .unwrap()
                        .insert(name, Arc::new(FulltextIndex::new_mem(start, field)?));
                }
                proto::core::field::Type::Geo => todo!(),
                proto::core::field::Type::Vector => todo!(),
            }
        }
        Ok(())
    }

    pub fn write_records(
        &self,
        records: Vec<RecordWrapper>,
        max: u64,
        marker: Option<String>,
    ) -> Vec<CoreError> {
        self.index_term
            .read()
            .unwrap()
            .par_iter()
            .for_each(|(_, i)| i.write(&records));

        self.index_fulltext
            .read()
            .unwrap()
            .par_iter()
            .for_each(|(_, i)| i.write(&records));

        let to_value = |value| {
            let value: Value = value?;
            if let Some(Kind::ObjectValue(obj)) = value.kind {
                Some(obj)
            } else {
                None
            }
        };

        let mut source_bw = BatchWrite::default();
        let mut name_bw = BatchWrite::default();

        let results = records
            .into_iter()
            .map(|r| {
                let id = r.abs_id(self.start);
                if !r.record.name.is_empty() {
                    name_bw.put(r.record.name.clone(), id);
                }

                source_bw.put(id, to_value(r.value).unwrap());

                r.result
            })
            .collect();

        //write name -> id mapping
        let mut name_store = { self.name_store.write().unwrap().clone() };
        name_store.write(name_bw);
        {
            *self.name_store.write().unwrap() = name_store;
        }

        let map = source_bw.into_map();

        let source_bw = BatchWrite::from(map);

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

    pub fn find_by_id(&self, id: u64) -> Option<Cow<ObjectValue>> {
        self.source_store
            .read()
            .unwrap()
            .get(&((id - self.start) as u32))
            .cloned()
            .map(Cow::Owned)
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

        for (n, i) in self.index_term.read().unwrap().iter() {
            index_term.insert(n.to_string(), i.reader());
        }

        for (n, i) in self.index_fulltext.read().unwrap().iter() {
            index_fulltext.insert(n.to_string(), Arc::new(i.reader()));
        }

        MemSegmentReader {
            start: self.start(),
            end: self.end(),
            fields: self.fields.clone(),
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
    pub fields: Arc<Vec<Field>>,
    pub dels: RwLock<Bitmap>,
    pub dels_history: Bitmap64,
    pub source_store: BTree<u32, ObjectValue>,
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

    pub(crate) fn doc(&self, id: u64) -> Option<Cow<ObjectValue>> {
        self.source_store.get(&self.abs_id(id)).map(Cow::Borrowed)
    }

    pub(crate) fn batch_doc(&self, ids: &[u64]) -> Vec<Cow<ObjectValue>> {
        let ids = ids.iter().map(|id| self.abs_id(*id)).collect_vec();
        self.source_store
            .mget(&ids)
            .iter()
            .filter_map(|v| *v)
            .map(Cow::Borrowed)
            .collect::<Vec<_>>()
    }

    pub(crate) fn find_by_name(&self, name: &str) -> Option<u64> {
        self.name_store.get(name).map(|v| (*v as u64) + self.start)
    }

    pub(crate) fn get(&self, name: &str) -> Option<Cow<ObjectValue>> {
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
