use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicI32, AtomicU64},
        mpsc, Arc, RwLock,
    },
    time::Duration,
};

use croaring::{Bitmap, Bitmap64};
use itertools::Itertools;
use mem_btree::{BTree, BatchWrite};
use proto::core::{field::TermOption, value::Kind, Field, ObjectValue, Value};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use std::sync::atomic::Ordering::SeqCst;

use crate::{
    index_store::index_term::TermIndex,
    util::{CoreError, CoreResult},
    RecordWrapper,
};

use super::{
    index_fulltext::{reader::FulltextIndexReader, FulltextIndex},
    index_term::reader::TermIndexReader,
    index_vector::VectorIndex,
    store::VectorIndexReader,
};

pub(self) struct Index {
    source_store: Arc<RwLock<BTree<u32, ObjectValue>>>,
    name_store: RwLock<BTree<String, u32>>,
    index_term: RwLock<HashMap<String, Arc<TermIndex>>>,
    index_fulltext: RwLock<HashMap<String, Arc<FulltextIndex>>>,
    index_vector: RwLock<HashMap<String, Arc<VectorIndex>>>,
    marker: RwLock<Option<String>>,
}
impl Index {
    fn new() -> Self {
        Self {
            source_store: Arc::new(RwLock::new(BTree::new(32))),
            name_store: RwLock::new(BTree::new(32)),
            index_term: RwLock::new(HashMap::new()),
            index_fulltext: RwLock::new(HashMap::new()),
            index_vector: RwLock::new(HashMap::new()),
            marker: RwLock::new(None),
        }
    }

    fn make_index(&self, records: &[RecordWrapper]) {
        std::thread::scope(|s| {
            s.spawn(|| {
                let start = std::time::Instant::now();
                self.index_term
                    .read()
                    .unwrap()
                    .par_iter()
                    .for_each(|(_, i)| i.write(records));
                println!("index_term write cost: {:?}", start.elapsed());
            });

            s.spawn(|| {
                let start = std::time::Instant::now();
                self.index_fulltext
                    .read()
                    .unwrap()
                    .par_iter()
                    .for_each(|(_, i)| i.write(records));
                println!("index_fulltext write cost: {:?}", start.elapsed());
            });

            s.spawn(|| {
                self.index_vector
                    .read()
                    .unwrap()
                    .par_iter()
                    .for_each(|(_, i)| {
                        i.write(records);
                    });
            });
        });
    }
}

pub struct MemSegment {
    start: u64,
    end: AtomicU64,
    fields: Arc<Vec<Field>>,
    dels: RwLock<Bitmap>,
    dels_history: RwLock<Bitmap64>,
    index: Arc<Index>,
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
            index: Arc::new(Index::new()),
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
                        self.index
                            .index_term
                            .write()
                            .unwrap()
                            .insert(name, Arc::new(TermIndex::new_mem(start, field)?));
                    }
                }
                proto::core::field::Type::Text => {
                    self.index
                        .index_fulltext
                        .write()
                        .unwrap()
                        .insert(name, Arc::new(FulltextIndex::new_mem(start, field)?));
                }
                proto::core::field::Type::Geo => todo!(),
                proto::core::field::Type::Vector => {
                    self.index
                        .index_vector
                        .write()
                        .unwrap()
                        .insert(name, Arc::new(VectorIndex::new(start, field.clone())?));
                }
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
        if records.is_empty() && max == 0 && marker.is_none() {
            return vec![];
        }

        self.index
            .index_term
            .read()
            .unwrap()
            .par_iter()
            .for_each(|(_, i)| i.write(&records));

        self.index
            .index_fulltext
            .read()
            .unwrap()
            .par_iter()
            .for_each(|(_, i)| i.write(&records));

        self.index
            .index_vector
            .read()
            .unwrap()
            .par_iter()
            .for_each(|(_, i)| {
                i.write(&records);
            });

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

        // here may be has concurrency problem
        let results = records
            .into_iter()
            .filter(|r| r.result.is_ok())
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
        let mut name_store = { self.index.name_store.write().unwrap().clone() };
        name_store.write(name_bw);
        {
            *self.index.name_store.write().unwrap() = name_store;
        }

        let map = source_bw.into_map();

        let source_bw = BatchWrite::from(map);

        //write id -> source mapping
        let mut source_store = { self.index.source_store.write().unwrap().clone() };
        source_store.write(source_bw);
        {
            *self.index.source_store.write().unwrap() = source_store;
        }
        if marker.is_some() {
            *self.index.marker.write().unwrap() = marker;
        }

        self.end.store(max, SeqCst);

        results
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn end(&self) -> u64 {
        self.end.load(SeqCst)
    }

    pub(crate) fn mark_delete(&self, del: u64) {
        if del < self.start {
            self.dels_history.write().unwrap().add(del);
        } else {
            self.dels.write().unwrap().remove((del - self.start) as u32);
        }
    }

    pub fn find_by_id(&self, id: u64) -> Option<Cow<ObjectValue>> {
        self.index
            .source_store
            .read()
            .unwrap()
            .get(&((id - self.start) as u32))
            .cloned()
            .map(Cow::Owned)
    }

    pub(crate) fn find_by_name(&self, name: &String) -> Option<u64> {
        self.index
            .name_store
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
        let mut index_vector = HashMap::new();

        for (n, i) in self.index.index_term.read().unwrap().iter() {
            index_term.insert(n.to_string(), i.reader());
        }

        for (n, i) in self.index.index_fulltext.read().unwrap().iter() {
            index_fulltext.insert(n.to_string(), Arc::new(i.reader()));
        }

        for (n, i) in self.index.index_vector.read().unwrap().iter() {
            index_vector.insert(n.to_string(), Arc::new(i.reader()));
        }

        MemSegmentReader {
            start: self.start(),
            end: self.end(),
            fields: self.fields.clone(),
            index: self.index.clone(),
            dels: RwLock::new(self.dels.read().unwrap().clone()),
            dels_history: self.dels_history.read().unwrap().clone(),
            source_store: self.index.source_store.read().unwrap().clone(),
            name_store: self.index.name_store.read().unwrap().clone(),
            index_term,
            index_fulltext,
            index_vector,
            live_time: self.created_at.elapsed(),
        }
    }
}

pub struct MemSegmentReader {
    pub start: u64,
    pub end: u64,
    pub fields: Arc<Vec<Field>>,
    pub dels: RwLock<Bitmap>,
    pub dels_history: Bitmap64,
    index: Arc<Index>,
    pub source_store: BTree<u32, ObjectValue>,
    pub name_store: BTree<String, u32>,
    pub index_term: HashMap<String, TermIndexReader>,
    pub index_fulltext: HashMap<String, Arc<FulltextIndexReader>>,
    pub(crate) index_vector: HashMap<String, Arc<VectorIndexReader>>,
    pub live_time: Duration,
}

impl MemSegmentReader {
    pub fn term(&self, field: &Field, value: &Vec<u8>) -> CoreResult<Bitmap> {
        Ok(self
            .term_reader(&field.name)?
            .term(value)
            .unwrap_or_default())
    }

    pub fn all_record(&self) -> Bitmap {
        if self.is_empty() {
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

    pub(crate) fn is_empty(&self) -> bool {
        self.start >= self.end && self.source_store.is_empty()
    }

    pub(crate) fn batch_doc(
        &self,
        columns: Option<&[String]>,
        ids: &[u64],
    ) -> Vec<Cow<ObjectValue>> {
        let ids = ids.iter().map(|id| self.abs_id(*id)).collect_vec();

        let need_project = columns
            .map(|c| {
                if self.fields.len() == c.len() {
                    false
                } else {
                    true
                }
            })
            .unwrap_or(false);

        if need_project {
            self.source_store
                .mget(&ids)
                .iter()
                .filter_map(|v| *v)
                .map(|v| {
                    let mut fields = HashMap::new();
                    for c in columns.unwrap_or(&[]) {
                        if let Some(v) = v.fields.get(c).cloned() {
                            fields.insert(c.clone(), v);
                        }
                    }
                    Cow::Owned(ObjectValue { fields })
                })
                .collect::<Vec<_>>()
        } else {
            self.source_store
                .mget(&ids)
                .iter()
                .filter_map(|v| *v)
                .map(Cow::Borrowed)
                .collect::<Vec<_>>()
        }
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

    pub(crate) fn get_vector_reader(&self, field: &Field) -> CoreResult<Arc<VectorIndexReader>> {
        self.index_vector.get(&field.name).cloned().ok_or_else(|| {
            CoreError::InvalidParam(format!("field:{:?} not found in vector index", field.name))
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
            marker: self.marker(),
        })
    }

    pub fn marker(&self) -> Option<String> {
        self.index.marker.read().unwrap().clone()
    }
}
