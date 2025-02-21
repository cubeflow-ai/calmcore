use std::{
    borrow::Cow,
    collections::HashMap,
    error::Error,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use croaring::{Bitmap, Portable};
use mem_btree::persist;
use proto::core::{Field, Record};

use crate::{
    index_store::index_fulltext::FulltextIndex,
    util::{CoreError, CoreResult},
};

use super::{
    index_fulltext::reader::FulltextIndexReader,
    index_term::{reader::TermIndexReader, TermIndex},
};

pub struct DiskSegment {
    start: u64,
    end: u64,
    dels: RwLock<Bitmap>,
    name_store: persist::TreeReader<String, u32>,
    source_store: persist::TreeReader<u32, Record>,
    index_terms: HashMap<String, TermIndexReader>,
    index_fulltext: HashMap<String, Arc<FulltextIndexReader>>,
    marker: Option<String>,
    usage_bytes: u64,
}

struct RecordDeserializer;

impl persist::KVDeserializer<u32, Record> for RecordDeserializer {
    fn deserialize_value(&self, v: &[u8]) -> std::result::Result<Record, Box<dyn Error>> {
        bincode::deserialize(v).map_err(|e| e.into())
    }

    fn serialize_key<'a>(&self, k: &'a u32) -> Cow<'a, [u8]> {
        Cow::Owned(k.to_be_bytes().to_vec())
    }
}

struct U32BeDeserializer;

impl persist::KVDeserializer<String, u32> for U32BeDeserializer {
    fn deserialize_value(&self, v: &[u8]) -> std::result::Result<u32, Box<dyn Error>> {
        Ok(u32::from_be_bytes(v.try_into().unwrap()))
    }

    fn serialize_key<'a>(&self, k: &'a String) -> Cow<'a, [u8]> {
        Cow::Borrowed(k.as_bytes())
    }
}

impl DiskSegment {
    pub fn new(
        path: PathBuf,
        fields: &HashMap<String, Arc<proto::core::Field>>,
    ) -> CoreResult<Self> {
        let name: Cow<'_, str> = path.file_name().unwrap().to_string_lossy();

        let mut iter = name.split("-");
        let (start, end) = match (iter.next(), iter.next()) {
            (Some(start), Some(end)) => (start.parse::<u64>()?, end.parse::<u64>()?),
            _ => {
                return Err(CoreError::Internal(format!(
                    "{:?} not found start-end",
                    path
                )))
            }
        };

        // dir path size
        let usage_bytes = std::fs::metadata(&path)?.len();

        log::info!(
            "load segment:{:?} start:{:?} end:{:?} size_bytes:{}",
            name,
            start,
            end,
            usage_bytes
        );

        let source_store =
            persist::TreeReader::new(&path.join("_source"), Box::new(RecordDeserializer {}))?;

        let name_store =
            persist::TreeReader::new(&path.join("_name"), Box::new(U32BeDeserializer {}))?;

        let dels_path = path.join("_dels");

        let dels = if dels_path.exists() {
            let buffer = std::fs::read(dels_path)?;
            RwLock::new(Bitmap::deserialize::<Portable>(&buffer))
        } else {
            RwLock::new(Bitmap::new())
        };

        //read version
        let marker = crate::persist::read_version(&path)?.marker;

        let mut index_terms = HashMap::new();

        let mut index_fulltext = HashMap::new();

        for (name, field) in fields.iter() {
            let field_path = path.join(name);

            use proto::core::field::Type::*;
            match field.r#type() {
                Bool | Int | Float | String => {
                    match TermIndex::new_disk(start, field.clone(), field_path) {
                        Ok(ti) => {
                            index_terms.insert(name.clone(), ti.reader());
                        }
                        Err(e) => {
                            log::error!("load term:{:?} index error:{:?}", name, e);
                            return Err(e);
                        }
                    };
                }
                proto::core::field::Type::Text => {
                    match FulltextIndex::new_disk(start, field.clone(), field_path) {
                        Ok(fi) => {
                            index_fulltext.insert(name.clone(), Arc::new(fi.reader()));
                        }
                        Err(e) => {
                            log::error!("load fulltext:{:?} index error:{:?}", name, e);
                            return Err(e);
                        }
                    };
                }
                proto::core::field::Type::Geo => todo!(),
                proto::core::field::Type::Vector => todo!(),
            }
        }

        Ok(DiskSegment {
            start,
            end,
            dels,
            name_store,
            source_store,
            index_terms,
            index_fulltext,
            marker,
            usage_bytes,
        })
    }

    pub(crate) fn get_text_reader(&self, field: &Field) -> CoreResult<Arc<FulltextIndexReader>> {
        self.index_fulltext
            .get(&field.name)
            .cloned()
            .ok_or_else(|| {
                CoreError::InvalidParam(format!("field:{:?} not found in text index", field.name))
            })
    }

    pub fn mark_delete(&self, _del: u64) {
        unimplemented!();
        // self.dels.write().unwrap().add((del - self.start) as u32);
    }

    pub fn end(&self) -> u64 {
        self.end
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn all_record(&self) -> Bitmap {
        let all_record = Bitmap::from_iter((0..self.end - self.start + 1).map(|v| v as u32));
        all_record - &*self.dels.read().unwrap()
    }

    pub fn term(&self, field: &Field, value: &Vec<u8>) -> CoreResult<Bitmap> {
        Ok(self
            .term_reader(&field.name)?
            .term(value)
            .unwrap_or_default())
    }

    pub(crate) fn in_terms(&self, field: &Field, list: &[Vec<u8>]) -> CoreResult<Bitmap> {
        Ok(self.term_reader(&field.name)?.in_terms(list))
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

    fn term_reader(&self, name: &str) -> CoreResult<&TermIndexReader> {
        self.index_terms
            .get(name)
            .ok_or_else(|| CoreError::InvalidParam(format!("field:{:?} not found", name)))
    }

    pub(crate) fn doc(&self, id: u64) -> Option<Cow<Record>> {
        self.source_store.get(&self.abs_id(id)).map(Cow::Owned)
    }

    pub(crate) fn find_by_name(&self, name: &String) -> Option<u64> {
        self.name_store.get(name).map(|id| id as u64 + self.start)
    }

    pub(crate) fn get(&self, name: &String) -> Option<Cow<Record>> {
        self.find_by_name(name).and_then(|id| self.doc(id))
    }

    pub(crate) fn get_field(&self, field: &str) -> Option<Arc<Field>> {
        self.index_terms.get(field).map(|v| v.field().clone())
    }

    fn abs_id(&self, id: u64) -> u32 {
        (id - self.start) as u32
    }

    pub(crate) fn info(&self) -> CoreResult<super::SegmentInfo> {
        Ok(super::SegmentInfo {
            start: self.start,
            end: self.end,
            store_type: "warm".to_string(),
            size_bytes: self.usage_bytes,
            doc_count: self.source_store.len(),
            del_count: self.dels.read().unwrap().cardinality() as u32,
            marker: self.marker.clone(),
        })
    }
}
