//! # persist of store file structure
//! |-engine_name
//!    |-wal
//!     |-[timstamp].wal
//!    |-schema
//!     |-schema.json
//!     |-user_schema.json
//!    |-segments
//!     |-[start-end]
//!         |-version
//!         |-dels
//!         |-[field_name]
//!             |-field_name.koffset //if variable_index it exist  , u64 array [k1.offset , k2.offset ....]
//!             |-field_name.keys   //key values array [SEGMENT_VERSION][INDEX_TYPE][version, type, fixed_len, key_len]
//!             |-field_name.offset
//!             |-field_name.data
//!
//!
//!

pub mod schema;

use crate::{
    index_store::{
        index_fulltext::{
            reader::FulltextIndexReader,
            serializer::{DocSerializer, TokenSerializer, DOC_INDEX, INDEX_INFO, TERM_INDEX},
        },
        index_term::{reader::TermIndexReader, serializer::TermSerializer},
        segment_mem::MemSegmentReader,
    },
    store::Store,
    util::CoreResult,
};
use croaring::Bitmap;
use mem_btree::{
    persist::{self, KVSerializer, TreeWriter},
    BTree, BatchWrite,
};
use proto::core::Record;
use serde_json::json;
use std::{
    borrow::Cow,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Version {
    pub version: i32,
    pub marker: Option<String>,
}
impl Version {
    pub(crate) fn new(marker: Option<String>) -> Self {
        Self { version: 1, marker }
    }
}

pub fn pos_write(path: PathBuf, data: &[u8]) -> CoreResult<()> {
    let mut bak = path.clone();
    bak.set_extension("tmp");
    std::fs::write(bak.as_path(), data)?;
    std::fs::rename(bak.as_path(), path)?;
    Ok(())
}

pub fn write_segment(store: &Store, reader: Box<MemSegmentReader>) -> CoreResult<()> {
    // // 1.create dir for segment
    let active_path = store
        .base_path()
        .join("segments")
        .join(format!("{}-{}", reader.start, reader.end));

    if active_path.exists() {
        log::warn!("exist segment dir: {:?} so skip it", active_path);
        return Ok(());
    }
    let data_path = store
        .base_path()
        .join("segments")
        .join(format!("{}-{}-tmp", reader.start, reader.end));

    if data_path.exists() {
        std::fs::remove_dir_all(&data_path).unwrap();
        log::warn!("remove exist segment dir: {:?} so remove it", data_path);
    }
    std::fs::create_dir_all(&data_path)?;

    let version = serde_json::to_vec_pretty(&Version::new(reader.marker.clone()))?;

    pos_write(data_path.join("version"), &version)?;

    write_name(&data_path, &reader)?;

    write_source(&data_path, &reader)?;

    write_terms(&data_path, &reader)?;

    wrrite_fulltext(&data_path, &reader)?;

    std::fs::rename(&data_path, active_path)?;

    Ok(())
}

fn wrrite_fulltext(path: &Path, reader: &MemSegmentReader) -> CoreResult<()> {
    let write_fulltext =
        |path: PathBuf, ft: &FulltextIndexReader, dels: &Bitmap| -> std::io::Result<()> {
            let tser: Box<dyn KVSerializer<String, Bitmap>> = Box::new(TokenSerializer);
            let mut persist_tree = BTree::new(1024);
            let mut batch_write = BatchWrite::default();
            ft.token_index.range(None, |k, v| {
                let v = v - dels;
                batch_write.put(k.mem_value().to_string(), v);
                true
            });
            persist_tree.write(batch_write);
            TreeWriter::new(persist_tree, 0, tser).persist(&path.join(TERM_INDEX))?;

            let dser: Box<dyn KVSerializer<(u32, String), Vec<u32>>> = Box::new(DocSerializer);
            let mut persist_tree = BTree::new(1024);
            let mut batch_write = BatchWrite::default();
            ft.doc_index.range(None, |k, v| {
                batch_write.put(k.mem_value().clone(), v.clone());
                true
            });
            persist_tree.write(batch_write);
            TreeWriter::new(persist_tree, 0, dser).persist(&path.join(DOC_INDEX))?;

            let info = json!({
                "doc_count":ft.doc_count,
                "total_term":ft.total_term,
            });

            pos_write(path.join(INDEX_INFO), info.to_string().as_bytes())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            Ok(())
        };

    for (field, ft) in reader.index_fulltext.iter() {
        write_fulltext(path.join(field), ft, &reader.dels)?;
    }

    Ok(())
}

fn write_terms(path: &Path, reader: &MemSegmentReader) -> CoreResult<()> {
    let write_term =
        |path: PathBuf, term: &TermIndexReader, dels: &Bitmap| -> std::io::Result<()> {
            let ser: Box<dyn KVSerializer<Vec<u8>, Bitmap>> = Box::new(TermSerializer {});

            let mut persist_tree = BTree::new(1024);

            let mut batch_write = BatchWrite::default();

            term.range(None, |k, v| {
                let v = v - dels;
                batch_write.put(k.to_vec(ser.as_ref()), v);

                true
            });
            persist_tree.write(batch_write);

            let len = match term.field().r#type() {
                proto::core::field::Type::Bool => 1,
                proto::core::field::Type::Int => 8,
                proto::core::field::Type::Float => 8,
                proto::core::field::Type::String => 0,
                proto::core::field::Type::Text => 0,
                _ => unreachable!(),
            };

            TreeWriter::new(persist_tree, len, ser).persist(&path)
        };

    for e in reader.index_term.iter() {
        write_term(path.join(e.0), e.1, &reader.dels)?;
    }

    Ok(())
}

struct SourceSerializer;

impl persist::KVSerializer<u32, Record> for SourceSerializer {
    fn serialize_key<'a>(&self, k: &'a u32) -> std::borrow::Cow<'a, [u8]> {
        Cow::Owned(k.to_be_bytes().into())
    }

    fn serialize_value<'a>(&self, v: &'a Record) -> std::borrow::Cow<'a, [u8]> {
        Cow::Owned(bincode::serialize(v).unwrap())
    }
}

struct NameSerializer;

impl persist::KVSerializer<String, u32> for NameSerializer {
    fn serialize_key<'a>(&self, k: &'a String) -> std::borrow::Cow<'a, [u8]> {
        Cow::Borrowed(k.as_bytes())
    }

    fn serialize_value<'a>(&self, v: &'a u32) -> std::borrow::Cow<'a, [u8]> {
        Cow::Owned(v.to_be_bytes().into())
    }
}

fn write_name(path: &Path, reader: &MemSegmentReader) -> CoreResult<()> {
    let mut persist_tree = BTree::new(1024);

    let mut bw = BatchWrite::default();

    reader.name_store.iter().for_each(|e| {
        bw.put(e.0.clone(), e.1);
    });

    persist_tree.write(bw);

    TreeWriter::new(persist_tree, 0, Box::new(NameSerializer {})).persist(&path.join("_name"))?;

    Ok(())
}

fn write_source(path: &Path, reader: &MemSegmentReader) -> CoreResult<()> {
    let dels = &reader.dels;
    let mut persist_tree = BTree::new(1024);

    let mut bw = BatchWrite::default();

    reader
        .source_store
        .iter()
        .filter(|e| !dels.contains(e.0))
        .for_each(|e| {
            bw.put(e.0, e.1.clone());
        });

    persist_tree.write(bw);

    TreeWriter::new(persist_tree, 4, Box::new(SourceSerializer {}))
        .persist(&path.join("_source"))?;

    Ok(())
}

pub fn read_version(path: &Path) -> CoreResult<Version> {
    let mut file = File::open(path.join("version"))?;
    let mut data = String::new();
    file.read_to_string(&mut data)?;
    let version: Version = serde_json::from_str(&data)?;
    Ok(version)
}
