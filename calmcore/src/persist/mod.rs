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

pub mod arrow_util;
pub mod block_reader;
pub mod schema;

use crate::{
    entity::TermPosition,
    index_store::{
        index_fulltext::{
            reader::FulltextIndexReader,
            serializer::{
                PositionSerializer, TokenSerializer, INDEX_INFO, TERM_INDEX, TERM_POSITION,
                VECTOR_INDEX,
            },
        },
        index_term::{reader::TermIndexReader, serializer::TermSerializer},
        segment_mem::MemSegmentReader,
        store::VectorIndexReader,
    },
    store::Store,
    util::{CoreError, CoreResult},
};
use arrow::array::{RecordBatch, StructArray, StructBuilder};
use arrow_util::{write_none, write_object_to_arrow};
use croaring::{Bitmap, Bitmap64, Portable};
use hora::core::ann_index::SerializableIndex;
use itertools::Itertools;
use mem_btree::{
    persist::{self, KVSerializer, TreeWriter},
    BTree, BatchWrite,
};

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde_json::json;
use std::{
    borrow::Cow,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
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

pub fn write_segment(store: &Store, reader: Arc<MemSegmentReader>) -> CoreResult<()> {
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

    let version = serde_json::to_vec_pretty(&Version::new(reader.marker()))?;

    pos_write(data_path.join("version"), &version)?;

    write_del(&data_path, &reader)?;

    let start = std::time::Instant::now();
    write_name(&data_path, &reader)?;
    println!("write_name cost:{:?}", start.elapsed());

    let start = std::time::Instant::now();
    write_source(&data_path, &reader)?;
    println!("write_source cost:{:?}", start.elapsed());

    write_terms(&data_path, &reader)?;

    write_fulltext(&data_path, &reader)?;

    write_vector(&data_path, &reader)?;

    std::fs::rename(&data_path, active_path)?;

    Ok(())
}

fn write_del(data_path: &Path, reader: &MemSegmentReader) -> CoreResult<()> {
    if !reader.dels.read().unwrap().is_empty() {
        let buffer = reader.dels.read().unwrap().serialize::<Portable>();
        pos_write(data_path.join("_dels"), &buffer)?;
    }
    if !reader.dels_history.is_empty() {
        let buffer = reader.dels_history.serialize::<Portable>();
        pos_write(data_path.join("_dels_history"), &buffer)?;
    }
    Ok(())
}

pub fn read_del(data_path: &Path) -> CoreResult<Bitmap> {
    let path = data_path.join("_dels");
    if path.exists() {
        let buffer = std::fs::read(path)?;
        Ok(Bitmap::deserialize::<Portable>(&buffer))
    } else {
        Ok(Bitmap::new())
    }
}

pub fn read_history_del(data_path: &Path) -> CoreResult<Option<Bitmap64>> {
    let path = data_path.join("_dels_history");
    if path.exists() {
        let buffer = std::fs::read(path)?;
        Ok(Some(Bitmap64::deserialize::<Portable>(&buffer)))
    } else {
        Ok(None)
    }
}

pub fn rm_history_del(data_path: &Path) -> CoreResult<()> {
    let path = data_path.join("_dels_history");
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

pub fn merge_del_history(data_path: &Path, dels: &Bitmap) -> CoreResult<()> {
    let mut data = read_del(data_path)?;
    data.or_inplace(dels);
    data.run_optimize();
    data.shrink_to_fit();
    let buffer = data.serialize::<Portable>();
    pos_write(data_path.join("_dels"), &buffer)
}

fn write_fulltext(path: &Path, reader: &MemSegmentReader) -> CoreResult<()> {
    let write_fulltext = |path: PathBuf, ft: &FulltextIndexReader| -> CoreResult<()> {
        let dser: Box<dyn KVSerializer<String, Arc<RwLock<TermPosition>>>> =
            Box::new(PositionSerializer::new());
        let mut persist_tree = BTree::new(1024);

        persist_tree.merge(ft.term_position.clone_map());

        TreeWriter::new(persist_tree, 0, dser).persist(&path.join(TERM_POSITION))?;

        let info = json!({
            "doc_count":ft.doc_count,
            "total_term":ft.total_term,
        });

        pos_write(path.join(INDEX_INFO), info.to_string().as_bytes())?;

        Ok(())
    };

    for (field, ft) in reader.index_fulltext.iter() {
        write_fulltext(path.join(field), ft)?;
    }

    Ok(())
}

fn write_vector(path: &Path, reader: &MemSegmentReader) -> CoreResult<()> {
    let writer = |path: PathBuf, ft: &VectorIndexReader| -> CoreResult<()> {
        let mut index = ft.build_index()?;
        index
            .dump(path.join(VECTOR_INDEX).as_os_str().to_str().unwrap())
            .map_err(|s| CoreError::Internal(s.to_string()))?;
        Ok(())
    };

    for (field, ft) in reader.index_vector.iter() {
        writer(path.join(field), ft)?;
    }

    Ok(())
}

fn write_terms(path: &Path, reader: &MemSegmentReader) -> CoreResult<()> {
    let write_term = |path: PathBuf, term: &TermIndexReader, dels: &Bitmap| -> CoreResult<()> {
        let ser: Box<dyn KVSerializer<Vec<u8>, Bitmap>> = Box::new(TermSerializer {});

        let mut persist_tree = BTree::new(1024);

        let mut batch_write = BatchWrite::default();

        term.range(None, |k, v| {
            let v = v - dels;
            batch_write.put(k.to_vec(ser.as_ref()), v);

            true
        })?;
        persist_tree.write(batch_write);

        let len = match term.field().r#type() {
            proto::core::field::Type::Bool => 1,
            proto::core::field::Type::Int => 8,
            proto::core::field::Type::Float => 8,
            proto::core::field::Type::String => 0,
            proto::core::field::Type::Text => 0,
            _ => unreachable!(),
        };

        Ok(TreeWriter::new(persist_tree, len, ser).persist(&path)?)
    };

    for r in reader
        .index_term
        .par_iter()
        .map(|(field, term)| write_term(path.join(field), term, &reader.dels.read().unwrap()))
        .collect::<Vec<_>>()
    {
        r?;
    }

    Ok(())
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
    persist_tree.merge(reader.name_store.clone());
    TreeWriter::new(persist_tree, 0, Box::new(NameSerializer {})).persist(&path.join("_name"))?;

    Ok(())
}

fn write_source(path: &Path, reader: &MemSegmentReader) -> CoreResult<()> {
    let path = &path.join("_source");
    std::fs::create_dir_all(path)?;

    let dels = &reader.dels;

    let schema = arrow_util::make_arrow_schema(reader)?;

    let mut builder = StructBuilder::from_fields(schema.fields.clone(), 1024);

    let mut index_file = File::create(path.join("index"))?;
    let mut data_file = File::create(path.join("data"))?;

    for chunk in &reader.source_store.iter().chunks(1024) {
        let arr = chunk.collect_vec();

        let start = arr.first().unwrap().0 as u64 + reader.start;

        let end = arr.last().unwrap().0 as u64 + reader.start;

        for a in arr {
            if dels.read().unwrap().contains(a.0) {
                write_none(&schema, &mut builder);
            } else {
                write_object_to_arrow(&schema, &mut builder, &a.1)?;
            }
            builder.append(true);
        }

        write_block(
            &mut index_file,
            &mut data_file,
            start,
            end,
            builder.finish(),
        )?;
    }

    index_file.flush()?;
    data_file.flush()?;

    Ok(())
}

fn write_block(
    index_file: &mut File,
    data_file: &mut File,
    start: u64,
    end: u64,
    sa: StructArray,
) -> CoreResult<()> {
    use parquet::{
        arrow::ArrowWriter,
        basic::Compression,
        file::properties::{EnabledStatistics, WriterProperties},
    };

    let record_batch = RecordBatch::from(sa);

    let props = WriterProperties::builder()
        .set_compression(Compression::LZ4)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_size(1024 * 1024)
        .build();

    index_file.write_all(start.to_be_bytes().as_ref())?;
    index_file.write_all(end.to_be_bytes().as_ref())?;
    index_file.write_all(data_file.metadata().unwrap().len().to_be_bytes().as_ref())?;

    {
        // 创建ArrowWriter，它会将Arrow数据转换为Parquet格式
        let mut writer = ArrowWriter::try_new(&mut *data_file, record_batch.schema(), Some(props))?;
        // 写入记录批次
        writer.write(&record_batch)?;
        // 关闭写入器，确保数据被刷新到磁盘
        writer.close()?;
    }

    index_file.write_all(data_file.metadata().unwrap().len().to_be_bytes().as_ref())?;

    Ok(())
}

pub fn read_version(path: &Path) -> CoreResult<Version> {
    let mut file = File::open(path.join("version"))?;
    let mut data = String::new();
    file.read_to_string(&mut data)?;
    let version: Version = serde_json::from_str(&data)?;
    Ok(version)
}
