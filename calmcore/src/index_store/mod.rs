pub(crate) mod index_fulltext;
pub(crate) mod index_term;
mod index_vector;
pub mod seacher;
pub mod segment;
mod segment_disk;
pub mod segment_mem;
mod store;
pub mod stream;

use itertools::Itertools;
use proto::core::{Field, Record};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use regex::Regex;
use segment::{Segment, SegmentReader};
use segment_mem::MemSegment;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::HashMap,
    mem,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    util::{CoreError, CoreResult},
    RecordWrapper,
};

/// index fields schema
/// in the future, one field may have multiple index
/// example  title need two index type,  {title: fulltext, title: keyword} so we can make  title1 -> keywordindex  and title2 -> fulltext index
/// title -> [Field{name:title1 , field_name=title, index_opt=keyword}, Field{name:title2 , field_name=title, index_opt=fulltext}]
pub struct IndexStore {
    path: PathBuf,
    freezed: Vec<Segment>,
    current: MemSegment,
}

impl IndexStore {
    pub fn new(path: &Path, fields: HashMap<String, Arc<proto::core::Field>>) -> CoreResult<Self> {
        let path = path.join("segments");

        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }

        let pattern = Regex::new(r"\d+-\d+(-tmp)?").unwrap();
        let dir = std::fs::read_dir(&path)?
            .filter_map(|r| match r {
                Ok(d) => {
                    let name = d.file_name().to_string_lossy().into_owned();
                    if !d.path().is_dir() || !pattern.is_match(&name) {
                        log::warn!("Invalid segment directory: {}", name);
                        return None;
                    }
                    if name.ends_with("-tmp") {
                        if let Err(e) = std::fs::remove_dir_all(d.path()) {
                            log::warn!("Failed to remove tmp directory {}: {}", name, e);
                        }
                        return None;
                    }
                    Some(d)
                }
                Err(_) => None,
            })
            .collect_vec();

        let freezed = dir
            .par_iter()
            .map(|d| Segment::new(d.path(), &fields))
            .collect::<Result<Vec<Segment>, CoreError>>()?;

        let end = freezed.iter().map(|f| f.end()).max().unwrap_or(0);

        let current = MemSegment::new(end, fields)?;
        Ok(Self {
            path,
            freezed,
            current,
        })
    }

    pub fn write(
        &self,
        records: Vec<RecordWrapper>,
        dels: Vec<u64>,
        max: u64,
        marker: Option<String>,
    ) -> Vec<CoreError> {
        dels.into_iter().for_each(|del| {
            if del >= self.current.start() {
                self.current.mark_delete(del);
            } else {
                let index = self
                    .freezed
                    .binary_search_by(|f| f.start().cmp(&del))
                    .unwrap_or_else(|v| v - 1);
                self.freezed[index].mark_delete(del);
            }
        });

        self.current.write_records(records, max, marker)
    }
    pub fn find_by_id(&self, id: u64) -> Option<Record> {
        if id >= self.current.start() {
            self.current.find_by_id(id)
        } else {
            let index = self
                .freezed
                .binary_search_by(|f| f.start().cmp(&id))
                .unwrap_or_else(|v| v - 1);
            match self.freezed[index].reader().doc(id)? {
                Cow::Borrowed(v) => Some(v.clone()),
                Cow::Owned(v) => Some(v),
            }
        }
    }

    pub fn find_by_name(&self, name: &String) -> Option<u64> {
        if name.is_empty() {
            return None;
        }
        let id = self.current.find_by_name(name);
        if id.is_some() {
            return id;
        }
        self.freezed
            .par_iter()
            .find_map_any(|s| s.find_by_name(name))
    }

    pub(crate) fn segment_readers(&self) -> Vec<SegmentReader> {
        let mut readers = Vec::with_capacity(self.freezed.len() + 1);
        readers.push(SegmentReader::Hot(Box::new(self.current.reader())));
        readers.extend(self.freezed.iter().map(|f| f.reader()));
        readers
    }

    pub fn new_current_segment(&mut self, fields: HashMap<String, Arc<Field>>) -> CoreResult<()> {
        let start = self.current.end();

        let mut segment = MemSegment::new(start, fields)?;
        mem::swap(&mut self.current, &mut segment);
        self.freezed.push(Segment::Hot(Box::new(segment)));
        Ok(())
    }

    pub(crate) fn open_disk_segment(
        &self,
        start: u64,
        end: u64,
        fields: HashMap<String, Arc<proto::core::Field>>,
    ) -> CoreResult<Segment> {
        let path = self.path.join(format!("{}-{}", start, end));
        log::info!("open_disk_segment:{}", path.display());
        Segment::new(path, &fields)
    }

    pub(crate) fn hot_to_warm(&mut self, segment: Segment) -> CoreResult<()> {
        self.freezed
            .iter_mut()
            .find(|f| f.start() == segment.start())
            .map_or_else(
                || Err(CoreError::Internal("segment not found".to_string())),
                |f| {
                    *f = segment;
                    Ok(())
                },
            )
    }

    pub(crate) fn info(&self, name: String) -> CoreResult<StoreInfo> {
        StoreInfo::new(name, &self.path, &self.segment_readers())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoreInfo {
    pub name: String,
    pub path: String,
    pub segments: Vec<SegmentInfo>,
    pub total_doc_count: u32,
    pub disk_size_bytes: u64,
    pub mem_size_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SegmentInfo {
    pub start: u64,
    pub end: u64,
    pub store_type: String,
    pub size_bytes: u64,
    pub doc_count: u32,
    pub del_count: u32,
    pub marker: Option<String>,
}

impl StoreInfo {
    fn new(name: String, path: &Path, segments: &[SegmentReader]) -> CoreResult<StoreInfo> {
        if segments.is_empty() {
            return Err(CoreError::InvalidParam("no segments provided".to_string()));
        }

        let mut total_doc_count = 0u32;
        let mut disk_size_bytes = 0u64;
        let mut mem_size_bytes = 0u64;

        let segments = segments
            .iter()
            .map(|seg| seg.info())
            .collect::<CoreResult<Vec<SegmentInfo>>>()?;

        for seg in &segments {
            if seg.store_type == "hot" {
                mem_size_bytes += seg.size_bytes;
            } else {
                disk_size_bytes += seg.size_bytes;
            }
            total_doc_count += seg.doc_count;
        }
        Ok(StoreInfo {
            name,
            path: path.to_string_lossy().to_string(),
            segments,
            total_doc_count,
            disk_size_bytes,
            mem_size_bytes,
        })
    }
}
