pub(crate) mod index_fulltext;
pub(crate) mod index_term;
mod index_vector;
pub mod seacher;
pub mod segment;
mod segment_disk;
pub mod segment_mem;
mod store;
pub mod stream;

use croaring::{Bitmap, Bitmap64};
use proto::core::{Field, Record};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use regex::Regex;
use segment::SegmentReader;
use segment_disk::DiskSegment;
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
    freezed: Vec<SegmentReader>,
    current: MemSegment,
}

impl IndexStore {
    pub fn new(path: &Path, fields: HashMap<String, Arc<proto::core::Field>>) -> CoreResult<Self> {
        let path = path.join("segments");

        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }

        let dirs = Self::segment_dirs(&path)?;

        let freezed = dirs
            .into_par_iter()
            .map(|d| DiskSegment::new(d, &fields).map(|s| SegmentReader::Warm(Arc::new(s))))
            .collect::<CoreResult<Vec<_>>>()?;

        for d in freezed.iter().filter_map(|f| match f {
            SegmentReader::Hot(_) => None,
            SegmentReader::Warm(d) => Some(d),
        }) {
            if let Some(hd) = crate::persist::read_history_del(&d.path)? {
                Self::write_delete(&freezed, hd)?;
                crate::persist::rm_history_del(&d.path)?;
            }
        }

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
            self.current.mark_delete(del);
            if del < self.current.start() {
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
            match self.freezed[index].doc(id)? {
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
        readers.push(SegmentReader::Hot(Arc::new(self.current.reader())));
        readers.extend(self.freezed.iter().cloned());
        readers
    }

    pub fn new_current_segment(&mut self, fields: HashMap<String, Arc<Field>>) -> CoreResult<()> {
        let start = self.current.end();

        let mut segment = MemSegment::new(start, fields)?;
        mem::swap(&mut self.current, &mut segment);
        self.freezed
            .push(SegmentReader::Hot(Arc::new(segment.reader())));
        Ok(())
    }

    pub(crate) fn open_disk_segment(
        &self,
        start: u64,
        end: u64,
        fields: HashMap<String, Arc<proto::core::Field>>,
    ) -> CoreResult<DiskSegment> {
        let path = self.path.join(format!("{}-{}", start, end));
        log::info!("open_disk_segment:{}", path.display());
        DiskSegment::new(path, &fields)
    }

    pub(crate) fn hot_to_warm(&mut self, segment: DiskSegment) -> CoreResult<()> {
        self.freezed
            .iter_mut()
            .find(|f| f.start() == segment.start())
            .map_or_else(
                || Err(CoreError::Internal("segment not found".to_string())),
                |f| {
                    *f = SegmentReader::Warm(Arc::new(segment));
                    Ok(())
                },
            )
    }

    pub(crate) fn info(&self, name: String) -> CoreResult<StoreInfo> {
        StoreInfo::new(name, &self.path, &self.segment_readers())
    }

    /// read segment dirs from path and return sorted value
    fn segment_dirs(path: &Path) -> CoreResult<Vec<PathBuf>> {
        let pattern = Regex::new(r"\d+-\d+(-tmp)?").unwrap();
        let segments = std::fs::read_dir(path)?
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

                    // 解析目录名中的数字,格式为"start-end"
                    let parts: Vec<&str> = name.split('-').collect();
                    if parts.len() < 2 {
                        log::warn!("Invalid segment directory format: {}", name);
                        return None;
                    }

                    let start = parts[0].parse::<u64>().ok()?;
                    let end = parts[1].parse::<u64>().ok()?;
                    let path = d.path();

                    Some((path, start, end))
                }
                Err(_) => None,
            })
            .collect::<Vec<_>>();

        // 按第一个数字(start)分组
        let mut grouped = std::collections::HashMap::new();
        for (path, start, end) in segments {
            grouped
                .entry(start)
                .and_modify(|e: &mut (PathBuf, u64, u64)| {
                    // 如果已有相同start的目录,保留范围更大的(end值更大的)
                    if end > e.2 {
                        *e = (path.clone(), start, end);
                    }
                })
                .or_insert((path, start, end));
        }

        // 提取出最终的目录并按照start排序
        let mut result = grouped
            .into_iter()
            .map(|(_, (dir_entry, _, _))| dir_entry)
            .collect::<Vec<_>>();

        result.sort_by_key(|d| {
            let name = d.file_name().unwrap().to_string_lossy().to_string();
            let start = name
                .split('-')
                .next()
                .unwrap_or("0")
                .parse::<u64>()
                .unwrap_or(0);
            start
        });

        Ok(result)
    }

    fn write_delete(freezed: &[SegmentReader], hd: Bitmap64) -> CoreResult<()> {
        let mut iter = freezed.iter();
        let mut hd_iter = hd.iter();

        let mut bitmap = Bitmap::new();
        let mut pre_disk = None;

        let mut pre = hd_iter.next();
        while let Some(SegmentReader::Warm(d)) = iter.next() {
            if pre.is_none() {
                return Ok(());
            }

            loop {
                let tmp = pre.unwrap();
                if d.start() <= tmp && tmp < d.end() {
                    bitmap.add((tmp - d.start()) as u32);
                    pre = hd_iter.next();
                    pre_disk = Some(d);
                } else if pre_disk.is_none() || bitmap.is_empty() {
                    break;
                } else {
                    let pre_disk = pre_disk.unwrap();
                    pre_disk.merge_delete(&bitmap);
                    crate::persist::merge_del_history(&pre_disk.path, &bitmap)?;
                    bitmap.clear();
                    pre = Some(tmp);
                }
            }
        }

        if pre_disk.is_some() && !bitmap.is_empty() {
            let pre_disk = pre_disk.unwrap();
            pre_disk.merge_delete(&bitmap);
            crate::persist::merge_del_history(&pre_disk.path, &bitmap)?;
        }

        Ok(())
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
