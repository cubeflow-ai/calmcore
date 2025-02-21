use std::{borrow::Cow, collections::HashMap, path::PathBuf, sync::Arc};

use croaring::Bitmap;
use proto::core::{Field, Record};

use crate::util::CoreResult;

use super::{
    index_fulltext::reader::FulltextIndexReader,
    segment_disk::DiskSegment,
    segment_mem::{MemSegment, MemSegmentReader},
};

pub enum Segment {
    Hot(Box<MemSegment>),
    Warm(Arc<DiskSegment>),
}

impl Segment {
    pub fn new(
        path: PathBuf,
        fields: &HashMap<String, Arc<proto::core::Field>>,
    ) -> CoreResult<Self> {
        DiskSegment::new(path, fields).map(|d| Segment::Warm(Arc::new(d)))
    }

    pub(crate) fn mark_delete(&self, del: u64) {
        match self {
            Segment::Hot(s) => s.mark_delete(del),
            Segment::Warm(s) => s.mark_delete(del),
        }
    }

    pub(crate) fn end(&self) -> u64 {
        match self {
            Segment::Hot(h) => h.end(),
            Segment::Warm(w) => w.end(),
        }
    }

    pub(crate) fn start(&self) -> u64 {
        match self {
            Segment::Hot(h) => h.start(),
            Segment::Warm(w) => w.start(),
        }
    }

    pub(crate) fn reader(&self) -> SegmentReader {
        match self {
            Segment::Hot(h) => SegmentReader::Hot(Box::new(h.reader())),
            Segment::Warm(w) => SegmentReader::Warm(w.clone()),
        }
    }

    pub(crate) fn find_by_name(&self, name: &String) -> Option<u64> {
        match self {
            Segment::Hot(h) => h.find_by_name(name),
            Segment::Warm(w) => w.find_by_name(name),
        }
    }
}

pub enum SegmentReader {
    Hot(Box<MemSegmentReader>),
    Warm(Arc<DiskSegment>),
}

impl SegmentReader {
    pub fn start(&self) -> u64 {
        match self {
            SegmentReader::Hot(h) => h.start,
            SegmentReader::Warm(w) => w.start(),
        }
    }

    pub fn end(&self) -> u64 {
        match self {
            SegmentReader::Hot(h) => h.end,
            SegmentReader::Warm(w) => w.end(),
        }
    }

    pub(crate) fn term(&self, field: &Field, value: &Vec<u8>) -> CoreResult<Bitmap> {
        match self {
            SegmentReader::Hot(mem) => mem.term(field, value),
            SegmentReader::Warm(disk) => disk.term(field, value),
        }
    }

    pub(crate) fn all_record(&self) -> Bitmap {
        match self {
            SegmentReader::Hot(h) => h.all_record(),
            SegmentReader::Warm(w) => w.all_record(),
        }
    }

    pub(crate) fn between(
        &self,
        field: &Field,
        low: Option<&Vec<u8>>,
        low_eq: bool,
        high: Option<&Vec<u8>>,
        high_eq: bool,
    ) -> CoreResult<Bitmap> {
        match self {
            SegmentReader::Hot(h) => h.between(field, low, low_eq, high, high_eq),
            SegmentReader::Warm(w) => w.between(field, low, low_eq, high, high_eq),
        }
    }

    pub(crate) fn in_terms(&self, field: &Field, list: &[Vec<u8>]) -> CoreResult<Bitmap> {
        match self {
            SegmentReader::Hot(h) => h.in_terms(field, list),
            SegmentReader::Warm(w) => w.in_terms(field, list),
        }
    }

    pub(crate) fn get_text_reader(&self, field: &Field) -> CoreResult<Arc<FulltextIndexReader>> {
        match self {
            SegmentReader::Hot(h) => h.get_text_reader(field),
            SegmentReader::Warm(w) => w.get_text_reader(field),
        }
    }

    pub(crate) fn doc(&self, id: u64) -> Option<Cow<Record>> {
        match self {
            SegmentReader::Hot(h) => h.doc(id),
            SegmentReader::Warm(w) => w.doc(id),
        }
    }

    pub(crate) fn get(&self, name: &String) -> Option<Cow<Record>> {
        match self {
            SegmentReader::Hot(h) => h.get(name),
            SegmentReader::Warm(w) => w.get(name),
        }
    }

    pub(crate) fn get_field(&self, field: &str) -> Option<Arc<Field>> {
        match self {
            SegmentReader::Hot(h) => h.get_field(field),
            SegmentReader::Warm(w) => w.get_field(field),
        }
    }

    pub fn is_hot(&self) -> bool {
        match self {
            SegmentReader::Hot(_) => true,
            SegmentReader::Warm(_) => false,
        }
    }

    pub fn live_time(&self) -> std::time::Duration {
        match self {
            SegmentReader::Hot(h) => h.live_time,
            SegmentReader::Warm(_) => unreachable!(),
        }
    }

    pub(crate) fn info(&self) -> CoreResult<super::SegmentInfo> {
        match self {
            SegmentReader::Hot(h) => h.info(),
            SegmentReader::Warm(w) => w.info(),
        }
    }
}
