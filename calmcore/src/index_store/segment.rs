use std::{borrow::Cow, sync::Arc};

use croaring::Bitmap;
use proto::core::{Field, Record};

use crate::util::CoreResult;

use super::{
    index_fulltext::reader::FulltextIndexReader, segment_disk::DiskSegment,
    segment_mem::MemSegmentReader,
};

#[derive(Clone)]
pub enum SegmentReader {
    Hot(Arc<MemSegmentReader>),
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

    pub(crate) fn batch_doc(&self, ids: &[u64]) -> Vec<Option<Cow<Record>>> {
        match self {
            SegmentReader::Hot(h) => h.batch_doc(ids),
            SegmentReader::Warm(w) => w.batch_doc(ids),
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

    pub(crate) fn mark_delete(&self, del: u64) {
        match self {
            SegmentReader::Hot(h) => h.mark_delete(del),
            SegmentReader::Warm(w) => w.mark_delete(del),
        }
    }

    pub(crate) fn find_by_name(&self, name: &String) -> Option<u64> {
        match self {
            SegmentReader::Hot(h) => h.find_by_name(name),
            SegmentReader::Warm(w) => w.find_by_name(name),
        }
    }
}
