use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{atomic::AtomicUsize, Arc},
};

use croaring::Bitmap;
use itertools::Position;
use proto::core::Hit;

use crate::{analyzer::Token, searcher::plan};

use super::index_fulltext::reader::{FulltextIndexReader, PositionList};

pub trait HitStream: Send {
    fn score(&self, id: u64) -> Option<f32>;
    fn next_value(&mut self, value: u64) -> Option<u64>;
}

pub struct CombHitStream {
    streams: Vec<Box<dyn HitStream>>,
    operator: plan::LogicOperator,
    score: f32,
}

impl CombHitStream {
    pub fn new(capacity: usize, operator: plan::LogicOperator) -> Self {
        Self {
            streams: Vec::with_capacity(capacity),
            operator,
            score: 0.0,
        }
    }

    pub fn add(&mut self, stream: Box<dyn HitStream>) {
        self.streams.push(stream);
    }
}

impl HitStream for CombHitStream {
    fn score(&self, id: u64) -> Option<f32> {
        let mut score = 0.0;
        for stream in self.streams.iter() {
            if let Some(s) = stream.score(id) {
                score += s;
            } else if self.operator == plan::LogicOperator::And {
                return None;
            }
        }
        Some(score)
    }
}

pub struct BitmapStream {
    start: u64,
    bitmap: Bitmap,
    boost: f32,
}

impl BitmapStream {
    pub fn new(start: u64, bitmap: Bitmap, boost: f32) -> Self {
        Self {
            start,
            bitmap,
            boost,
        }
    }
}

impl HitStream for BitmapStream {
    fn score(&self, id: u64) -> Option<f32> {
        if self.bitmap.contains((id - self.start) as u32) {
            Some(self.boost)
        } else {
            Some(0.0)
        }
    }
}

pub struct TextStream {
    reader: Arc<FulltextIndexReader>,
    boost: f32,
    term_position: HashMap<String, Option<PositionList>>,
    operator: bool,
    value: Option<u32>,
    score: f32,
    end: bool,
}

impl Debug for TextStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextStream")
            .field("reader", &self.reader.doc_count)
            .field("boost", &self.boost)
            .field("operator", &self.operator)
            .field("value", &self.value)
            .field("score", &self.score)
            .field("end", &self.end)
            .finish()
    }
}

impl TextStream {
    pub(crate) fn new(
        reader: Arc<FulltextIndexReader>,
        boost: f32,
        term_position: HashMap<String, Option<PositionList>>,
        operator: bool,
    ) -> Self {
        Self {
            reader,
            boost,
            term_position,
            operator,
            value: None,
            score: 0.0,
            end: false,
        }
    }
}

impl HitStream for TextStream {
    fn score(&self, id: u64) -> Option<f32> {
        if !self.bitmap.contains((id - self.reader.start) as u32) {
            return None;
        }

        let avgdl = self.reader.avgdl();

        Some(
            self.reader
                .score(id, &self.tokens, &self.token_doc_len, avgdl)
                * self.boost,
        )
    }
}

pub struct PhraseStream {
    boost: f32,
    hits: Vec<Hit>,
    index: AtomicUsize,
}

impl PhraseStream {
    pub(crate) fn new(boost: f32, hits: Vec<Hit>) -> Self {
        Self {
            boost,
            hits,
            index: AtomicUsize::new(0),
        }
    }
}
impl HitStream for PhraseStream {
    fn score(&self, id: u64) -> Option<f32> {
        //binary search hits
        // if  not found return None and set index to pre
        // if found return score and set index to next

        let index = self.index.load(std::sync::atomic::Ordering::Relaxed);

        if index >= self.hits.len() {
            return None;
        }

        match self.hits[index..].binary_search_by(|h| h.id.cmp(&id)) {
            Ok(v) => {
                let hit = &self.hits[v];
                if hit.id == id {
                    self.index
                        .fetch_add(v + 1, std::sync::atomic::Ordering::Relaxed);
                    Some(hit.score * self.boost)
                } else {
                    None
                }
            }
            Err(v) => {
                self.index.store(v, std::sync::atomic::Ordering::Relaxed);
                None
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct VectorStream {
    boost: f32,
    map: HashMap<u64, f32>,
}

impl VectorStream {
    pub(crate) fn new(boost: f32, value: Vec<(f32, u64)>) -> Self {
        let map = value
            .into_iter()
            .map(|(s, i)| (i, s))
            .collect::<HashMap<_, _>>();
        Self { boost, map }
    }
}

impl HitStream for VectorStream {
    fn score(&self, id: u64) -> Option<f32> {
        self.map
            .get(&id)
            .map(|s| s * self.boost)
            .or_else(|| Some(0.0))
    }
}
