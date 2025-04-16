use std::{collections::HashMap, fmt::Debug, sync::Arc};

use croaring::Bitmap;

use crate::{analyzer::Token, searcher::plan};

use super::index_fulltext::reader::FulltextIndexReader;

pub trait HitStream: Send {
    fn score(&self, id: u64) -> Option<f32>;
}

pub struct CombHitStream {
    streams: Vec<Box<dyn HitStream>>,
    operator: plan::LogicOperator,
}

impl CombHitStream {
    pub fn new(capacity: usize, operator: plan::LogicOperator) -> Self {
        Self {
            streams: Vec::with_capacity(capacity),
            operator,
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
    tokens: Vec<Token>,
    bitmap: Bitmap,
    token_doc_len: HashMap<String, usize>,
    operator: bool,
    slop: i32,
    value: Option<u32>,
    score: f32,
    end: bool,
}

impl Debug for TextStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextStream")
            .field("reader", &self.reader.doc_count)
            .field("boost", &self.boost)
            .field("tokens", &self.tokens)
            .field("token_doc_len", &self.token_doc_len)
            .field("operator", &self.operator)
            .field("slop", &self.slop)
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
        tokens: Vec<Token>,
        bitmap: Bitmap,
        token_doc_len: HashMap<String, usize>,
        operator: bool,
        slop: i32,
    ) -> Self {
        Self {
            reader,
            boost,
            tokens,
            bitmap,
            token_doc_len,
            operator,
            slop,
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

        self.reader
            .score(id, &self.tokens, &self.token_doc_len, self.slop)
            .map(|s| s * self.boost)
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
