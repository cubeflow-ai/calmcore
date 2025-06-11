use std::{
    cmp::max,
    collections::HashMap,
    fmt::Debug,
    sync::{atomic::AtomicUsize, Arc},
};

use croaring::bitmap::BitmapIterator;
use itertools::Itertools;

use crate::{
    analyzer::Token, index_store::index_fulltext::reader::PositionListIter, searcher::plan,
};

use super::index_fulltext::reader::{FulltextIndexReader, PositionList};

pub trait HitStream: Send {
    fn score(&self) -> f32;
    fn next_value(&mut self, value: Option<u64>) -> Option<u64>;
}

pub struct CombHitStream {
    start: u64,
    streams: Vec<Box<dyn HitStream>>,
    operator: plan::LogicOperator,
    value: Option<u64>,
    score: f32,
    buffer: Vec<Option<(u64, f32)>>,
}

impl CombHitStream {
    pub fn new(start: u64, capacity: usize, operator: plan::LogicOperator) -> Self {
        Self {
            start,
            streams: Vec::with_capacity(capacity),
            operator,
            value: Some(0),
            score: 0.0,
            buffer: Vec::with_capacity(capacity),
        }
    }

    pub fn add(&mut self, stream: Box<dyn HitStream>) {
        self.streams.push(stream);
        self.buffer.push(None);
    }
}

impl HitStream for CombHitStream {
    fn score(&self) -> f32 {
        self.score
    }

    fn next_value(&mut self, skip: Option<u64>) -> Option<u64> {
        let value = self.value?;

        let skip = match skip {
            Some(skip) => {
                if skip < value {
                    return Some(value);
                } else {
                    skip
                }
            }
            None => self.start,
        };

        match self.operator {
            plan::LogicOperator::And => {
                let mut max_value = skip;
                loop {
                    let mut all_same = true;
                    let mut all_score = 0.0;

                    for stream in self.streams.iter_mut() {
                        match stream.next_value(Some(max_value)) {
                            Some(v) => {
                                if max_value < v {
                                    if max_value != 0 {
                                        all_same = false;
                                    }
                                    max_value = v;
                                }
                                if all_same {
                                    all_score += stream.score();
                                }
                            }
                            None => {
                                // if more than one value is none, set self.value to None
                                self.value = None;
                                return None;
                            }
                        }
                    }

                    if all_same {
                        // if all values are some, set value and score
                        self.value = Some(max_value);
                        self.score = all_score;
                        return Some(max_value);
                    }

                    // if more than one value is not same, skip value to max . loop check for this
                }
            }
            plan::LogicOperator::Or => {
                for i in 0..self.buffer.len() {
                    let stream = &mut self.streams[i];
                    if self.buffer[i].is_none() {
                        if let Some(id) = stream.next_value(Some(skip)) {
                            self.buffer[i] = Some((id, stream.score()));
                        }
                    }
                }

                let id = self
                    .buffer
                    .iter()
                    .filter(|v| v.is_some())
                    .min_by_key(|v| v.unwrap().0);

                if id.is_none() {
                    self.value = None;
                    return None;
                }

                let min = id.unwrap().unwrap().0;

                for value in self.buffer.iter_mut() {
                    if let Some((id, score)) = value {
                        if *id == min {
                            self.score += *score;
                            *value = None;
                        }
                    }
                }

                self.value = Some(min);
                self.value
            }
        }
    }
}

pub struct BitmapStream {
    start: u64,
    iter: BitmapIterator<'static>,
    value: Option<u64>,
    score: f32,
}

impl BitmapStream {
    pub fn new(start: u64, boost: f32, iter: BitmapIterator<'_>) -> Self {
        let iter =
            unsafe { std::mem::transmute::<BitmapIterator<'_>, BitmapIterator<'static>>(iter) };
        Self {
            start,
            iter,
            value: Some(0),
            score: boost,
        }
    }
}

impl HitStream for BitmapStream {
    fn score(&self) -> f32 {
        self.score
    }

    fn next_value(&mut self, skip: Option<u64>) -> Option<u64> {
        let value = self.value?;
        let skip = match skip {
            Some(skip) => {
                if skip < value {
                    return Some(value);
                } else {
                    skip
                }
            }
            None => self.start,
        };

        let value = (skip - self.start) as u32;
        loop {
            let v = match self.iter.next() {
                Some(v) => v,
                None => {
                    self.value = None;
                    return None;
                }
            };
            if v >= value {
                self.value = Some(v as u64 + self.start);
                return Some(v as u64 + self.start);
            }
        }
    }
}

pub struct TextStream {
    reader: Arc<FulltextIndexReader>,
    boost: f32,
    tokens: Vec<Token>,
    buffer: Vec<Option<u32>>,
    iters: Vec<Option<PositionListIter>>,
    term_position: HashMap<String, Option<Arc<PositionList>>>,
    avgdl: f32,
    operator: bool,
    value: Option<u64>,
    score: f32,
}

impl Debug for TextStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextStream")
            .field("reader", &self.reader.doc_count)
            .field("boost", &self.boost)
            .field("operator", &self.operator)
            .field("value", &self.value)
            .field("score", &self.score)
            .finish()
    }
}

impl TextStream {
    pub(crate) fn new(
        reader: Arc<FulltextIndexReader>,
        boost: f32,
        tokens: Vec<Token>,
        term_position: HashMap<String, Option<PositionList>>,
        operator: bool,
    ) -> Self {
        let term_position: HashMap<String, Option<Arc<PositionList>>> = term_position
            .into_iter()
            .map(|(k, v)| (k, v.map(Arc::new)))
            .collect();

        let iters = tokens
            .iter()
            .map(|token| match term_position.get(&token.name).cloned() {
                Some(Some(inner)) => Some(PositionListIter::new(inner)),
                _ => None,
            })
            .collect();

        let value = if term_position.iter().any(|(_, v)| v.is_none()) {
            None
        } else {
            Some(0)
        };

        let avgdl = reader.avgdl();

        Self {
            reader,
            boost,
            buffer: vec![None; tokens.len()],
            tokens,
            iters,
            term_position,
            operator,
            avgdl,
            value,
            score: 0.0,
        }
    }
}

impl HitStream for TextStream {
    fn score(&self) -> f32 {
        self.score * self.boost
    }

    fn next_value(&mut self, skip: Option<u64>) -> Option<u64> {
        let value = self.value?;

        let skip = match skip {
            Some(skip) => {
                if skip < value {
                    return Some(value);
                } else {
                    skip
                }
            }
            None => self.reader.start as u64,
        };

        let except_id = (skip - self.reader.start) as u32;

        if self.operator || self.tokens.len() == 1 {
            let mut max_value = (skip - self.reader.start) as u32;
            loop {
                for (i, iter) in self.iters.iter_mut().enumerate() {
                    match iter.as_mut().unwrap().next(max_value) {
                        Ok(true) => {
                            self.buffer[i] = Some(max_value);
                        }
                        Ok(false) => {
                            self.value = None;
                            return None;
                        }
                        Err(v) => {
                            self.buffer[i] = Some(v);
                        }
                    }
                }

                let all_same = self.buffer.iter().all(|v| {
                    if let Some(v) = v {
                        max_value = max(max_value, *v);
                        *v == self.buffer[0].unwrap()
                    } else {
                        false
                    }
                });

                if all_same {
                    // if all values are some, set value and score
                    if max_value < 10 {
                        println!(
                            "TextStream::next_value all_same: {:?}============={:?}",
                            max_value, self.reader.start
                        );
                    }

                    self.value = Some(max_value as u64 + self.reader.start);
                    self.score = self.reader.score_text(
                        max_value as u64 + self.reader.start,
                        &self.tokens,
                        &self.term_position,
                        self.avgdl,
                    );
                    return self.value;
                }
            }
        } else {
            for (i, iter) in self.iters.iter_mut().enumerate() {
                if let Some(iter) = iter {
                    match iter.next(except_id) {
                        Ok(true) => {
                            self.buffer[i] = Some(except_id);
                        }
                        Ok(false) => {
                            self.buffer[i] = None;
                        }
                        Err(v) => {
                            self.buffer[i] = Some(v);
                        }
                    }
                }
            }

            let id = self
                .buffer
                .iter()
                .filter(|v| v.is_some())
                .min_by_key(|v| v.unwrap());

            if id.is_none() {
                self.value = None;
                return None;
            }

            let min = id.unwrap().unwrap();

            let score = self.reader.score_text(
                min as u64 + self.reader.start,
                &self.tokens,
                &self.term_position,
                self.avgdl,
            );

            self.value = Some(min as u64 + self.reader.start);
            self.score = score;
        }

        self.value
    }
}

pub struct PhraseStream {
    reader: Arc<FulltextIndexReader>,
    boost: f32,
    tokens: Vec<Token>,
    term_position: HashMap<String, Arc<PositionList>>,
    hits: Vec<u64>,
    index: AtomicUsize,
    value: Option<u64>,
    score: f32,
    avgdl: f32,
}

impl PhraseStream {
    pub(crate) fn new(
        reader: Arc<FulltextIndexReader>,
        boost: f32,
        hits: Vec<u64>,
        tokens: Vec<Token>,
        term_position: HashMap<String, Arc<PositionList>>,
    ) -> Self {
        let avgdl = reader.avgdl();
        Self {
            reader,
            boost,
            hits,
            tokens,
            term_position,
            index: AtomicUsize::new(0),
            value: Some(0),
            score: 0.0,
            avgdl,
        }
    }
}
impl HitStream for PhraseStream {
    fn score(&self) -> f32 {
        self.score * self.boost
    }

    fn next_value(&mut self, skip: Option<u64>) -> Option<u64> {
        let value = self.value?;

        let skip = match skip {
            Some(skip) => {
                if skip < value {
                    return Some(value);
                } else {
                    skip
                }
            }
            None => self.reader.start as u64,
        };

        for i in &self.hits[self.index.load(std::sync::atomic::Ordering::Relaxed)..] {
            let id = *i;
            if id < skip {
                self.index
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                self.value = Some(id);
                self.score =
                    self.reader
                        .score_phrase(id, &self.tokens, &self.term_position, self.avgdl);
                break;
            }
        }
        self.value
    }
}

#[derive(Default, Debug)]
pub struct VectorStream {
    start: u64,
    boost: f32,
    hits: Vec<(u64, f32)>,
    index: usize,
    value: Option<u64>,
    score: f32,
}

impl VectorStream {
    pub(crate) fn new(start: u64, boost: f32, value: Vec<(f32, u64)>) -> Self {
        let hits = value
            .into_iter()
            .map(|(f, i)| (i, f))
            .sorted_by(|a, b| a.0.cmp(&b.0))
            .collect();
        Self {
            start,
            boost,
            hits,
            index: 0,
            value: Some(0),
            score: 0.0,
        }
    }
}

impl HitStream for VectorStream {
    fn score(&self) -> f32 {
        self.score * self.boost
    }

    fn next_value(&mut self, skip: Option<u64>) -> Option<u64> {
        let value = self.value?;

        let skip = match skip {
            Some(skip) => {
                if skip < value {
                    return Some(value);
                } else {
                    skip
                }
            }
            None => self.start as u64,
        };

        if self.index >= self.hits.len() {
            self.value = None;
            return None;
        }

        let (id, score) = &self.hits[self.index];
        if *id < skip {
            self.index += 1;
            return self.next_value(Some(skip));
        } else {
            self.value = Some(*id);
            self.score = *score;
            return Some(*id);
        }
    }
}
