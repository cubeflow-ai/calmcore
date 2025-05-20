use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    sync::{atomic::AtomicUsize, Arc},
};

use arrow::csv::reader;
use bytes::buf;
use croaring::{bitmap::BitmapIterator, Bitmap};
use itertools::{Itertools, Position};
use proto::core::Hit;

use crate::{analyzer::Token, searcher::plan};

use super::index_fulltext::reader::{FulltextIndexReader, PositionList};

pub trait HitStream: Send {
    fn score(&self) -> f32;
    fn next_value(&mut self, value: u64) -> Option<u64>;
}

pub struct CombHitStream {
    streams: Vec<Box<dyn HitStream>>,
    operator: plan::LogicOperator,
    value: Option<u64>,
    score: f32,
    buffer: Vec<Option<(u64, f32)>>,
}

impl CombHitStream {
    pub fn new(capacity: usize, operator: plan::LogicOperator) -> Self {
        Self {
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

    fn next_value(&mut self, skip: u64) -> Option<u64> {
        let value = self.value?;

        if skip < value {
            return Some(value);
        }

        match self.operator {
            plan::LogicOperator::And => {
                loop {
                    let mut max_value = 0;
                    let mut all_same = true;
                    let mut all_score = 0.0;

                    for stream in self.streams.iter_mut() {
                        match stream.next_value(max(max_value, skip)) {
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
                        if let Some(id) = stream.next_value(skip) {
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

    fn next_value(&mut self, skip: u64) -> Option<u64> {
        let value = self.value?;
        if value >= skip {
            return Some(value);
        }

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

struct PositionListIter {
    inner: Arc<PositionList>,
    index: usize,
    len: usize,
}

impl PositionListIter {
    fn next(&mut self, id: u32) -> Result<bool, u32> {
        loop {
            if self.index >= self.len {
                return Ok(false);
            }
            let current_id = self.current_id();
            if id > current_id {
                self.index += 1;
                continue;
            } else if id == current_id {
                return Ok(true);
            } else {
                return Err(current_id);
            }
        }
    }

    fn current_id(&self) -> u32 {
        match &*self.inner {
            PositionList::Memory(arc) => arc.read().unwrap().ids[self.index],
            PositionList::Disk(archived) => archived.ids[self.index].to_native(),
        }
    }

    fn end(&self) -> bool {
        self.index >= self.len
    }
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
                Some(Some(inner)) => Some(PositionListIter {
                    len: inner.len(),
                    inner,
                    index: 0,
                }),
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

    fn next_value(&mut self, skip: u64) -> Option<u64> {
        let value = self.value?;

        if value >= skip && skip != 0 {
            return Some(value);
        }

        let except_id = (skip - self.reader.start) as u32;

        if self.operator {
            loop {
                let mut max_value = 0;
                let mut all_score = 0.0;

                for (i, iter) in self.iters.iter_mut().enumerate() {
                    match iter
                        .unwrap()
                        .next(max(max_value, (skip - self.reader.start) as u32))
                    {
                        Ok(true) => {
                            self.buffer[i] = Some(max_value);
                        }
                        Ok(false) => {
                            self.buffer[i] = None;
                        }
                        Err(v) => {
                            self.buffer[i] = Some(v);
                        }
                    }
                }

                let all_same = self
                    .buffer
                    .iter()
                    .all(|v| v.is_some() && v.unwrap() == self.buffer[0].unwrap());

                if all_same {
                    // if all values are some, set value and score
                    self.value = Some(max_value as u64 + self.reader.start);
                    self.score = self.reader.score_text(
                        max_value as u64 + self.reader.start,
                        &self.tokens,
                        &self.term_position,
                        self.avgdl,
                    );
                }
            }
        } else {
            for (i, iter) in self.iters.iter_mut().enumerate() {
                if let Some(iter) = iter {
                    if iter.end() {
                        continue;
                    }
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
    term_position: HashMap<String, PositionList>,
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
        term_position: HashMap<String, PositionList>,
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

    fn next_value(&mut self, skip: u64) -> Option<u64> {
        let value = self.value?;

        if value >= skip && value != 0 {
            return Some(value);
        }

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
    boost: f32,
    hits: Vec<(u64, f32)>,
    index: usize,
    value: Option<u64>,
    score: f32,
}

impl VectorStream {
    pub(crate) fn new(boost: f32, mut value: Vec<(f32, u64)>) -> Self {
        let hits = value
            .into_iter()
            .map(|(f, i)| (i, f))
            .sorted_by(|a, b| a.0.cmp(&b.0))
            .collect();
        Self {
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

    fn next_value(&mut self, skip: u64) -> Option<u64> {
        let value = self.value?;

        if value >= skip {
            return Some(value);
        }

        if self.index >= self.hits.len() {
            self.value = None;
            return None;
        }

        let (id, score) = &self.hits[self.index];
        if *id < skip {
            self.index += 1;
            return self.next_value(skip);
        } else {
            self.value = Some(*id);
            self.score = *score;
            return Some(*id);
        }
    }
}
