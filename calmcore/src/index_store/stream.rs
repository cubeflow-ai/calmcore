use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    sync::Arc,
};

use croaring::bitmap::BitmapIterator;

use crate::analyzer::Token;

use super::{index_fulltext::reader::FulltextIndexReader, seacher::plan};

pub trait HitStream: Send + Debug {
    fn next(&mut self);
    fn value(&self) -> Option<u64>;
    fn score(&self) -> f32;
    fn next_value(&mut self, value: u64) -> Option<u64>;
}

#[derive(Debug)]
pub struct CombHitStream {
    streams: Vec<Box<dyn HitStream>>,
    operator: plan::LogicOperator,
    value: Option<u64>,
    score: f32,
    map: BTreeMap<u64, f32>,
}

impl CombHitStream {
    pub fn new(capacity: usize, operator: plan::LogicOperator) -> Self {
        Self {
            streams: Vec::with_capacity(capacity),
            operator,
            value: Some(0),
            score: 0.0,
            map: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, stream: Box<dyn HitStream>) {
        self.streams.push(stream);
    }
}

impl HitStream for CombHitStream {
    fn next(&mut self) {
        let value = match self.value {
            Some(v) => v,
            None => return,
        };

        match self.operator {
            plan::LogicOperator::And => {
                loop {
                    let mut max_value = 0;
                    let mut all_same = true;
                    let mut all_score = 0.0;

                    for stream in self.streams.iter_mut() {
                        let value = stream.next_value(value + 1);
                        match value {
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
                                return;
                            }
                        }
                    }

                    if all_same {
                        // if all values are some, set value and score
                        self.value = Some(max_value);
                        self.score = all_score;
                        return;
                    }

                    // if more than one value is not same, skip value to max . loop check for this
                    self.value = Some(max_value + 1);
                }
            }
            plan::LogicOperator::Or => {
                if let Some((value, score)) = self.map.pop_first() {
                    self.value = Some(value);
                    self.score = score;
                    return;
                }

                let mut map = BTreeMap::new();

                let mut all_none = true;
                for stream in self.streams.iter_mut() {
                    stream.next();
                    if let Some(value) = stream.value() {
                        if let Some(existing_score) = map.get_mut(&value) {
                            *existing_score += stream.score();
                        } else {
                            map.insert(value, stream.score());
                        }
                        all_none = false;
                    }
                }

                if all_none {
                    self.value = None;
                    return;
                }

                let (value, score) = map.pop_first().unwrap();
                self.value = Some(value);
                self.score = score;
                self.map = map;
            }
        };
    }

    fn value(&self) -> Option<u64> {
        self.value
    }

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
                        let value = stream.next_value(max(max_value, skip));

                        match value {
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
                if let Some((value, score)) = self.map.pop_first() {
                    if value >= skip {
                        self.value = Some(value);
                        self.score = score;
                        return Some(value);
                    }
                }

                let mut map = BTreeMap::new();

                let mut all_none = true;
                for stream in self.streams.iter_mut() {
                    stream.next();
                    if let Some(value) = stream.value() {
                        if let Some(existing_score) = map.get_mut(&value) {
                            *existing_score += stream.score();
                        } else {
                            map.insert(value, stream.score());
                        }
                        all_none = false;
                    }
                }

                if all_none {
                    self.value = None;
                    return None;
                }

                let (value, score) = map.pop_first().unwrap();
                self.value = Some(value);
                self.score = score;
                self.map = map;
                self.value
            }
        }
    }
}

pub struct BitmapStream {
    start: u64,
    iter: BitmapIterator<'static>,
    value: Option<u32>,
    score: f32,
    end: bool,
}

impl Debug for BitmapStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitmapStream")
            .field("start", &self.start)
            .field("value", &self.value)
            .field("score", &self.score)
            .field("end", &self.end)
            .finish()
    }
}

impl BitmapStream {
    pub fn new(start: u64, iter: BitmapIterator<'_>, score: f32) -> Self {
        let iter =
            unsafe { std::mem::transmute::<BitmapIterator<'_>, BitmapIterator<'static>>(iter) };
        Self {
            start,
            iter,
            value: None,
            score,
            end: false,
        }
    }
}

impl HitStream for BitmapStream {
    fn next(&mut self) {
        if self.end {
            return;
        }
        self.value = self.iter.next();
        if self.value.is_none() {
            self.end = true;
        }
    }

    fn value(&self) -> Option<u64> {
        Some(self.value? as u64 + self.start)
    }

    fn score(&self) -> f32 {
        self.score
    }

    fn next_value(&mut self, value: u64) -> Option<u64> {
        let value = (value - self.start) as u32;
        loop {
            let v = match self.value {
                Some(v) => v,
                None => self.iter.next()?,
            };
            if v >= value {
                return Some(v as u64 + self.start);
            }
            self.iter.next();
        }
    }
}

pub struct TextStream {
    reader: Arc<FulltextIndexReader>,
    boost: f32,
    tokens: Vec<Token>,
    iter: BitmapIterator<'static>,
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
        iter: BitmapIterator<'_>,
        token_doc_len: HashMap<String, usize>,
        operator: bool,
        slop: i32,
    ) -> Self {
        let iter =
            unsafe { std::mem::transmute::<BitmapIterator<'_>, BitmapIterator<'static>>(iter) };

        Self {
            reader,
            boost,
            tokens,
            iter,
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
    fn next(&mut self) {
        if self.end {
            return;
        }
        loop {
            self.value = self.iter.next();

            if self.value.is_none() {
                self.end = true;
                break;
            }

            if let Some(score) = self.score(self.value.unwrap()) {
                self.score = score;
                break;
            };
        }
    }

    fn value(&self) -> Option<u64> {
        self.value.map(|v| self.full_id(v))
    }

    fn score(&self) -> f32 {
        self.score * self.boost
    }

    fn next_value(&mut self, value: u64) -> Option<u64> {
        let value = (value - self.reader.start) as u32;
        loop {
            let id = match self.value {
                Some(v) => v,
                None => self.iter.next()?,
            };

            if id < value {
                self.value = None;
                continue;
            }

            match self.score(id) {
                Some(score) => {
                    self.value = Some(id);
                    self.score = score;
                    return Some(self.full_id(id));
                }
                None => self.value = None,
            }
        }
    }
}

impl TextStream {
    fn score(&self, id: u32) -> Option<f32> {
        self.reader.score(
            id,
            &self.tokens,
            &self.token_doc_len,
            self.operator,
            self.slop,
        )
    }

    fn full_id(&self, id: u32) -> u64 {
        id as u64 + self.reader.start
    }
}
