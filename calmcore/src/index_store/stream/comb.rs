use std::{cmp::max, collections::BTreeMap};

use itertools::Itertools;

use crate::index_store::seacher::plan;

use super::{HitStream, HitStreamIter};

pub struct CombHitStream {
    streams: Vec<Box<dyn HitStream>>,
    operator: plan::LogicOperator,
    map: BTreeMap<u64, f32>,
}

impl CombHitStream {
    pub fn new(capacity: usize, operator: plan::LogicOperator) -> Self {
        Self {
            streams: Vec::with_capacity(capacity),
            operator,
            map: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, stream: Box<dyn HitStream>) {
        self.streams.push(stream);
    }
}

impl HitStream for CombHitStream {
    fn iter<'a>(&'a mut self) -> Box<dyn super::HitStreamIter<'a>> {
        let streams = self.streams.iter_mut().map(|s| s.iter()).collect_vec();
        Box::new(CombHitStreamIter {
            operator: self.operator.clone(),
            value: None,
            score: 0.0,
            streams,
        })
    }
}

struct CombHitStreamIter<'a> {
    operator: plan::LogicOperator,
    value: Option<u64>,
    score: f32,
    streams: Vec<Box<dyn HitStreamIter<'a>>>,
}

impl<'a> HitStreamIter<'a> for CombHitStreamIter<'a> {
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
        let value = match self.value {
            Some(v) => v,
            None => return None,
        };

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
                return Some(value);
            }
        }
    }
}
