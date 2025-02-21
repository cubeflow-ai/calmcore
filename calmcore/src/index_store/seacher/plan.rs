use croaring::Bitmap;
use itertools::Itertools;

use std::{collections::HashMap, sync::Arc};

use proto::core::Field;

use crate::{
    analyzer::Token,
    index_store::{
        index_fulltext::reader::FulltextIndexReader,
        segment::SegmentReader,
        stream::{BitmapStream, CombHitStream, HitStream, TextStream},
    },
    util::CoreResult,
};

use super::context::SegmentContext;

#[derive(Debug, Clone)]
pub enum LogicOperator {
    And,
    Or,
}

#[derive(Debug, Clone)]
pub enum ComparisonOperator {
    Eq,
    NotEq,
}

#[derive(Debug, Clone)]
pub enum Query {
    Term {
        value: Vec<u8>,
        boost: f32,
        operator: ComparisonOperator,
        field: Arc<Field>,
    },
    Between {
        low: Option<Vec<u8>>,
        low_eq: bool,
        high: Option<Vec<u8>>,
        high_eq: bool,
        boost: f32,
        field: Arc<Field>,
    },
    InList {
        list: Vec<Vec<u8>>,
        boost: f32,
        field: Arc<Field>,
    },
    Phrase {
        value: String,
        slop: i32,
        boost: f32,
        field: Arc<Field>,
    },
    Text {
        value: String,
        boost: f32,
        operator: String, // default is "or"
        field: Arc<Field>,
    },
    Logical {
        left: Box<Query>,
        right: Box<Query>,
        operator: LogicOperator,
    },
    Search {
        // from: String,
        projection: Vec<String>,
        query: Option<Box<Query>>,
        order_by: Vec<(String, bool)>,
        limit: (usize, usize),
    },
}
impl Query {
    pub(crate) fn set_boost(&mut self, boost: f32) {
        match self {
            Query::Term { boost: b, .. } => *b = boost,
            Query::Between { boost: b, .. } => *b = boost,
            Query::InList { boost: b, .. } => *b = boost,
            Query::Phrase { boost: b, .. } => *b = boost,
            Query::Text { boost: b, .. } => *b = boost,
            Query::Logical { .. } | Query::Search { .. } => unreachable!(),
        }
    }
}

pub enum PhysicsPlan {
    Map(f32, u32),
    // boost, terms, total_bitmap, token_bitmap, operator(true is and/ false is or), phrase_len(zero means no phrase)
    Text(
        Arc<FulltextIndexReader>,
        f32,
        Vec<Token>,
        u32,
        HashMap<String, usize>,
        bool,
        i32,
    ),
    Combin(Vec<PhysicsPlan>, LogicOperator),
}

impl PhysicsPlan {
    pub(crate) fn new(
        segment: &SegmentReader,
        query: &Query,
        sc: &mut SegmentContext,
    ) -> CoreResult<Self> {
        match query {
            Query::Term {
                value,
                boost,
                operator,
                field,
            } => match operator {
                ComparisonOperator::Eq => {
                    let key = format!("{}/{}={:?}", segment.start(), &field.name, value);
                    if let Some(id) = sc.get(&key) {
                        return Ok(PhysicsPlan::Map(*boost, id));
                    }

                    let rb = segment.term(field, value)?;

                    Ok(PhysicsPlan::Map(*boost, sc.insert(key, rb)))
                }
                ComparisonOperator::NotEq => {
                    let key = format!("{}{}!{:?}", segment.start(), &field.name, value);
                    if let Some(id) = sc.get(&key) {
                        return Ok(PhysicsPlan::Map(*boost, id));
                    }
                    let rb = segment.all_record() - segment.term(field, value)?;
                    Ok(PhysicsPlan::Map(*boost, sc.insert(key, rb)))
                }
            },
            Query::Between {
                low,
                low_eq,
                high,
                high_eq,
                boost,
                field,
            } => {
                let key = format!(
                    "{}/{}bt{:?}{}{:?}{}",
                    segment.start(),
                    &field.name,
                    low,
                    low_eq,
                    high,
                    high_eq,
                );
                if let Some(id) = sc.get(&key) {
                    return Ok(PhysicsPlan::Map(*boost, id));
                }
                let rb = segment.between(field, low.as_ref(), *low_eq, high.as_ref(), *high_eq)?;
                Ok(PhysicsPlan::Map(*boost, sc.insert(key, rb)))
            }
            Query::InList { list, boost, field } => {
                let key = format!("{}/{}in{:?}", segment.start(), &field.name, list,);
                if let Some(id) = sc.get(&key) {
                    return Ok(PhysicsPlan::Map(*boost, id));
                }
                let rb = segment.in_terms(field, list)?;
                Ok(PhysicsPlan::Map(*boost, sc.insert(key, rb)))
            }
            Query::Phrase {
                value,
                slop,
                boost,
                field,
            } => {
                let reader = segment.get_text_reader(field)?;

                let tokens = reader.analyzer(value)?;

                let mut total = None;

                let mut out_tokens = Vec::new();
                let mut out_keys = Vec::new();

                let mut token_doc_len = HashMap::new();

                for t in tokens.iter().map(|t| &t.name).unique() {
                    let key = format!("{}/{}={:?}", segment.start(), &field.name, t);
                    match sc.get(&key) {
                        Some(id) => {
                            let term_bitmap = sc.value_ref_get(id);
                            token_doc_len.insert(t.clone(), term_bitmap.cardinality() as usize);
                            total = total
                                .map(|total| total & term_bitmap)
                                .or_else(|| Some(term_bitmap.clone()));
                        }
                        None => {
                            out_keys.push(key);
                            out_tokens.push(t);
                        }
                    }
                }

                for ((b, key), t) in reader
                    .tokens(&out_tokens)?
                    .into_iter()
                    .zip(out_keys)
                    .zip(out_tokens)
                {
                    token_doc_len.insert(t.clone(), b.cardinality() as usize);
                    total = total.map(|total| total & &b).or_else(|| Some(b.clone()));
                    sc.insert(key, b);
                }

                let total_map = sc.value_insert(total.unwrap_or_default());

                Ok(Self::Text(
                    reader,
                    *boost,
                    tokens,
                    total_map,
                    token_doc_len,
                    true,
                    *slop,
                ))
            }
            Query::Text {
                value,
                operator,
                boost,
                field,
            } => {
                let operator = "and".eq_ignore_ascii_case(operator);

                let reader = segment.get_text_reader(field)?;

                let tokens = reader.analyzer(value)?;

                let mut total = None;

                let mut out_tokens = Vec::new();
                let mut out_keys = Vec::new();

                let mut token_doc_len = HashMap::new();

                for t in tokens.iter().map(|t| &t.name).unique() {
                    let key = format!("{}/{}={:?}", segment.start(), &field.name, t);
                    match sc.get(&key) {
                        Some(id) => {
                            let b = sc.value_ref_get(id);
                            total = total
                                .map(|total| if operator { total & b } else { total | b })
                                .or_else(|| Some(b.clone()));
                            token_doc_len.insert(t.clone(), b.cardinality() as usize);
                        }
                        None => {
                            out_keys.push(key);
                            out_tokens.push(t);
                        }
                    }
                }

                for ((b, key), t) in reader
                    .tokens(&out_tokens)?
                    .into_iter()
                    .zip(out_keys)
                    .zip(out_tokens)
                {
                    token_doc_len.insert(t.clone(), b.cardinality() as usize);

                    total = total
                        .map(|total| if operator { total & &b } else { total | &b })
                        .or_else(|| Some(b.clone()));

                    sc.insert(key, b);
                }

                let total_map = sc.value_insert(total.unwrap_or_default());

                Ok(Self::Text(
                    reader,
                    *boost,
                    tokens,
                    total_map,
                    token_doc_len,
                    true,
                    -1,
                ))
            }
            Query::Logical {
                left,
                right,
                operator,
            } => {
                let l = Self::new(segment, left, sc)?;
                let r = Self::new(segment, right, sc)?;
                match operator {
                    LogicOperator::And => Ok(l.and(r, sc)),
                    LogicOperator::Or => Ok(l.or(r)),
                }
            }
            Query::Search { .. } => unreachable!(),
        }
    }

    pub fn into_stream(
        self,
        start: u64,
        sc: &mut SegmentContext,
        filter: &Bitmap,
    ) -> Box<dyn HitStream> {
        match self {
            PhysicsPlan::Map(boost, key) => Box::new(BitmapStream::new(
                start,
                sc.and_value(key, filter).iter(),
                boost,
            )),
            PhysicsPlan::Combin(vec, logic_operator) => {
                let mut cs = CombHitStream::new(vec.len(), logic_operator);
                for v in vec.into_iter() {
                    cs.add(v.into_stream(start, sc, filter));
                }
                Box::new(cs)
            }
            PhysicsPlan::Text(
                reader,
                boost,
                tokens,
                total_bitmap,
                token_doc_len,
                operator,
                slop,
            ) => Box::new(TextStream::new(
                reader,
                boost,
                tokens,
                sc.and_value(total_bitmap, filter).iter(),
                token_doc_len,
                operator,
                slop,
            )),
        }
    }

    pub fn as_filter(&self, sc: &SegmentContext) -> Bitmap {
        match self {
            PhysicsPlan::Map(_, key) => sc.value_get(*key),
            PhysicsPlan::Combin(vec, logic_operator) => vec
                .iter()
                .fold(None, |acc, item| {
                    let r = item.as_filter(sc);
                    match (acc, logic_operator) {
                        (Some(acc), LogicOperator::And) => Some(acc & r),
                        (Some(acc), LogicOperator::Or) => Some(acc | r),
                        (None, _) => Some(r),
                    }
                })
                .unwrap_or_default(),
            PhysicsPlan::Text(_, _, _, total_bitmap, _, _, _) => sc.value_get(*total_bitmap),
        }
    }

    fn can_merge(&self) -> bool {
        match self {
            PhysicsPlan::Map(..) => true,
            PhysicsPlan::Text(..) => false,
            PhysicsPlan::Combin(..) => true,
        }
    }
}

impl PhysicsPlan {
    fn and(self, other: PhysicsPlan, sc: &mut SegmentContext) -> Self {
        if !self.can_merge() || !other.can_merge() {
            return PhysicsPlan::Combin(vec![self, other], LogicOperator::And);
        }

        match (self, other) {
            (PhysicsPlan::Map(s1, k1), PhysicsPlan::Map(s2, k2)) => {
                let k = sc.value_insert(sc.value_ref_get(k1) & sc.value_ref_get(k2));
                PhysicsPlan::Map(s1 + s2, k)
            }
            (PhysicsPlan::Map(s, k), PhysicsPlan::Combin(vec, op)) => {
                PhysicsPlan::Combin(Self::and_map(vec, s, k, sc), op)
            }
            (PhysicsPlan::Combin(vec, op), PhysicsPlan::Map(s, k)) => {
                PhysicsPlan::Combin(Self::and_map(vec, s, k, sc), op)
            }
            (PhysicsPlan::Combin(v1, op1), PhysicsPlan::Combin(v2, op2)) => {
                if v1.is_empty() || v2.is_empty() {
                    return PhysicsPlan::Combin(Vec::new(), LogicOperator::And);
                }
                PhysicsPlan::Combin(
                    vec![PhysicsPlan::Combin(v1, op1), PhysicsPlan::Combin(v2, op2)],
                    LogicOperator::And,
                )
            }
            _ => unreachable!(),
        }
    }

    fn or(self, other: PhysicsPlan) -> PhysicsPlan {
        PhysicsPlan::Combin(vec![self, other], LogicOperator::Or)
    }

    fn and_map(vec: Vec<PhysicsPlan>, s: f32, k: u32, sc: &mut SegmentContext) -> Vec<PhysicsPlan> {
        let rb = sc.value_ref_get(k).clone();
        let mut items = Vec::new();
        for v in vec.into_iter() {
            match v {
                PhysicsPlan::Map(s2, k2) => {
                    let rb = sc.value_ref_get(k2) & &rb;
                    items.push(PhysicsPlan::Map(s + s2, sc.value_insert(rb)));
                }
                PhysicsPlan::Combin(vec, op) => {
                    items.push(PhysicsPlan::Combin(PhysicsPlan::and_map(vec, s, k, sc), op));
                }
                _ => unreachable!(),
            }
        }
        items
    }
}
