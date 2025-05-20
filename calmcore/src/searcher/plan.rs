use croaring::Bitmap;
use itertools::Itertools;

use std::{collections::HashMap, sync::Arc};

use proto::core::{Field, Hit};

use crate::{
    analyzer::Token,
    index_store::{
        index_fulltext::reader::{FulltextIndexReader, PositionList},
        segment::SegmentReader,
        store::VectorIndexReader,
        stream::{BitmapStream, CombHitStream, HitStream, PhraseStream, TextStream, VectorStream},
    },
    util::{merge_bitmap, CoreResult},
};

use super::context::SegmentContext;

#[derive(Debug, Clone, PartialEq)]
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
    Vector {
        value: Vec<f32>,
        boost: f32,
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
            Query::Vector { boost: b, .. } => *b = boost,
            Query::Between { boost: b, .. } => *b = boost,
            Query::InList { boost: b, .. } => *b = boost,
            Query::Phrase { boost: b, .. } => *b = boost,
            Query::Text { boost: b, .. } => *b = boost,
            Query::Logical { .. } | Query::Search { .. } => unreachable!(),
        }
    }

    pub(crate) fn need_realcount(&self) -> bool {
        match self {
            Query::Phrase { .. } => true,
            Query::Logical {
                left,
                right,
                operator: _,
            } => left.need_realcount() || right.need_realcount(),

            _ => return false,
        }
    }
}

pub enum PhysicsPlan {
    Map(f32, Bitmap),
    Vector(Arc<VectorIndexReader>, f32, Vec<f32>, Bitmap),
    // reader ,boost, terms, total_bitmap, token_bitmap, operator(true is and/ false is or), phrase_len(zero means no phrase)
    Text(
        Arc<FulltextIndexReader>,
        f32,                                   //boost
        HashMap<String, Option<PositionList>>, // term_position
        bool,                                  // operator(true is and/ false is or)
    ),
    Phrase(f32, u64, Vec<Hit>),
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
                ComparisonOperator::Eq => Ok(PhysicsPlan::Map(*boost, segment.term(field, value)?)),
                ComparisonOperator::NotEq => Ok(PhysicsPlan::Map(
                    *boost,
                    segment.all_record() - segment.term(field, value)?,
                )),
            },
            Query::Vector {
                value,
                boost,
                field,
            } => {
                let reader = segment.get_vector_reader(field)?;
                let value = value.clone();
                Ok(PhysicsPlan::Vector(
                    reader,
                    *boost,
                    value,
                    segment.all_record(),
                ))
            }
            Query::Between {
                low,
                low_eq,
                high,
                high_eq,
                boost,
                field,
            } => Ok(PhysicsPlan::Map(
                *boost,
                segment.between(field, low.as_ref(), *low_eq, high.as_ref(), *high_eq)?,
            )),
            Query::InList { list, boost, field } => {
                Ok(PhysicsPlan::Map(*boost, segment.in_terms(field, list)?))
            }
            Query::Phrase {
                value,
                slop,
                boost,
                field,
            } => {
                let reader = segment.get_text_reader(field)?;
                let tokens = reader.analyzer(value)?;
                Ok(Self::Phrase(
                    *boost,
                    reader.start,
                    reader.phrase_tokens(&tokens, *slop)?,
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

                let term_position = reader.tokens(&tokens)?;

                Ok(Self::Text(reader, *boost, term_position, operator))
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
    ) -> CoreResult<Box<dyn HitStream>> {
        let result: Box<dyn HitStream> = match self {
            PhysicsPlan::Map(boost, bitmap) => Box::new(BitmapStream::new(start, bitmap, boost)),
            PhysicsPlan::Combin(vec, logic_operator) => {
                let mut cs = CombHitStream::new(vec.len(), logic_operator);
                for v in vec.into_iter() {
                    cs.add(v.into_stream(start, sc, filter)?);
                }
                Box::new(cs)
            }
            PhysicsPlan::Text(reader, boost, term_count_map, operator) => {
                Box::new(TextStream::new(
                    reader,
                    boost,
                    tokens,
                    total_bitmap,
                    term_count_map,
                    operator,
                    slop,
                ))
            }
            PhysicsPlan::Vector(vector_index_reader, boost, value, _) => {
                let result = vector_index_reader.search(&value, 2000, &filter)?;
                Box::new(VectorStream::new(boost, result))
            }
            PhysicsPlan::Phrase(boost, hits) => Box::new(PhraseStream::new(boost, hits)),
        };

        Ok(result)
    }

    pub fn as_filter(&self, sc: &SegmentContext) -> Bitmap {
        match self {
            PhysicsPlan::Map(_, bm) => bm.clone(),
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
            PhysicsPlan::Text(_, _, _, total_bitmap, _, _, _) => total_bitmap.clone(),
            PhysicsPlan::Vector(_, _, _, total_bitmap) => total_bitmap.clone(),
            PhysicsPlan::Phrase(_, start, hits) => {
                Bitmap::from_iter(hits.iter().map(|h| (h.id - start) as u32))
            }
        }
    }

    fn can_merge(&self) -> bool {
        match self {
            PhysicsPlan::Map(..) => true,
            PhysicsPlan::Text(..) => false,
            PhysicsPlan::Combin(..) => true,
            PhysicsPlan::Vector(..) => false,
            PhysicsPlan::Phrase(..) => false,
        }
    }
}

impl PhysicsPlan {
    fn and(self, other: PhysicsPlan, sc: &mut SegmentContext) -> Self {
        if !self.can_merge() || !other.can_merge() {
            return PhysicsPlan::Combin(vec![self, other], LogicOperator::And);
        }

        match (self, other) {
            (PhysicsPlan::Map(s1, mut k1), PhysicsPlan::Map(s2, k2)) => {
                k1.and_inplace(&k2);
                PhysicsPlan::Map(s1 + s2, k1)
            }
            (PhysicsPlan::Map(s, k), PhysicsPlan::Combin(vec, op)) => {
                PhysicsPlan::Combin(Self::and_map(vec, s, &k, sc), op)
            }
            (PhysicsPlan::Combin(vec, op), PhysicsPlan::Map(s, k)) => {
                PhysicsPlan::Combin(Self::and_map(vec, s, &k, sc), op)
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

    fn and_map(
        vec: Vec<PhysicsPlan>,
        s: f32,
        k1: &Bitmap,
        sc: &mut SegmentContext,
    ) -> Vec<PhysicsPlan> {
        let mut items = Vec::new();
        for v in vec.into_iter() {
            match v {
                PhysicsPlan::Map(s2, k2) => {
                    items.push(PhysicsPlan::Map(s + s2, k2 & k1));
                }
                PhysicsPlan::Combin(vec, op) => {
                    items.push(PhysicsPlan::Combin(
                        PhysicsPlan::and_map(vec, s, k1, sc),
                        op,
                    ));
                }
                _ => unreachable!(),
            }
        }
        items
    }
}
