use std::sync::Arc;

use croaring::Bitmap;
use itertools::Itertools;
use mem_btree::persist::KVSerializer;

use crate::{
    index_store::store::{InvertIndexReader, IterKey},
    util::CoreResult,
};

pub struct TermIndexReader {
    pub start: u64,
    pub(crate) term_record_index: InvertIndexReader<Vec<u8>, Bitmap>,
    pub inner: Arc<proto::core::Field>,
    pub term_serializer: Box<dyn KVSerializer<Vec<u8>, Bitmap>>,
}

impl TermIndexReader {
    pub fn term(&self, v: &Vec<u8>) -> Option<Bitmap> {
        self.term_record_index.get(v)
    }

    pub fn field(&self) -> &Arc<proto::core::Field> {
        &self.inner
    }

    pub fn range<F>(&self, start: Option<&Vec<u8>>, f: F)
    where
        F: FnMut(IterKey<Vec<u8>>, &Bitmap) -> bool,
    {
        self.term_record_index.range(start, f);
    }

    pub fn in_terms(&self, list: &[Vec<u8>]) -> Bitmap {
        let mut result = Bitmap::new();
        for v in list {
            if let Some(bi) = self.term(v) {
                result |= bi;
            }
        }
        result
    }

    pub fn between(
        &self,
        low: Option<&Vec<u8>>,
        low_eq: bool,
        high: Option<&Vec<u8>>,
        high_eq: bool,
    ) -> CoreResult<Bitmap> {
        let mut results = Vec::new();

        self.term_record_index.range(low, |k, v| {
            if !low_eq {
                if let Some(low_value) = low {
                    if k.cmp_key(low_value, self.term_serializer.as_ref()).is_eq() {
                        return true;
                    }
                }
            }

            if let Some(high) = high {
                let order = k.cmp_key(high, self.term_serializer.as_ref());

                if high_eq {
                    if order.is_gt() {
                        return false;
                    }
                } else if order.is_gt() || order.is_eq() {
                    return false;
                }
            }
            results.push(v.clone());
            true
        });

        Ok(Bitmap::fast_or(&results.iter().collect_vec()))
    }
}
