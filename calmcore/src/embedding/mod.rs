#![allow(dead_code)]

use std::{borrow::Cow, sync::Arc};

use proto::core::Value;

use crate::util::{CoreError, CoreResult};

pub trait Embedding {
    fn embedding<'a>(&self, value: &'a Value) -> CoreResult<Option<Cow<'a, Vec<f32>>>>;
    fn dimension(&self) -> usize {
        0
    }
}

pub fn new_embedding(name: &str) -> CoreResult<Arc<dyn Embedding + Send + Sync + 'static>> {
    match name.to_lowercase().as_str() {
        "" | "no" => Ok(Arc::new(NoEmbedding {})),
        _ => Err(CoreError::InvalidParam(format!(
            "embedding {} not support",
            name
        ))),
    }
}

struct NoEmbedding;

impl Embedding for NoEmbedding {
    fn embedding<'a>(&self, value: &'a Value) -> CoreResult<Option<Cow<'a, Vec<f32>>>> {
        match &value.kind {
            Some(proto::core::value::Kind::VectorValue(v)) => Ok(Some(Cow::Borrowed(&v.e))),
            Some(proto::core::value::Kind::StringValue(v)) => {
                let e: Result<Vec<f32>, std::num::ParseFloatError> = v[1..v.len() - 1]
                    .split(',')
                    .map(|s| s.trim().parse())
                    .collect();
                Ok(Some(Cow::Owned(e?)))
            }
            None => Ok(None),
            _ => Err(CoreError::InvalidParam(format!(
                "value {:?} not support for NoEmbedding",
                value
            ))),
        }
    }
}
