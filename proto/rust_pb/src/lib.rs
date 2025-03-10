use core::{value::Kind, ObjectValue, Value};
use std::cmp::Ordering;

pub mod calmserver;
pub mod core;

impl Value {
    pub fn obj(&self) -> &ObjectValue {
        match &self.kind {
            Some(Kind::ObjectValue(obj)) => obj,
            _ => unreachable!("value:{:?} must cast to obj", self),
        }
    }

    pub fn to_obj(self) -> ObjectValue {
        match self.kind {
            Some(Kind::ObjectValue(obj)) => obj,
            _ => unreachable!("value:{:?} must cast to obj", self),
        }
    }
}

impl Eq for core::Hit {}

impl PartialOrd for core::Hit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for core::Hit {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.score.partial_cmp(&other.score) {
            Some(v) => v,
            None => {
                if self.score.is_nan() && !other.score.is_nan() {
                    Ordering::Less
                } else if !self.score.is_nan() && other.score.is_nan() {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
        }
    }
}

impl core::QueryResult {
    pub fn to_wrapper(self) -> result_wrapper::QueryResultWrapper {
        result_wrapper::QueryResultWrapper::new(self)
    }
}

pub mod result_wrapper {
    use std::collections::HashMap;
    use std::fmt::Debug;

    use crate::calmserver::*;
    use crate::core::value::Kind;
    use crate::core::*;

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct SearchResponseWrapper {
        pub status: Option<Status>,
        pub result: Option<QueryResultWrapper>,
        pub timeuse_mill: u32,
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    pub struct QueryResultWrapper {
        pub hits: ::prost::alloc::vec::Vec<HitWrapper>,
        pub total_hits: u64,
    }

    impl Debug for QueryResultWrapper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            serde_json::to_string(self)
                .map_err(|_| std::fmt::Error)
                .and_then(|s| write!(f, "{}", s))
        }
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    pub struct ObjectValueWrapper {
        pub value: HashMap<String, serde_json::Value>,
    }

    impl ObjectValueWrapper {
        pub fn new(value: ObjectValue) -> Self {
            let value = value
                .fields
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        match v.kind.unwrap() {
                            Kind::BoolValue(b) => serde_json::json!(b),
                            Kind::IntValue(i) => serde_json::json!(i),
                            Kind::FloatValue(f) => serde_json::json!(f),
                            Kind::StringValue(s) => serde_json::json!(s),
                            _ => unreachable!("unsupported value type"),
                        },
                    )
                })
                .collect();
            Self { value }
        }
    }

    impl Debug for ObjectValueWrapper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            serde_json::to_string(self)
                .map_err(|_| std::fmt::Error)
                .and_then(|s| write!(f, "{}", s))
        }
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    pub struct HitWrapper {
        pub id: u64,
        pub score: f32,
        pub value: Option<ObjectValueWrapper>,
    }

    impl Debug for HitWrapper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            serde_json::to_string(self)
                .map_err(|_| std::fmt::Error)
                .and_then(|s| write!(f, "{}", s))
        }
    }

    impl HitWrapper {
        pub fn new(hit: Hit) -> Self {
            Self {
                id: hit.id,
                score: hit.score,
                value: hit.value.map(ObjectValueWrapper::new),
            }
        }
    }

    impl QueryResultWrapper {
        pub fn new(result: QueryResult) -> Self {
            Self {
                hits: result.hits.into_iter().map(HitWrapper::new).collect(),
                total_hits: result.total_hits,
            }
        }
    }

    impl SearchResponseWrapper {
        pub fn new(rep: SearchResponse) -> Self {
            let SearchResponse {
                status,
                result,
                timeuse_mill,
            } = rep;
            SearchResponseWrapper {
                status,
                result: result.map(QueryResultWrapper::new),
                timeuse_mill,
            }
        }
    }
}
