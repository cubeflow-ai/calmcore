use async_graphql::*;
use calmcore::util::{CoreError, CoreResult};
use proto::core::{
    field::{self, fulltext_option::Tokenizer, vector_option::Metric, FulltextOption, Type},
    Dict, Field,
};
use serde::Serialize;

#[derive(InputObject, Serialize)]
pub struct GqlField {
    pub name: String,
    pub field_type: GqlType,
    pub text_option: Option<GqlFulltextOption>,
    pub embedding_option: Option<GqlEmbeddingOption>,
}

impl TryInto<Field> for GqlField {
    type Error = CoreError;

    fn try_into(self) -> CoreResult<Field> {
        let mut field = Field {
            name: self.name,
            r#type: 0,
            option: None,
        };

        match self.field_type {
            GqlType::Bool => field.set_type(Type::Bool),
            GqlType::Int => field.set_type(Type::Int),
            GqlType::Float => field.set_type(Type::Float),
            GqlType::String => field.set_type(Type::String),
            GqlType::Text => {
                field.set_type(Type::Text);
                let o = self.text_option.unwrap_or_default();
                field.option = Some(field::Option::Fulltext(FulltextOption {
                    tokenizer: Tokenizer::Standard as i32,
                    filters: vec![],
                    stopwords: o.stopwords.map(|gd| gd.into()),
                    synonyms: o.synonyms.map(|gd| gd.into()),
                    no_store: false,
                }));
            }
            GqlType::Geo => field.set_type(Type::Geo),
            GqlType::Embedding => {
                field.set_type(Type::Vector);
                let o: GqlEmbeddingOption = self.embedding_option.unwrap_or_default();
                if o.dimension == 0 && o.index_params.is_empty() {
                    return Err(CoreError::InvalidParam(format!(
                        "field:{:?} dimension and embedding can't be empty",
                        field
                    )));
                }

                let GqlEmbeddingOption {
                    dimension,
                    metric,
                    index_params,
                } = o;

                let mut eo = field::VectorOption {
                    dimension,
                    metric: 0,
                    index_params,
                };

                match metric {
                    GqlMetric::DotProduct => eo.set_metric(Metric::DotProduct),
                    GqlMetric::Euclidean => eo.set_metric(Metric::Euclidean),
                    GqlMetric::Manhattan => eo.set_metric(Metric::Manhattan),
                    GqlMetric::CosineSimilarity => eo.set_metric(Metric::CosineSimilarity),
                    GqlMetric::Angular => eo.set_metric(Metric::Angular),
                }

                field.option = Some(field::Option::Vector(eo));
            }
        };

        Ok(field)
    }
}

#[derive(Default, Enum, Copy, Clone, Eq, PartialEq, Serialize)]
pub enum GqlMetric {
    #[default]
    DotProduct,
    Euclidean,
    Manhattan,
    CosineSimilarity,
    Angular,
}

#[derive(InputObject, Serialize, Default)]
pub struct GqlFulltextOption {
    pub tokenizer: Option<String>,
    pub lowercase: Option<bool>,
    pub stopwords: Option<GqlDict>,
    pub synonyms: Option<GqlDict>,
    pub keywrods: Option<GqlDict>,
}

#[derive(Default, Enum, Copy, Clone, Eq, PartialEq, Serialize)]
pub enum GqlProtocol {
    #[default]
    Json,
    Api,
    File,
}

#[derive(InputObject, Serialize, Default)]
pub struct GqlDict {
    pub name: String,
    pub protocol: GqlProtocol,
    pub value: String,
}

impl From<GqlDict> for Dict {
    fn from(d: GqlDict) -> Self {
        Dict {
            name: d.name,
            protocol: match d.protocol {
                GqlProtocol::Json => 1,
                GqlProtocol::Api => 2,
                GqlProtocol::File => 3,
            },
            value: d.value,
        }
    }
}

#[derive(InputObject, Serialize, Default)]
pub struct GqlEmbeddingOption {
    pub dimension: i32,
    pub metric: GqlMetric,
    pub index_params: String,
}

#[derive(InputObject, Serialize)]
pub struct GqlSchema {
    pub name: String,
    pub id: u64,
    pub schema_id: u64,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Serialize)]
pub enum GqlType {
    Bool,
    Int,
    Float,
    String,
    Text,
    Geo,
    Embedding,
}

pub mod result_wrapper {
    use std::collections::HashMap;

    use proto::calmserver::SearchResponse;
    use serde_json::json;

    #[derive(serde::Serialize, serde::Deserialize)]
    pub struct SearchResponseWrapper {
        pub status: Option<proto::calmserver::Status>,
        pub result: Option<QueryResultWrapper>,
        pub timeuse_mill: u32,
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    pub struct QueryResultWrapper {
        pub hits: ::prost::alloc::vec::Vec<HitWrapper>,
        pub total_hits: u64,
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    pub struct HitWrapper {
        pub id: u64,
        pub score: f32,
        pub value: Option<ObjectValueWrapper>,
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    pub struct ObjectValueWrapper {
        pub value: HashMap<String, serde_json::Value>,
    }

    impl ObjectValueWrapper {
        pub fn new(value: proto::core::ObjectValue) -> Self {
            let value = value
                .fields
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        match v.kind.unwrap() {
                            proto::core::value::Kind::BoolValue(b) => json!(b),
                            proto::core::value::Kind::IntValue(i) => json!(i),
                            proto::core::value::Kind::FloatValue(f) => json!(f),
                            proto::core::value::Kind::StringValue(s) => json!(s),
                            _ => unreachable!("unsupported value type"),
                        },
                    )
                })
                .collect();
            Self { value }
        }
    }

    impl HitWrapper {
        pub fn new(hit: proto::core::Hit) -> Self {
            Self {
                id: hit.id,
                score: hit.score,
                value: hit.value.map(ObjectValueWrapper::new),
            }
        }
    }

    impl QueryResultWrapper {
        pub fn new(result: proto::core::QueryResult) -> Self {
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
