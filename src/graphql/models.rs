use async_graphql::*;
use calmcore::util::{CoreError, CoreResult};
use proto::core::{
    field::{self, embedding_option::Metric, fulltext_option::Tokenizer, FulltextOption, Type},
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
                }));
            }
            GqlType::Geo => field.set_type(Type::Geo),
            GqlType::Embedding => {
                field.set_type(Type::Vector);
                let o: GqlEmbeddingOption = self.embedding_option.unwrap_or_default();
                if o.dimension == 0 && o.embedding.is_none() {
                    return Err(CoreError::InvalidParam(format!(
                        "field:{:?} dimension and embedding can't be empty",
                        field
                    )));
                }

                let GqlEmbeddingOption {
                    dimension,
                    metric,
                    embedding,
                    batch_size,
                } = o;

                let mut eo = field::EmbeddingOption {
                    dimension,
                    metric: 0,
                    embedding: embedding.unwrap_or("".to_string()),
                    batch_size: batch_size.unwrap_or(0),
                };

                match metric {
                    GqlMetric::DotProduct => eo.set_metric(Metric::DotProduct),
                    GqlMetric::Manhattan => eo.set_metric(Metric::Manhattan),
                    GqlMetric::Euclidean => eo.set_metric(Metric::Euclidean),
                    GqlMetric::CosineSimilarity => eo.set_metric(Metric::CosineSimilarity),
                    GqlMetric::Angular => eo.set_metric(Metric::Angular),
                }

                field.option = Some(field::Option::Embedding(eo));
            }
        };

        Ok(field)
    }
}

#[derive(Default, Enum, Copy, Clone, Eq, PartialEq, Serialize)]
pub enum GqlMetric {
    #[default]
    DotProduct,
    Manhattan,
    Euclidean,
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
    pub embedding: Option<String>,
    pub batch_size: Option<i32>,
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
        pub record: Option<RecordWrapper>,
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    pub struct RecordWrapper {
        pub name: String,
        pub data: serde_json::Value,
        pub vectors: Vec<proto::core::Vector>,
    }

    impl RecordWrapper {
        pub fn new(record: proto::core::Record) -> Self {
            let data = serde_json::from_slice(&record.data).unwrap_or(json!(&record.data));
            Self {
                name: record.name,
                data,
                vectors: record.vectors,
            }
        }
    }

    impl HitWrapper {
        pub fn new(hit: proto::core::Hit) -> Self {
            Self {
                id: hit.id,
                score: hit.score,
                record: hit.record.map(RecordWrapper::new),
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
