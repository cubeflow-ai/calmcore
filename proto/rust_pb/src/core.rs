// This file is @generated by prost-build.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Value {
    #[prost(oneof = "value::Kind", tags = "1, 2, 3, 4, 5, 6, 7")]
    pub kind: ::core::option::Option<value::Kind>,
}
/// Nested message and enum types in `Value`.
pub mod value {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Kind {
        #[prost(bool, tag = "1")]
        BoolValue(bool),
        #[prost(int64, tag = "2")]
        IntValue(i64),
        #[prost(float, tag = "3")]
        FloatValue(f32),
        #[prost(string, tag = "4")]
        StringValue(::prost::alloc::string::String),
        #[prost(message, tag = "5")]
        ListValue(super::ListValue),
        #[prost(message, tag = "6")]
        ObjectValue(super::ObjectValue),
        #[prost(message, tag = "7")]
        VectorValue(super::Embedding),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ObjectValue {
    #[prost(map = "string, message", tag = "1")]
    pub fields: ::std::collections::HashMap<::prost::alloc::string::String, Value>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListValue {
    #[prost(message, repeated, tag = "1")]
    pub values: ::prost::alloc::vec::Vec<Value>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Embedding {
    #[prost(float, repeated, tag = "1")]
    pub e: ::prost::alloc::vec::Vec<f32>,
}
/// links as an object, rather than a field type
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Link {
    #[prost(uint64, tag = "1")]
    pub source_space: u64,
    #[prost(uint64, tag = "2")]
    pub source_record: u64,
    #[prost(string, tag = "3")]
    pub name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "4")]
    pub target_space: u64,
    #[prost(uint64, tag = "5")]
    pub target_record: u64,
    #[prost(message, optional, tag = "6")]
    pub metadata: ::core::option::Option<ObjectValue>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Fields {
    #[prost(message, repeated, tag = "1")]
    pub fields: ::prost::alloc::vec::Vec<Field>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Schema {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(bool, tag = "2")]
    pub schemaless: bool,
    #[prost(map = "string, message", tag = "3")]
    pub fields: ::std::collections::HashMap<::prost::alloc::string::String, Field>,
    #[prost(message, optional, tag = "4")]
    pub metadata: ::core::option::Option<ObjectValue>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UserSchema {
    #[prost(map = "string, message", tag = "1")]
    pub fields: ::std::collections::HashMap<::prost::alloc::string::String, Field>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Vector {
    #[prost(string, tag = "1")]
    pub field_name: ::prost::alloc::string::String,
    #[prost(float, repeated, tag = "2")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Record {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub id: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "4")]
    pub vectors: ::prost::alloc::vec::Vec<Vector>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Field {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration = "field::Type", tag = "2")]
    pub r#type: i32,
    #[prost(oneof = "field::Option", tags = "3, 4")]
    pub option: ::core::option::Option<field::Option>,
}
/// Nested message and enum types in `Field`.
pub mod field {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct EmbeddingOption {
        #[prost(int32, tag = "1")]
        pub dimension: i32,
        #[prost(string, tag = "2")]
        pub embedding: ::prost::alloc::string::String,
        #[prost(enumeration = "embedding_option::Metric", tag = "3")]
        pub metric: i32,
        #[prost(int32, tag = "4")]
        pub batch_size: i32,
    }
    /// Nested message and enum types in `EmbeddingOption`.
    pub mod embedding_option {
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(
            Clone,
            Copy,
            Debug,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            ::prost::Enumeration
        )]
        #[repr(i32)]
        pub enum Metric {
            DotProduct = 0,
            Manhattan = 1,
            Euclidean = 2,
            CosineSimilarity = 3,
            Angular = 4,
        }
        impl Metric {
            /// String value of the enum field names used in the ProtoBuf definition.
            ///
            /// The values are not transformed in any way and thus are considered stable
            /// (if the ProtoBuf definition does not change) and safe for programmatic use.
            pub fn as_str_name(&self) -> &'static str {
                match self {
                    Self::DotProduct => "DotProduct",
                    Self::Manhattan => "Manhattan",
                    Self::Euclidean => "Euclidean",
                    Self::CosineSimilarity => "CosineSimilarity",
                    Self::Angular => "Angular",
                }
            }
            /// Creates an enum from field names used in the ProtoBuf definition.
            pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
                match value {
                    "DotProduct" => Some(Self::DotProduct),
                    "Manhattan" => Some(Self::Manhattan),
                    "Euclidean" => Some(Self::Euclidean),
                    "CosineSimilarity" => Some(Self::CosineSimilarity),
                    "Angular" => Some(Self::Angular),
                    _ => None,
                }
            }
        }
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct FulltextOption {
        #[prost(enumeration = "fulltext_option::Tokenizer", tag = "1")]
        pub tokenizer: i32,
        #[prost(enumeration = "fulltext_option::Filter", repeated, tag = "2")]
        pub filters: ::prost::alloc::vec::Vec<i32>,
        #[prost(message, optional, tag = "3")]
        pub stopwords: ::core::option::Option<super::Dict>,
        #[prost(message, optional, tag = "4")]
        pub synonyms: ::core::option::Option<super::Dict>,
    }
    /// Nested message and enum types in `FulltextOption`.
    pub mod fulltext_option {
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(
            Clone,
            Copy,
            Debug,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            ::prost::Enumeration
        )]
        #[repr(i32)]
        pub enum Tokenizer {
            Standard = 0,
            Whitespace = 1,
        }
        impl Tokenizer {
            /// String value of the enum field names used in the ProtoBuf definition.
            ///
            /// The values are not transformed in any way and thus are considered stable
            /// (if the ProtoBuf definition does not change) and safe for programmatic use.
            pub fn as_str_name(&self) -> &'static str {
                match self {
                    Self::Standard => "Standard",
                    Self::Whitespace => "Whitespace",
                }
            }
            /// Creates an enum from field names used in the ProtoBuf definition.
            pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
                match value {
                    "Standard" => Some(Self::Standard),
                    "Whitespace" => Some(Self::Whitespace),
                    _ => None,
                }
            }
        }
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(
            Clone,
            Copy,
            Debug,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            ::prost::Enumeration
        )]
        #[repr(i32)]
        pub enum Filter {
            Lowercase = 0,
            Stemmer = 1,
        }
        impl Filter {
            /// String value of the enum field names used in the ProtoBuf definition.
            ///
            /// The values are not transformed in any way and thus are considered stable
            /// (if the ProtoBuf definition does not change) and safe for programmatic use.
            pub fn as_str_name(&self) -> &'static str {
                match self {
                    Self::Lowercase => "Lowercase",
                    Self::Stemmer => "Stemmer",
                }
            }
            /// Creates an enum from field names used in the ProtoBuf definition.
            pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
                match value {
                    "Lowercase" => Some(Self::Lowercase),
                    "Stemmer" => Some(Self::Stemmer),
                    _ => None,
                }
            }
        }
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum Type {
        Bool = 0,
        Int = 1,
        Float = 2,
        String = 3,
        Text = 4,
        Geo = 5,
        Vector = 6,
    }
    impl Type {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Self::Bool => "Bool",
                Self::Int => "Int",
                Self::Float => "Float",
                Self::String => "String",
                Self::Text => "Text",
                Self::Geo => "Geo",
                Self::Vector => "Vector",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "Bool" => Some(Self::Bool),
                "Int" => Some(Self::Int),
                "Float" => Some(Self::Float),
                "String" => Some(Self::String),
                "Text" => Some(Self::Text),
                "Geo" => Some(Self::Geo),
                "Vector" => Some(Self::Vector),
                _ => None,
            }
        }
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Option {
        #[prost(message, tag = "3")]
        Embedding(EmbeddingOption),
        #[prost(message, tag = "4")]
        Fulltext(FulltextOption),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Dict {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration = "dict::Protocol", tag = "2")]
    pub protocol: i32,
    #[prost(string, tag = "3")]
    pub value: ::prost::alloc::string::String,
}
/// Nested message and enum types in `Dict`.
pub mod dict {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum Protocol {
        Json = 0,
        Api = 1,
        File = 2,
    }
    impl Protocol {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Self::Json => "Json",
                Self::Api => "Api",
                Self::File => "File",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "Json" => Some(Self::Json),
                "Api" => Some(Self::Api),
                "File" => Some(Self::File),
                _ => None,
            }
        }
    }
}
/// query protos
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Query {
    #[prost(string, tag = "1")]
    pub query: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub offset: u32,
    #[prost(uint32, tag = "3")]
    pub limit: u32,
    #[prost(string, repeated, tag = "5")]
    pub order_by: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "6")]
    pub group_by: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "7")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Hit {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(float, tag = "2")]
    pub score: f32,
    #[prost(message, optional, tag = "3")]
    pub record: ::core::option::Option<Record>,
    #[prost(bytes = "vec", repeated, tag = "4")]
    pub sort: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryResult {
    #[prost(message, repeated, tag = "1")]
    pub hits: ::prost::alloc::vec::Vec<Hit>,
    #[prost(uint64, tag = "2")]
    pub total_hits: u64,
}
