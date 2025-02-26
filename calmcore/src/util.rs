use std::{borrow::Cow, collections::HashMap, sync::RwLock};

use proto::core::{field, value::Kind, ListValue, ObjectValue, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::Scope;

pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum CoreError {
    #[error("ok")]
    Ok(u64),

    #[error("{0}")]
    Internal(String),

    #[error("{0} duplicated.")]
    Duplicated(String),

    #[error("{0} not existed.")]
    NotExisted(String),

    #[error("source store has err:'{0}'")]
    IOError(String),

    #[error("decode error:'{0}' data:{1:?}")]
    DecodeError(String, Vec<u8>),

    #[error("no support:{0}")]
    Notsupport(String),

    #[error("invalid param err:{0}")]
    InvalidParam(String),

    #[error("{0} existed.")]
    Existed(String),
}

impl CoreError {
    pub fn code(&self) -> i32 {
        match self {
            CoreError::Ok(_) => 0,
            CoreError::Internal(_) => 1,
            CoreError::Duplicated(_) => 2,
            CoreError::NotExisted(_) => 3,
            CoreError::IOError(_) => 4,
            CoreError::DecodeError(_, _) => 5,
            CoreError::Notsupport(_) => 6,
            CoreError::InvalidParam(_) => 7,
            CoreError::Existed(_) => 8,
        }
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, CoreError::Ok(_))
    }
}

impl From<prost::DecodeError> for CoreError {
    fn from(e: prost::DecodeError) -> Self {
        CoreError::DecodeError(e.to_string(), vec![])
    }
}

impl From<serde_json::Error> for CoreError {
    fn from(e: serde_json::Error) -> Self {
        CoreError::DecodeError(e.to_string(), vec![])
    }
}

impl From<std::str::ParseBoolError> for CoreError {
    fn from(e: std::str::ParseBoolError) -> Self {
        CoreError::InvalidParam(e.to_string())
    }
}

impl From<std::num::ParseIntError> for CoreError {
    fn from(e: std::num::ParseIntError) -> Self {
        CoreError::InvalidParam(e.to_string())
    }
}

impl From<std::convert::Infallible> for CoreError {
    fn from(e: std::convert::Infallible) -> Self {
        CoreError::InvalidParam(e.to_string())
    }
}

impl From<sqlparser::parser::ParserError> for CoreError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        CoreError::InvalidParam(e.to_string())
    }
}

impl From<std::num::ParseFloatError> for CoreError {
    fn from(e: std::num::ParseFloatError) -> Self {
        CoreError::InvalidParam(e.to_string())
    }
}

impl From<std::io::Error> for CoreError {
    fn from(e: std::io::Error) -> Self {
        CoreError::IOError(e.to_string())
    }
}

impl From<bincode::Error> for CoreError {
    fn from(e: bincode::Error) -> Self {
        CoreError::DecodeError(e.to_string(), vec![])
    }
}

pub fn value_to_json(value: Value) -> serde_json::Value {
    if value.kind.is_none() {
        return serde_json::Value::Null;
    }

    match value.kind.unwrap() {
        Kind::BoolValue(v) => serde_json::Value::Bool(v),
        Kind::IntValue(v) => serde_json::Value::Number(serde_json::Number::from(v)),
        Kind::FloatValue(v) => {
            serde_json::Value::Number(serde_json::Number::from_f64(v as f64).unwrap())
        }
        Kind::StringValue(v) => serde_json::Value::String(v),
        Kind::ListValue(v) => {
            serde_json::Value::Array(v.values.into_iter().map(value_to_json).collect())
        }
        Kind::ObjectValue(v) => serde_json::Value::Object(
            v.fields
                .into_iter()
                .map(|(k, v)| (k, value_to_json(v)))
                .collect(),
        ),
        Kind::VectorValue(v) => serde_json::Value::Array(
            v.e.iter()
                .map(|v| {
                    serde_json::Value::Number(serde_json::Number::from_f64(*v as f64).unwrap())
                })
                .collect(),
        ),
    }
}

// make json value to proto value

pub fn json_data_to_value(scope: &Scope, data: &[u8]) -> CoreResult<Value> {
    let json = serde_json::from_slice(data).map_err(|e| {
        CoreError::InvalidParam(format!("data:{:?} is not json, error:{:?}", data, e))
    })?;
    json_to_value(scope, json)
}

pub fn json_value_to_string(value: &serde_json::Value) -> Cow<'_, str> {
    match value {
        serde_json::Value::Null => Cow::Borrowed(""),
        serde_json::Value::Bool(b) => {
            if *b {
                Cow::Borrowed("true")
            } else {
                Cow::Borrowed("false")
            }
        }
        serde_json::Value::Number(n) => Cow::Owned(n.to_string()),
        serde_json::Value::String(s) => Cow::Borrowed(s),
        serde_json::Value::Array(arr) => Cow::Owned(
            arr.iter()
                .map(|v| json_value_to_string(v))
                .collect::<Vec<_>>()
                .join(","),
        ),
        serde_json::Value::Object(obj) => {
            Cow::Owned(serde_json::to_string(obj).unwrap_or_default())
        }
    }
}

pub fn json_to_value_none_schema(json: serde_json::Value) -> CoreResult<Value> {
    json_to_value(
        &Scope {
            schema: proto::core::Schema::default(),
            user_fields: RwLock::new(HashMap::new()),
        },
        json,
    )
}

pub fn json_to_value(scope: &Scope, json: serde_json::Value) -> CoreResult<Value> {
    let value = match json {
        serde_json::Value::Bool(b) => Value {
            kind: Some(Kind::BoolValue(b)),
        },
        serde_json::Value::Number(v) => v
            .as_i64()
            .map(|v| Value {
                kind: Some(Kind::IntValue(v)),
            })
            .or_else(|| {
                v.as_f64().map(|v| Value {
                    kind: Some(Kind::FloatValue(v as f32)),
                })
            })
            .ok_or(CoreError::InvalidParam(format!(
                "invalid number value:{:?}",
                v
            )))?,
        serde_json::Value::String(v) => Value {
            kind: Some(Kind::StringValue(v)),
        },
        serde_json::Value::Array(arr) => Value {
            kind: Some(Kind::ListValue(ListValue {
                values: arr
                    .into_iter()
                    .map(|v| json_to_value(scope, v))
                    .collect::<CoreResult<Vec<Value>>>()?,
            })),
        },
        serde_json::Value::Object(o) => {
            let mut obj = HashMap::new();
            for (k, v) in o {
                if let Some(f) = scope.get_field(&k) {
                    if field::Type::Vector == f.r#type() && v.is_array() {
                        obj.insert(
                            k,
                            Value {
                                kind: Some(Kind::VectorValue(proto::core::Embedding {
                                    e: v.as_array()
                                        .ok_or(CoreError::InvalidParam(format!(
                                            "field:{:?} value is not array",
                                            v
                                        )))?
                                        .iter()
                                        .map(|v| v.as_f64().unwrap() as f32)
                                        .collect::<Vec<f32>>(),
                                })),
                            },
                        );
                    } else {
                        obj.insert(k, json_to_value(scope, v)?);
                    }
                } else {
                    obj.insert(k, json_to_value(scope, v)?);
                }
            }

            Value {
                kind: Some(Kind::ObjectValue(ObjectValue { fields: obj })),
            }
        }
        serde_json::Value::Null => {
            return Err(CoreError::Notsupport(format!("json value:{:?}", json)))
        }
    };

    Ok(value)
}

pub enum KindType<T> {
    Single(T),
    Array(Vec<T>),
}

pub fn kind_to_string(kind: &Kind) -> Result<KindType<String>, CoreError> {
    let val = match kind {
        Kind::BoolValue(v) => v.to_string(),
        Kind::IntValue(v) => v.to_string(),
        Kind::FloatValue(v) => v.to_string(),
        Kind::StringValue(v) => v.clone(),
        Kind::ListValue(v) => {
            let mut vec = Vec::with_capacity(v.values.len());
            for v in &v.values {
                if let Some(v) = &v.kind {
                    match kind_to_string(v)? {
                        KindType::Single(v) => vec.push(v),
                        KindType::Array(v) => vec.extend(v),
                    }
                }
            }
            return Ok(KindType::Array(vec));
        }
        Kind::VectorValue(_) | Kind::ObjectValue(_) => {
            return Err(CoreError::InvalidParam(format!(
                "field:{:?} value is can to string",
                kind
            )));
        }
    };
    Ok(KindType::Single(val))
}
pub fn fix_kind_type(kind: &Kind, tp: &field::Type) -> CoreResult<Kind> {
    match (tp, kind) {
        (field::Type::Bool, Kind::BoolValue(v)) => Ok(Kind::BoolValue(*v)),
        (field::Type::Bool, Kind::IntValue(v)) => Ok(Kind::BoolValue(*v != 0)),
        (field::Type::Bool, Kind::FloatValue(v)) => Ok(Kind::BoolValue(*v != 0.0)),
        (field::Type::Bool, Kind::StringValue(v)) => Ok(Kind::BoolValue(v.parse()?)),
        (field::Type::Int, Kind::BoolValue(v)) => Ok(Kind::IntValue(*v as i64)),
        (field::Type::Int, Kind::IntValue(v)) => Ok(Kind::IntValue(*v)),
        (field::Type::Int, Kind::FloatValue(v)) => Ok(Kind::IntValue(*v as i64)),
        (field::Type::Int, Kind::StringValue(v)) => Ok(Kind::IntValue(v.parse()?)),
        (field::Type::Float, Kind::BoolValue(v)) => {
            Ok(Kind::FloatValue(if *v { 1.0 } else { 0.0 }))
        }
        (field::Type::Float, Kind::IntValue(v)) => Ok(Kind::FloatValue(*v as f32)),
        (field::Type::Float, Kind::FloatValue(v)) => Ok(Kind::FloatValue(*v)),
        (field::Type::Float, Kind::StringValue(v)) => Ok(Kind::StringValue(v.parse()?)),
        (field::Type::String, Kind::BoolValue(v)) => Ok(Kind::StringValue(v.to_string())),
        (field::Type::String, Kind::IntValue(v)) => Ok(Kind::StringValue(v.to_string())),
        (field::Type::String, Kind::FloatValue(v)) => Ok(Kind::StringValue(v.to_string())),
        (field::Type::String, Kind::StringValue(v)) => Ok(Kind::StringValue(v.to_string())),
        (field::Type::Text, Kind::BoolValue(v)) => Ok(Kind::StringValue(v.to_string())),
        (field::Type::Text, Kind::IntValue(v)) => Ok(Kind::StringValue(v.to_string())),
        (field::Type::Text, Kind::FloatValue(v)) => Ok(Kind::StringValue(v.to_string())),
        (field::Type::Text, Kind::StringValue(v)) => Ok(Kind::StringValue(v.to_string())),
        (_, Kind::ListValue(v)) => {
            let mut vec = Vec::with_capacity(v.values.len());
            for v in &v.values {
                if let Some(v) = &v.kind {
                    vec.push(Value {
                        kind: Some(fix_kind_type(v, tp)?),
                    });
                }
            }
            Ok(Kind::ListValue(ListValue { values: vec }))
        }
        _ => Err(CoreError::Notsupport(format!(
            "kind:{:?} cast to type:{:?} not support",
            kind, tp
        ))),
    }
}

pub fn str_to_vec_fix_type(value: &str, tp: &field::Type) -> CoreResult<Vec<u8>> {
    let kind = Kind::StringValue(value.to_string());
    let kind = fix_kind_type(&kind, tp)?;
    match kind_to_vec(&kind)? {
        KindType::Single(v) => Ok(v),
        KindType::Array(_) => unreachable!(),
    }
}

pub fn string_to_vec_fix_type(value: String, tp: &field::Type) -> CoreResult<Vec<u8>> {
    let kind = Kind::StringValue(value);
    let kind = fix_kind_type(&kind, tp)?;
    match kind_to_vec(&kind)? {
        KindType::Single(v) => Ok(v),
        KindType::Array(_) => unreachable!(),
    }
}

pub fn kind_to_vec_fix_type(kind: &Kind, tp: &field::Type) -> CoreResult<KindType<Vec<u8>>> {
    let kind = fix_kind_type(kind, tp)?;
    kind_to_vec(&kind)
}

pub fn kind_to_vec(kind: &Kind) -> CoreResult<KindType<Vec<u8>>> {
    let v = match kind {
        Kind::BoolValue(b) => Ok(vec![*b as u8]),
        Kind::IntValue(v) => memcomparable::to_vec(v),
        Kind::FloatValue(v) => memcomparable::to_vec(v),
        Kind::StringValue(v) => Ok(v.as_bytes().to_vec()),
        Kind::ListValue(v) => {
            let mut vec = Vec::with_capacity(v.values.len());
            for v in &v.values {
                if let Some(v) = &v.kind {
                    match kind_to_vec(v)? {
                        KindType::Single(v) => vec.push(v),
                        KindType::Array(arr) => vec.extend(arr),
                    };
                }
            }
            return Ok(KindType::Array(vec));
        }
        Kind::ObjectValue(_) | Kind::VectorValue(_) => {
            return Err(CoreError::InvalidParam(format!(
                "field:{:?} can not memcomparable",
                kind
            )));
        }
    };

    match v {
        Ok(v) => Ok(KindType::Single(v)),
        Err(e) => Err(CoreError::InvalidParam(format!(
            "field:{:?} can not memcomparable err:{:?}",
            kind, e
        ))),
    }
}
