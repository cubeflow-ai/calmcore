//! # The core engine for data indexing and storage
//!
//! ## Features
//!
//! * structured index ✅
//! * full-text index ✅
//! * vector index ✅
//!
//! Licensed under either of
//! * Apache License, Version 2.0,
//!   (./LICENSE-APACHE or <http://www.apache.org/licenses/LICENSE-2.0>)
//! * MIT license (./LICENSE-MIT or <http://opensource.org/licenses/MIT>)
//!   at your option.
//!
//! ## Examples
//!
//! All examples are in the [sub-repository](https://github.com/xxxxxx/examples), located in the examples directory.
//!
//! **Run an example:**
//!
//! ```shell script
//! cd test
//! cargo run --example zwxx_example
//! cargo run --example sample_search
//! cargo run --example vector_search
//! ```
//!

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use proto::core::{field, Field, Record, Schema, Value};
use serde::{Deserialize, Serialize};
use util::{CoreError, CoreResult};

pub mod analyzer;
mod calm_core;
mod embedding;
mod engine;
mod index_store;
mod job;
pub mod persist;
mod protocols;
mod store;
pub mod util;

pub type Engine = engine::Engine;
pub type CalmCore = calm_core::CalmCore;

#[derive(Debug, Serialize)]
pub struct Scope {
    pub schema: Schema,
    #[serde(skip)]
    pub user_fields: RwLock<HashMap<String, Arc<Field>>>,
}

impl Scope {
    pub fn get_field(&self, name: &str) -> Option<Arc<Field>> {
        self.user_fields.read().unwrap().get(name).cloned()
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum ActionType {
    Append,
    Insert,
    Delete,
    Upsert,
}

pub enum Action {
    Append(Record),
    Insert(Record),
    Delete(Record),
    Upsert(Record),
}

impl Action {
    pub fn record(&self) -> &Record {
        match self {
            Action::Append(record) => record,
            Action::Insert(record) => record,
            Action::Delete(record) => record,
            Action::Upsert(record) => record,
        }
    }

    pub fn to_record(self) -> Record {
        match self {
            Action::Append(record) => record,
            Action::Insert(record) => record,
            Action::Delete(record) => record,
            Action::Upsert(record) => record,
        }
    }

    /// Convert Action to RecordWrapper
    /// # Returns
    /// * RecordWrapper
    fn into_recordwrapper(self, scope: &Scope) -> RecordWrapper {
        match self {
            Action::Append(r) => RecordWrapper::new(scope, r, ActionType::Append),
            Action::Insert(r) => RecordWrapper::new(scope, r, ActionType::Insert),
            Action::Upsert(r) => RecordWrapper::new(scope, r, ActionType::Upsert),
            Action::Delete(r) => RecordWrapper::new(scope, r, ActionType::Delete),
        }
    }

    pub fn new(tp: ActionType, name: &str, data: &[u8]) -> Self {
        let record = Record {
            name: name.to_string(),
            data: data.to_vec(),
            ..Default::default()
        };
        match tp {
            ActionType::Insert => Action::Insert(record),
            ActionType::Delete => Action::Delete(record),
            ActionType::Upsert => Action::Upsert(record),
            ActionType::Append => Action::Append(record),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct RecordWrapper {
    pub action_type: ActionType,
    pub record: Record,
    pub value: Option<Value>,
    pub vectors: Option<Vec<proto::core::Vector>>,
    pub result: CoreError,
}

impl RecordWrapper {
    /// Create a new RecordWrapper ，
    /// it move Record.vectors -> RecordWrapper.vectors
    /// parse record.data json -> RecordWrapper.value
    /// # Arguments
    /// * `record` - Record
    /// * `action_type` - ActionType
    /// # Returns
    /// * RecordWrapper if error result set CoreError
    fn new(scope: &Scope, mut record: Record, action_type: ActionType) -> Self {
        let result = if record.data.is_empty() {
            None
        } else {
            Some(util::json_data_to_value(scope, &record.data))
        };
        let vectors = if record.vectors.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut record.vectors))
        };

        match result {
            Some(Ok(value)) => Self {
                action_type,
                record,
                value: Some(value),
                vectors,
                result: CoreError::Ok,
            },
            Some(Err(result)) => Self {
                action_type,
                record,
                value: None,
                vectors,
                result,
            },
            None => Self {
                action_type,
                record,
                value: None,
                vectors,
                result: CoreError::Ok,
            },
        }
    }

    /// Check if the record index valid
    /// # Returns
    /// * bool if true need index else skip
    pub fn valid_index(&self) -> bool {
        self.action_type != ActionType::Delete && self.result.is_ok()
    }

    /// Serialize RecordWrapper to Vec<u8>
    /// # Returns CoreResult
    pub fn serialize_record(buffer: &mut Vec<u8>, record: &Record) -> CoreResult<()> {
        // Clear buffer first
        buffer.clear();
        // Serialize into the buffer
        bincode::serialize_into(buffer, record)?;
        Ok(())
    }

    /// Deserialize Vec<u8> to RecordWrapper
    /// # Returns CoreResult<RecordWrapper>
    pub fn deserialize_record(data: &[u8]) -> CoreResult<Record> {
        Ok(bincode::deserialize(data)?)
    }

    pub fn name(&self) -> &str {
        &self.record.name
    }

    pub fn id(&self) -> u64 {
        self.record.id
    }

    pub fn abs_id(&self, start: u64) -> u32 {
        if self.record.id < start {
            println!("abs_id: {}---------------{}", self.record.id, start);
        }
        (self.record.id - start) as u32
    }
}

pub fn easy_schema(
    name: &str,
    fields: Vec<(String, field::Type, Option<field::Option>)>,
) -> Schema {
    Schema {
        name: name.to_string(),
        fields: fields
            .into_iter()
            .map(|(name, r#type, option)| {
                (
                    name.clone(),
                    Field {
                        name,
                        r#type: r#type as i32,
                        option,
                    },
                )
            })
            .collect(),
        metadata: None,
        schemaless: false,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use proto::core::field;

    use crate::{RecordWrapper, Scope};

    #[test]
    fn test_scope() {
        let scope = Scope {
            schema: Default::default(),
            space: Default::default(),
            user_fields: Default::default(),
        };

        let schema = crate::easy_schema(
            "test",
            vec![("name".to_string(), field::Type::String, None)],
        );
        let mut user_fields = HashMap::new();
        user_fields.insert(
            "name".to_string(),
            Arc::new(schema.fields.get("name").unwrap().clone()),
        );
        *scope.user_fields.write().unwrap() = user_fields;

        assert_eq!(scope.get_field("name").unwrap().name, "name");
    }

    #[test]
    fn test_record_wrapper() {
        let scope = Scope {
            schema: Default::default(),
            space: Default::default(),
            user_fields: Default::default(),
        };

        let schema = crate::easy_schema(
            "test",
            vec![("name".to_string(), field::Type::String, None)],
        );
        let mut user_fields = HashMap::new();
        user_fields.insert(
            "name".to_string(),
            Arc::new(schema.fields.get("name").unwrap().clone()),
        );
        *scope.user_fields.write().unwrap() = user_fields;

        let record = crate::Record {
            id: 1,
            name: "test".to_string(),
            data: r#"{"name":"test"}"#.as_bytes().to_vec(),
            vectors: vec![],
        };

        let record_wrapper = crate::RecordWrapper::new(&scope, record, crate::ActionType::Insert);

        assert_eq!(record_wrapper.name(), "test");
        assert_eq!(record_wrapper.id(), 1);
        assert_eq!(record_wrapper.abs_id(0), 1);
    }

    #[test]
    fn test_record_wrapper_serialize() {
        let scope = Scope {
            schema: Default::default(),
            space: Default::default(),
            user_fields: Default::default(),
        };

        let schema = crate::easy_schema(
            "test",
            vec![("name".to_string(), field::Type::String, None)],
        );
        let mut user_fields = HashMap::new();
        user_fields.insert(
            "name".to_string(),
            Arc::new(schema.fields.get("name").unwrap().clone()),
        );
        *scope.user_fields.write().unwrap() = user_fields;

        let record = crate::Record {
            id: 1,
            name: "test".to_string(),
            data: r#"{"name":"test"}"#.as_bytes().to_vec(),
            vectors: vec![],
        };

        let record_wrapper = crate::RecordWrapper::new(&scope, record, crate::ActionType::Insert);

        let mut buffer = Vec::new();
        RecordWrapper::serialize_record(&mut buffer, &record_wrapper.record).unwrap();

        let record = crate::RecordWrapper::deserialize_record(&buffer).unwrap();

        let record_wrapper = RecordWrapper::new(&scope, record, crate::ActionType::Insert);

        assert_eq!(record_wrapper.name(), "test");
        assert_eq!(record_wrapper.id(), 1);
        assert_eq!(record_wrapper.abs_id(0), 1);
    }
}
