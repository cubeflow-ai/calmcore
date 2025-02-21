use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

use itertools::Itertools;
use proto::core::{Field, Query, QueryResult, Record, Schema};

use crate::index_store::seacher::Searcher;
use crate::index_store::segment::SegmentReader;
use crate::index_store::StoreInfo;
use crate::job::Job;
use crate::persist::schema::SchemaStore;
use crate::protocols::sql;
use crate::store::Store;
use crate::util::{CoreError, CoreResult};
use crate::{Action, ActionType, RecordWrapper, Scope};

/// Engine is the core of the database, it is responsible for managing the data and the indexes.
/// It provides methods to insert, update, delete, search and get records.
/// It also provides methods to add and delete index fields.
/// The Engine is thread-safe and can be shared between threads.
/// The Engine is created by the CalmCore or user send space and schema to create it.
/// The Engine is closed by calling the close method, which will release all resources used by the Engine
pub struct Engine {
    scope: Arc<Scope>,
    pub store: Store,
    schema_store: SchemaStore,
    is_closeing: AtomicBool,
}

impl Engine {
    pub(crate) fn create(data_path: &str, schema: Schema) -> CoreResult<Self> {
        let mut path = PathBuf::from(data_path);
        path.push(&schema.name);
        if path.exists() {
            return Err(CoreError::InvalidParam(format!(
                "data_path:{:?} already exist",
                path
            )));
        }
        std::fs::create_dir_all(&path)?;

        //write schema and space to source_store
        let schema_store = SchemaStore::new(&path)?;
        schema_store.write_schema(&schema)?;
        schema_store.write_user_schema(schema.fields.values().cloned().collect())?;
        Self::open(data_path, &schema.name)
    }

    /// Open a new Engine with the given data path and scope.
    /// The data path is the directory where the data will be stored.
    /// The scope contains the schema and space information.
    /// If the data path does not exist, a new Engine will be created.
    /// If the data path exists, the Engine will be opened.
    /// The Engine will load the data from the source db and the indexes from the index db.
    /// The Engine will return an error if the schema is not valid or if the data is corrupted.
    /// The Engine will return an error if the data path is not accessible or if the data path is not a directory.
    /// The Engine will return an error if the data path is not writable.
    ///
    /// Arguments:
    /// - `data_path` - The directory where the data will be stored.
    /// - `scope` - The scope containing the schema and space information.
    ///
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn open(data_path: &str, name: &str) -> CoreResult<Self> {
        let mut path = PathBuf::from(data_path);
        path.push(name);
        let schema_store = SchemaStore::new(&path)?;

        let schema = schema_store.read_schema()?;
        let user_schema = schema_store.read_user_schema()?;

        let scope = Arc::new(Scope {
            schema,
            user_fields: RwLock::new(HashMap::new()),
        });

        for field in user_schema.fields {
            scope
                .user_fields
                .write()
                .unwrap()
                .insert(field.name.clone(), Arc::new(field));
        }

        Ok(Self {
            scope: scope.clone(),
            store: Store::new(scope, path)?,
            schema_store,
            is_closeing: AtomicBool::new(false),
        })
    }

    /// Close the Engine and release all resources used by the Engine.
    ///
    pub fn close(self: Arc<Self>) {
        log::info!("Engine:{:?}", self.scope.schema);
        self.is_closeing
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let mut times = 1;
        loop {
            if Arc::strong_count(&self) == 1 {
                break;
            }
            log::info!(
                "Engine:{:?} to close times:{:?}",
                self.scope.schema.name,
                times
            );
            times += 1;
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        log::info!(
            "Engine:{:?} to close end use times:{:?}",
            self.scope.schema.name,
            times
        );
    }

    /// Check if the Engine is closing.
    pub fn is_closeing(&self) -> bool {
        self.is_closeing.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn mutate_json(
        &self,
        action: ActionType,
        name: String,
        json: &[u8],
        marker: Option<String>,
    ) -> CoreResult<CoreError> {
        let record = RecordWrapper::new(
            &self.scope,
            Record {
                name,
                data: json.to_vec(),
                ..Default::default()
            },
            action,
        );

        self.mutate_records(vec![record], marker)?
            .into_iter()
            .next()
            .ok_or_else(|| CoreError::Internal("unimplemented none resule".to_string()))
    }

    fn mutate_records(
        &self,
        records: Vec<RecordWrapper>,
        marker: Option<String>,
    ) -> CoreResult<Vec<CoreError>> {
        Ok(self.store.write(records, marker))
    }

    /// Write records to the source db and update the index db
    /// Arguments:
    ///    - `records` - The records to write.
    ///    - `marker` - The marker to use for the write.
    ///      Return a list of results for each action
    pub fn mutate(
        &self,
        records: Vec<Action>,
        marker: Option<String>,
    ) -> CoreResult<Vec<CoreError>> {
        self.mutate_records(
            records
                .into_iter()
                .map(|v| Action::into_recordwrapper(v, &self.scope))
                .collect_vec(),
            marker,
        )
    }

    /// Get a record by name from the Engine.
    /// Arguments:
    /// - `record_name` - The name of the record.
    pub fn get(&self, record_name: &String) -> Option<Record> {
        self.store.get(record_name)
    }

    /// Search by Query struct
    /// Arguments:
    /// - `req` - The Query struct.
    ///   Return:
    /// - `QueryResult` - The result of the search.
    pub fn search(&self, req: Query) -> CoreResult<QueryResult> {
        let query = sql::pbquery_to_query(self.scope(), req)?;
        let searcher = Searcher::new(self.store.segment_readers());
        searcher.search_query(query)
    }

    /// Search by SQL string
    /// Arguments:
    /// - `sql` - The SQL string.
    ///   Return:
    /// - `QueryResult` - The result of the search.
    pub fn sql(&self, sql: &str) -> CoreResult<QueryResult> {
        let query = sql::sql_to_query(self.scope(), sql)?;
        let searcher = Searcher::new(self.store.segment_readers());
        searcher.search_query(query)
    }

    /// Add a new index field to the Engine. you can add your own index fields, the field name must not existd in schema field,
    /// Arguments:
    /// - `field` - The field to add.
    ///   Return:
    /// - `CoreResult` - Ok if the field was added successfully, an error otherwise.
    pub fn add_index_field(&self, field: Field) -> CoreResult<()> {
        let name = field.name.clone();

        if name.starts_with("_") {
            return Err(CoreError::InvalidParam(format!(
                "field:{:?} can not start with _",
                name,
            )));
        }

        let mut user_fields = self.scope.user_fields.write().unwrap();

        if user_fields.contains_key(&name) {
            return Err(CoreError::InvalidParam(format!(
                "field:{:?} exist in user schema{:?}",
                field, self.scope.schema,
            )));
        }

        user_fields.insert(name.clone(), Arc::new(field));

        self.schema_store
            .write_user_schema(user_fields.iter().map(|(_, v)| (**v).clone()).collect())?;

        match self.store.new_current_segment() {
            Ok(_) => Ok(()),
            Err(e) => {
                log::error!("add index field error:{:?}", e);
                user_fields.remove(name.as_str());
                self.schema_store
                    .write_user_schema(user_fields.iter().map(|(_, v)| (**v).clone()).collect())?;
                Err(CoreError::Internal("add index field error".to_string()))
            }
        }
    }

    /// Delete an index field from the Engine.
    /// Arguments:
    /// - `field_name` - The name of the field to delete.
    ///   Return:
    /// - `CoreResult` - Ok if the field was deleted successfully, an error otherwise.
    ///   The field must not exist in the schema field.
    ///   The field must exist in the user schema field.
    pub fn delete_index_field(&self, field_name: &str) -> CoreResult<()> {
        let name = field_name;

        if name.starts_with("_") {
            return Err(CoreError::InvalidParam(format!(
                "field:{:?} can not start with _",
                name,
            )));
        }

        let mut user_fields = self.scope.user_fields.write().unwrap();

        if !user_fields.contains_key(name) {
            return Err(CoreError::NotExisted(format!(
                "field:{:?} not exist in user schema{:?}",
                name, self.scope.schema,
            )));
        }

        let field = user_fields.remove(name);
        if field.is_none() {
            return Err(CoreError::NotExisted(format!(
                "field:{:?} not exist in user schema{:?}",
                name, self.scope.schema,
            )));
        }

        self.schema_store
            .write_user_schema(user_fields.iter().map(|(_, v)| (**v).clone()).collect())?;

        match self.store.new_current_segment() {
            Ok(_) => Ok(()),
            Err(e) => {
                log::error!("add index field error:{:?}", e);
                user_fields.insert(name.to_string(), field.unwrap());
                self.schema_store
                    .write_user_schema(user_fields.iter().map(|(_, v)| (**v).clone()).collect())?;
                Err(CoreError::Internal("add index field error".to_string()))
            }
        }
    }

    /// Persist the Engine data to disk.
    /// make new current segment and persist hot segment in freezed list to warm segment.
    /// This will write the data to the source db and update the index db.
    /// Return an error if the data could not be written to disk.
    pub fn persist(self: &Arc<Engine>) -> CoreResult<()> {
        self.store.new_current_segment()?;
        Job::persist(self.clone(), true)
    }

    pub fn info(&self) -> CoreResult<StoreInfo> {
        self.store.info()
    }
}

impl Engine {
    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn segment_readers(&self) -> Vec<SegmentReader> {
        self.store.segment_readers()
    }

    pub fn hot_to_warm(&self, start: u64, end: u64) -> CoreResult<()> {
        self.store.hot_to_warm(start, end)
    }
}

#[cfg(test)]
mod tests {
    use crate::Engine;
    use proto::core::{field::Type as FieldType, Field, Schema};
    use std::{collections::HashMap, path::PathBuf, sync::Arc};

    fn setup_test_engine() -> Arc<Engine> {
        // Create a temporary directory for testing
        let test_dir = PathBuf::from("./calmcore_test");
        let _ = std::fs::remove_dir_all(&test_dir); // Clean up any previous test data
        std::fs::create_dir_all(&test_dir).unwrap();

        // Define schema
        let schema = Schema {
            name: "test_schema".to_string(),
            metadata: Default::default(), // Add missing metadata field
            fields: {
                let mut map = HashMap::new();
                map.insert(
                    "id".to_string(),
                    Field {
                        name: "id".to_string(),
                        r#type: FieldType::Int as i32,
                        option: None,
                    },
                );
                map.insert(
                    "name".to_string(),
                    Field {
                        name: "name".to_string(),
                        r#type: FieldType::String as i32,
                        option: None,
                    },
                );
                map.insert(
                    "age".to_string(),
                    Field {
                        name: "age".to_string(),
                        r#type: FieldType::Int as i32,
                        option: None,
                    },
                );
                map
            },
            schemaless: false,
        };

        // Create and return engine
        Arc::new(Engine::create(test_dir.to_str().unwrap(), schema).unwrap())
    }

    #[test]
    fn test_insert_and_query() {
        let engine = setup_test_engine();

        // Insert test data
        for i in 0..100 {
            let test_data = format!(
                r#"{{
                "id": {},
                "name": "User{}",
                "age": {}
            }}"#,
                i,
                i,
                20 + (i % 10)
            );

            engine
                .mutate_json(
                    crate::ActionType::Append,
                    format!("record{}", i),
                    test_data.as_bytes(),
                )
                .unwrap();
        }

        // Test simple query
        let sql = "SELECT id, name FROM test_space WHERE age = 25 limit 5";
        let result = engine.sql(sql).unwrap();

        assert_eq!(result.total_hits, 10);

        assert_eq!(result.hits.len(), 5);

        assert_eq!(result.hits[0].id, 6);

        // Test range query
        let sql = "SELECT id, name FROM test_space WHERE age >= 20 AND age < 30";
        let result = engine.sql(sql).unwrap();
        assert_eq!(result.total_hits, 100);

        // Test no results query
        let sql = "SELECT id, name FROM test_space WHERE age >= 30";
        let result = engine.sql(sql).unwrap();
        assert_eq!(result.total_hits, 0);

        // Clean up
        std::fs::remove_dir_all("./calmcore_test").unwrap();
    }

    #[test]
    fn test_insert_and_query_score() {
        let engine = setup_test_engine();

        // Insert test data
        for i in 0..100 {
            let test_data = format!(
                r#"{{
                "id": {},
                "name": "User{}",
                "age": {}
            }}"#,
                i,
                i,
                20 + (i % 10)
            );

            engine
                .mutate_json(
                    crate::ActionType::Append,
                    format!("record{}", i),
                    test_data.as_bytes(),
                )
                .unwrap();
        }

        // Test simple query
        let sql = "SELECT id, name FROM test_space WHERE  (name = fn('User25', score=100)  or name = fn('User26', score=50)) order by _score asc  limit 5";
        let result = engine.sql(sql).unwrap();

        println!("{:?}---{:?}", result.total_hits, result.hits.len());

        for hi in result.hits.iter() {
            println!(
                "{:?}-------{}",
                hi.score,
                String::from_utf8(hi.record.as_ref().unwrap().data.clone()).unwrap()
            );
        }

        // Clean up
        std::fs::remove_dir_all("./calmcore_test").unwrap();
    }
}
