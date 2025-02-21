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
    sync::{Arc, Mutex, RwLock},
};

use crate::{
    util::{CoreError, CoreResult},
    *,
};
use job::Job;
use proto::core::Schema;

/// Configuration for CalmCore
///
/// This struct contains settings for:
/// 1. Rate limiting, circuit breaking and degradation
/// 2. Segment persistence configuration
/// 3. Performance tuning parameters
/// 4. System resource limits
pub struct Config {
    pub data_path: String, // store path

    // segment persist max size, default:1_000_000
    pub segment_max_size: usize,
    // segment persist interval default:3600
    pub flush_interval_secs: u64,
}

impl Config {
    pub fn new(data_path: &str) -> Self {
        Config {
            data_path: data_path.to_string(),
            segment_max_size: 1_000_000,
            flush_interval_secs: 3600,
        }
    }

    pub fn segment_max_size(mut self, segment_max_size: usize) -> Self {
        self.segment_max_size = segment_max_size;
        self
    }

    pub fn flush_interval_secs(mut self, flush_interval_secs: u64) -> Self {
        self.flush_interval_secs = flush_interval_secs;
        self
    }
}

/// The CalmCore is the core engine for data indexing and storage
pub struct CalmCore {
    data_path: String,
    engine_map: RwLock<HashMap<String, Arc<Engine>>>,
    engine_lock: Mutex<()>,
    job: Arc<Job>,
}

impl CalmCore {
    /// Create a new CalmCore instance
    /// # Arguments
    /// * `data_path` - The path to store the data
    /// * `model` - The model of the meta manager，if cluster model you need impl MetaManager to your own
    /// # Example
    /// ```rust
    /// use calm_core::CalmCore;
    /// use proto::core::field;
    /// use proto::core::Schema;
    /// use calm_core::MetaModel;
    /// use calm_core::util::CoreResult;
    /// fn main() -> CoreResult<()> {
    ///    let core = CalmCore::new("data", MetaModel::Local)?;
    /// }
    /// ```
    /// # Errors
    pub fn new(data_path: &str) -> CoreResult<Self> {
        Self::new_with_conf(Config::new(data_path))
    }

    pub fn new_with_conf(conf: Config) -> CoreResult<Self> {
        std::fs::create_dir_all(&conf.data_path).map_err(|e| {
            CoreError::Existed(format!("create dir:{} error:{:?}", conf.data_path, e))
        })?;

        let job = Job::new(conf.segment_max_size, conf.flush_interval_secs);

        Ok(CalmCore {
            data_path: conf.data_path,
            engine_map: RwLock::new(Default::default()),
            engine_lock: Default::default(),
            job,
        })
    }

    pub fn create_engine(&self, schema: Schema) -> CoreResult<Arc<Engine>> {
        let name = schema.name.clone();
        let lock = self.engine_lock.lock().unwrap();

        if self.engine_map.read().unwrap().contains_key(&name) {
            return Err(CoreError::Existed(format!(
                "engine name:{:?} is existed",
                name
            )));
        }

        let engine = Arc::new(Engine::create(&self.data_path, schema)?);

        self.engine_map
            .write()
            .unwrap()
            .insert(name, engine.clone());

        drop(lock);

        self.job.add_engine(engine.clone());

        Ok(engine)
    }

    /// load space if it exist, if not exist it will try to load from disk
    /// # Arguments
    /// * `name` - The name of the space
    /// # Returns a engine instance, the engine can operate document
    /// # Errors
    /// * If the space not exist, return CoreError::NotExisted
    /// * If the schema not exist, return CoreError::NotExisted
    /// * If the engine is closeing it will return NotExisted error
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn load_engine(&self, engine_name: &str) -> CoreResult<Arc<Engine>> {
        if let Some(v) = self.engine_map.read().unwrap().get(engine_name) {
            if v.is_closeing() {
                return Err(CoreError::Duplicated(format!(
                    "engine:{:?} is closeing please wait",
                    engine_name
                )));
            }
            return Ok(v.clone());
        };

        let lock = self.engine_lock.lock();

        if let Some(v) = self.engine_map.read().unwrap().get(engine_name) {
            return Ok(v.clone());
        };

        let engine = Arc::new(Engine::open(&self.data_path, engine_name)?);

        self.engine_map
            .write()
            .unwrap()
            .insert(engine_name.to_string(), engine.clone());

        drop(lock);

        Ok(engine)
    }

    /// get engine if it exist
    /// # Arguments
    /// * `name` - The name of the space
    /// # Returns a engine instance, the engine can operate document
    /// # Errors
    /// * If the engine not exist, return CoreError::NotExisted
    /// * If engine is closeing it will return NotExisted error
    pub fn get_engine(&self, name: &str) -> CoreResult<Arc<Engine>> {
        let engine_name = name;
        match self.engine_map.write().unwrap().get(engine_name) {
            Some(v) if !v.is_closeing() => Ok(v.clone()),
            _ => Err(CoreError::NotExisted(format!("space name:{:?}", name))),
        }
    }

    pub fn release_engine(&self, engine_name: &str) -> CoreResult<()> {
        let engine = { self.engine_map.write().unwrap().get(engine_name).cloned() };

        if engine.is_none() {
            return Ok(());
        }

        let engine = engine.unwrap();
        if engine.is_closeing() {
            return Err(CoreError::Duplicated(format!(
                "engine:{:?} is closeing please wait",
                engine_name
            )));
        }

        engine.close();

        self.engine_map.write().unwrap().remove(engine_name);

        Ok(())
    }

    pub fn list_engine(&self) -> CoreResult<Vec<String>> {
        Ok(self.engine_map.read().unwrap().keys().cloned().collect())
    }
}

#[cfg(test)]
mod tests {

    use proto::core::{field, Field, Schema};

    use crate::CalmCore;

    #[test]
    fn test_calm_core() {
        let core = CalmCore::new("data").unwrap();

        let schema = Schema {
            name: "test".to_string(),
            fields: vec![("name".to_string(), field::Type::String, None)]
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
        };

        let engine = core.create_engine(schema).unwrap();

        let engine = core.load_engine("test").unwrap();

        core.release_engine("test").unwrap();

        let engine = core.load_engine("test").unwrap();

        core.release_engine("test").unwrap();
    }
}
