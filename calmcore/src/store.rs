use std::{
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc, Mutex, RwLock},
};

use proto::core::Record;

use crate::{
    index_store::{segment::SegmentReader, IndexStore, StoreInfo},
    util::{CoreError, CoreResult},
    RecordWrapper, Scope,
};

pub struct Store {
    scope: Arc<Scope>,
    increment_id: AtomicU64,
    index_store: Arc<RwLock<IndexStore>>,
    base_path: PathBuf,
    write_lock: Mutex<()>,
}

impl Store {
    pub fn new(scope: Arc<Scope>, base_path: PathBuf) -> CoreResult<Self> {
        let index_store = Arc::new(RwLock::new(IndexStore::new(
            &base_path,
            scope.user_fields.read().unwrap().clone(),
        )?));

        let max_start = index_store
            .read()
            .unwrap()
            .segment_readers()
            .iter()
            .map(|r| r.start())
            .max()
            .unwrap();

        let store = Store {
            scope,
            increment_id: AtomicU64::new(max_start),
            index_store,
            base_path,
            write_lock: Mutex::new(()),
        };
        Ok(store)
    }

    pub fn write(&self, mut records: Vec<RecordWrapper>, marker: Option<String>) -> Vec<CoreError> {
        let mut dels = Vec::new();

        let _lock = self.write_lock.lock().unwrap();
        let index_store = self.index_store.read().unwrap();

        for r in records.iter_mut() {
            if !r.result.is_ok() {
                continue;
            }

            if r.action_type == crate::ActionType::Append {
                r.record.id = self.increment_id();
            } else {
                let name = &r.record.name;

                // find it by name, if name is empty, return None
                let oid = index_store.find_by_name(name);

                match (oid, r.action_type) {
                    (None, crate::ActionType::Insert) | (None, crate::ActionType::Upsert) => {
                        r.record.id = self.increment_id();
                    }
                    (Some(_), crate::ActionType::Insert) => {
                        r.result = CoreError::Duplicated(format!("doc key:{:?}", r.record.name));
                    }
                    (Some(oid), crate::ActionType::Upsert) => {
                        r.record.id = self.increment_id();
                        dels.push(oid);
                    }
                    (Some(oid), crate::ActionType::Delete) => {
                        dels.push(oid);
                    }
                    (None, crate::ActionType::Delete) => {
                        r.result = CoreError::NotExisted(format!("doc key:{:?}", r.record.name));
                    }
                    (_, crate::ActionType::Append) => {
                        unimplemented!()
                    }
                };
            }
        }

        self.index_store
            .read()
            .unwrap()
            .write(records, dels, self.max_id(), marker)
    }

    pub fn get(&self, name: &String) -> Option<Record> {
        let readers: Vec<SegmentReader> = self.index_store.read().unwrap().segment_readers();
        readers
            .iter()
            .find_map(|r| r.get(name))
            .map(|r| (*r).clone())
    }

    fn increment_id(&self) -> u64 {
        self.increment_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1
    }

    fn max_id(&self) -> u64 {
        self.increment_id.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn find_record_by_id(&self, id: u64) -> Option<Record> {
        self.index_store.read().unwrap().find_by_id(id)
    }

    pub(crate) fn base_path(&self) -> &PathBuf {
        &self.base_path
    }

    pub(crate) fn new_current_segment(&self) -> CoreResult<()> {
        self.index_store
            .write()
            .unwrap()
            .new_current_segment(self.scope.user_fields.read().unwrap().clone())
    }

    pub(crate) fn hot_to_warm(&self, start: u64, end: u64) -> CoreResult<()> {
        let segment = self.index_store.read().unwrap().open_disk_segment(
            start,
            end,
            self.scope.user_fields.read().unwrap().clone(),
        )?;

        self.index_store.write().unwrap().hot_to_warm(segment)
    }

    pub(crate) fn segment_readers(&self) -> Vec<SegmentReader> {
        self.index_store.read().unwrap().segment_readers()
    }

    pub(crate) fn info(&self) -> CoreResult<StoreInfo> {
        self.index_store
            .read()
            .unwrap()
            .info(self.scope.schema.name.clone())
    }
}
