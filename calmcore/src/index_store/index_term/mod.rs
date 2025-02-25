pub(crate) mod reader;
pub(crate) mod serializer;
mod writer;
use crate::{
    util::{kind_to_vec_fix_type, CoreResult, KindType},
    RecordWrapper,
};
use croaring::Bitmap;
use reader::TermIndexReader;
use serializer::TermDeserializer;
use std::{path::PathBuf, sync::Arc};
use writer::Handler;

use super::store::InvertIndex;

type TermInvertIndex = InvertIndex<Vec<u8>, Bitmap>;

pub struct TermIndex {
    start: u64,
    inner: Arc<proto::core::Field>,
    term_index: TermInvertIndex,
}

impl TermIndex {
    pub fn new_mem(start: u64, inner: Arc<proto::core::Field>) -> CoreResult<Self> {
        Ok(Self {
            start,
            inner,
            term_index: TermInvertIndex::new_memory(),
        })
    }

    pub fn new_disk(start: u64, inner: Arc<proto::core::Field>, path: PathBuf) -> CoreResult<Self> {
        Ok(Self {
            start,
            inner,
            term_index: TermInvertIndex::new_disk(path, Box::new(TermDeserializer {}))?,
        })
    }

    fn handler(&self) -> Handler {
        Handler::new(self.term_index.clone_map())
    }

    pub fn reader(&self) -> TermIndexReader {
        TermIndexReader {
            start: self.start,
            term_record_index: self.term_index.index_reader(),
            inner: self.inner.clone(),
            term_serializer: Box::new(serializer::TermSerializer {}),
        }
    }

    pub fn field_type(&self) -> proto::core::field::Type {
        self.inner.r#type()
    }

    pub fn field_name(&self) -> &str {
        &self.inner.name
    }
}

impl TermIndex {
    pub fn write(&self, records: &[RecordWrapper]) {
        if records.is_empty() {
            return;
        }

        let mut handler = self.handler();
        for r in records {
            if let Some(val) = &r.value {
                if let Some(value) = val.obj().fields.get(&self.inner.name) {
                    if let Some(kind) = value.kind.as_ref() {
                        match kind_to_vec_fix_type(kind, &self.field_type()) {
                            Ok(KindType::Single(v)) => handler.push_index(v, r.abs_id(self.start)),
                            Ok(KindType::Array(arr)) => {
                                for v in arr {
                                    handler.push_index(v, r.abs_id(self.start))
                                }
                            }
                            Err(e) => log::trace!("err:{:?}, ignore it", e),
                        }
                    } else {
                        log::trace!("field value:{:?} is not text, ignore it", value);
                    }
                }
            }
        }

        //replace maptree with new one
        self.term_index.replace(handler.release());
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock},
    };

    use itertools::Itertools;
    use proto::core::{Record, Schema};

    use crate::{ActionType, RecordWrapper, Scope};

    use super::TermIndex;

    fn init_field() -> TermIndex {
        let field = Arc::new(proto::core::Field {
            name: "test".to_string(),
            option: Default::default(),
            r#type: proto::core::field::Type::String as i32,
        });

        TermIndex::new_mem(0, field).unwrap()
    }

    fn init_scope() -> Scope {
        Scope {
            schema: Schema::default(),
            user_fields: RwLock::new(HashMap::new()),
        }
    }

    fn init_records() -> Vec<RecordWrapper> {
        let mut records = Vec::new();
        for i in 0..10 {
            let data = serde_json::json!({
                "test": format!("test{}", i),
            });

            let data = serde_json::to_vec(&data).expect("Failed to convert to bytes");
            let record = RecordWrapper::new(
                &init_scope(),
                Record {
                    id: i,
                    name: format!("test{}", i),
                    data,
                    ..Default::default()
                },
                ActionType::Insert,
            );
            records.push(record);
        }
        records
    }

    #[test]
    fn create_test() {
        init_field();
    }

    #[test]
    fn write_test() {
        let term_index = init_field();
        let records = init_records();
        term_index.write(&records);
    }

    #[test]
    fn term_write_test() {
        let term_index = init_field();
        let records = init_records();
        term_index.write(&records);
        for i in 0..10 {
            let result = term_index
                .reader()
                .term(&format!("test{}", i).as_bytes().to_vec());
            assert_eq!(result.unwrap().cardinality(), 1);
        }
    }

    #[test]
    fn term_range_test() {
        let term_index = init_field();
        let records = init_records();
        term_index.write(&records);

        // [3,8]
        let result = term_index
            .reader()
            .between(
                Some(&format!("test{}", 3).as_bytes().to_vec()),
                true,
                Some(&format!("test{}", 8).as_bytes().to_vec()),
                true,
            )
            .unwrap();
        assert_eq!(result.iter().collect_vec(), vec![3, 4, 5, 6, 7, 8]);

        // [3,8)
        let result = term_index
            .reader()
            .between(
                Some(&format!("test{}", 3).as_bytes().to_vec()),
                true,
                Some(&format!("test{}", 8).as_bytes().to_vec()),
                false,
            )
            .unwrap();
        assert_eq!(result.iter().collect_vec(), vec![3, 4, 5, 6, 7]);

        // (3,8]
        let result = term_index
            .reader()
            .between(
                Some(&format!("test{}", 3).as_bytes().to_vec()),
                false,
                Some(&format!("test{}", 8).as_bytes().to_vec()),
                true,
            )
            .unwrap();
        assert_eq!(result.iter().collect_vec(), vec![4, 5, 6, 7, 8]);

        // (3,8)
        let result = term_index
            .reader()
            .between(
                Some(&format!("test{}", 3).as_bytes().to_vec()),
                false,
                Some(&format!("test{}", 8).as_bytes().to_vec()),
                false,
            )
            .unwrap();
        assert_eq!(result.iter().collect_vec(), vec![4, 5, 6, 7]);

        // (None,8)
        let result = term_index
            .reader()
            .between(
                None,
                false,
                Some(&format!("test{}", 8).as_bytes().to_vec()),
                false,
            )
            .unwrap();
        assert_eq!(result.iter().collect_vec(), vec![0, 1, 2, 3, 4, 5, 6, 7]);

        // (3,None)
        let result = term_index
            .reader()
            .between(
                Some(&format!("test{}", 3).as_bytes().to_vec()),
                false,
                None,
                false,
            )
            .unwrap();
        assert_eq!(result.iter().collect_vec(), vec![4, 5, 6, 7, 8, 9]);

        // (None,None)
        let result = term_index
            .reader()
            .between(None, false, None, false)
            .unwrap();
        assert_eq!(
            result.iter().collect_vec(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        );

        // [None,None]
        let result = term_index.reader().between(None, true, None, true).unwrap();
        assert_eq!(
            result.iter().collect_vec(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        );
    }
}
