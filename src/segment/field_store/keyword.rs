use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use arrow::array::{ArrayRef, ListArray, RecordBatch, StringArray, UInt32Array};
use roaring::RoaringBitmap;

use crate::{
    arrow_downcast,
    partition::WriteInfo,
    schema::field::{FieldOption, FieldType},
    segment::field_store::{IndexWriter, InvertedIndex, PkWriter},
    utils::error::{CoreError, CoreResult},
};

pub struct Keyword {
    field: FieldOption,
    indexs: RwLock<super::InvertedIndex<String>>,
}

impl Keyword {
    pub fn new(field: &FieldOption) -> Self {
        Self {
            field: field.clone(),
            indexs: RwLock::new(InvertedIndex::new_memory(64)),
        }
    }
}

// impl PrimaryKey<String> for Keyword {
//     fn get(&self, key: &String) -> Option<u32> {
//         self.indexs.read().unwrap().primary_key(key)
//     }
// }

impl IndexWriter for Keyword {
    fn write(&self, data: &RecordBatch) -> CoreResult<()> {
        let Some(arr) = data.column_by_name(&self.field.name()) else {
            return Ok(());
        };

        let mut mtp: HashMap<String, Vec<u32>> = HashMap::new();

        if self.field.is_array() {
            for (a, id) in arrow_downcast!(arr, ListArray)
                .iter()
                .zip(arrow_downcast!(data.column(0), UInt32Array).iter())
            {
                if let (Some(a), Some(id)) = (a, id) {
                    for v in arrow_downcast!(a, StringArray).iter().filter_map(|v| v) {
                        if let Some(list) = mtp.get_mut(v) {
                            if list.last() != Some(&id) {
                                list.push(id);
                            }
                        } else {
                            mtp.insert(v.to_string(), vec![id]);
                        }
                    }
                }
            }
        } else {
            for (a, id) in arrow_downcast!(arr, StringArray)
                .iter()
                .zip(arrow_downcast!(data.column(0), UInt32Array).iter())
            {
                if let (Some(v), Some(id)) = (a, id) {
                    if let Some(list) = mtp.get_mut(v) {
                        if list.last() != Some(&id) {
                            list.push(id);
                        }
                    } else {
                        mtp.insert(v.to_string(), vec![id]);
                    }
                }
            }
        }

        let mut indexs = self.indexs.read().unwrap().clone();

        for (k, ids) in mtp {
            indexs.extend(k, ids);
        }

        *self.indexs.write().unwrap() = indexs;

        Ok(())
    }

    fn name(&self) -> &str {
        &self.field.name()
    }

    fn field_type(&self) -> FieldType {
        FieldType::Keyword
    }

    fn mget_internal_id(&self, pk_filter: &RwLock<RoaringBitmap>, column: &ArrayRef) -> Vec<u32> {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PkWriter for Keyword {
    fn write_pk(
        &self,
        data: &RecordBatch,
        info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<HashSet<u32>> {
        let mut cur_dels = HashSet::new();

        let pk = data.column_by_name(self.name()).ok_or_else(|| {
            CoreError::Internal(format!(
                "field:{:?} not found in recordbatch",
                self.field.name()
            ))
        })?;

        let mut mtp: HashMap<String, Vec<u32>> = HashMap::new();

        // set mtp and cur_dels
        for (a, id) in arrow_downcast!(pk, StringArray)
            .iter()
            .zip(arrow_downcast!(data.column(0), UInt32Array).iter())
        {
            if let (Some(v), Some(id)) = (a, id) {
                if let Some(list) = mtp.get_mut(v) {
                    if !list.is_empty() {
                        cur_dels.extend(list.clone());
                    }
                    list[0] = id;
                } else {
                    mtp.insert(v.to_string(), vec![id]);
                }
            }
        }

        let _lock = lock.write().unwrap();

        let mut indexs = self.indexs.read().unwrap().clone();

        for (k, ids) in mtp {
            if let Some(old) = indexs.insert(k, ids) {
                cur_dels.extend(old.1.read().unwrap().iter());
            }
        }

        // mark other segment dels
        if let Some(WriteInfo(segments, del_list)) = info {
            for (i, ids) in del_list {
                segments[i].mark_del(ids);
            }
        }

        *self.indexs.write().unwrap() = indexs;

        Ok(cur_dels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_keyword_write_batches_string() {
        use arrow::array::{StringBuilder, UInt64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let field = FieldOption::Keyword {
            name: "tags".to_string(),
            is_array: false,
            index: true,
        };

        let keyword = Keyword::new(&field);

        let batch_size = 1000;
        let batch_count = 10_000;
        let start = std::time::Instant::now();

        for batch in 0..batch_count {
            let mut id_vec = Vec::with_capacity(batch_size);
            let mut builder = StringBuilder::new();

            use rand::Rng;
            let tag_count = 100_000;
            let mut rng = rand::thread_rng();

            for i in 0..batch_size {
                id_vec.push((batch * batch_size + i) as u32);
                let tag = format!("tag{}", rng.gen_range(0..tag_count));
                builder.append_value(&tag);
            }

            let string_array = builder.finish();

            let data = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::UInt32, false),
                    Field::new("tags", DataType::Utf8, true),
                ])),
                vec![Arc::new(UInt32Array::from(id_vec)), Arc::new(string_array)],
            )
            .unwrap();

            keyword.write(&data).unwrap();
        }

        println!(
            "10000批次，每批1000条（String）写入耗时: {:?}",
            start.elapsed()
        );
    }
}
