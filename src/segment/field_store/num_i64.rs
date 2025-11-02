use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use arrow::array::{ArrayRef, Int64Array, ListArray, RecordBatch, UInt32Array};
use roaring::RoaringBitmap;

use super::{serializer::I64RoaringSerializer, IndexWriter, InvertedIndex, PkWriter};
use crate::{
    arrow_downcast,
    partition::WriteInfo,
    schema::field::{FieldOption, FieldType},
    utils::error::{CoreError, CoreResult},
};

pub struct NumI64 {
    field: FieldOption,
    indexs: RwLock<InvertedIndex<i64>>,
}

impl NumI64 {
    pub fn new(field: &FieldOption) -> Self {
        Self {
            field: field.clone(),
            indexs: RwLock::new(InvertedIndex::new_memory(64)),
        }
    }

    pub fn from_disk(field: &FieldOption, inverted_index: InvertedIndex<i64>) -> CoreResult<Self> {
        Ok(Self {
            field: field.clone(),
            indexs: RwLock::new(inverted_index),
        })
    }

    pub fn persist(&self, path: &str) -> CoreResult<Self> {
        use mem_btree::persist::TreeWriter;

        let memory_index = self.indexs.read().unwrap();
        let btree = match &*memory_index {
            InvertedIndex::Memory(tree) => tree,
            InvertedIndex::Disk(_) => {
                return Err(CoreError::Internal("Cannot persist disk index".to_string()));
            }
        };

        let len = btree.len();
        let iter = btree.iter().map(|item| {
            let (key, value_lock, _ttl) = &*item;
            let ids = value_lock.read().unwrap();
            let bitmap = RoaringBitmap::from_sorted_iter(ids.iter().copied()).unwrap();
            Arc::new((*key, bitmap, None))
        });

        let serializer = I64RoaringSerializer::default();
        let writer = TreeWriter::new(std::path::PathBuf::from(path), 128, 8);

        writer
            .persist::<i64, RoaringBitmap, RoaringBitmap>(len, Box::new(serializer), iter)
            .map_err(|e| CoreError::IOError(e.to_string()))?;

        let disk_index = InvertedIndex::new_disk(path, I64RoaringSerializer::default())?;

        Ok(Self {
            field: self.field.clone(),
            indexs: RwLock::new(disk_index),
        })
    }
}

impl IndexWriter for NumI64 {
    fn write(&self, data: &RecordBatch) -> CoreResult<()> {
        let Some(arr) = data.column_by_name(&self.field.name()) else {
            return Ok(());
        };

        let mut mtp: HashMap<i64, Vec<u32>> = HashMap::new();

        if self.field.is_array() {
            for (a, id) in arrow_downcast!(arr, ListArray)
                .iter()
                .zip(arrow_downcast!(data.column(0), UInt32Array).iter())
            {
                if let (Some(a), Some(id)) = (a, id) {
                    for v in arrow_downcast!(a, Int64Array).iter().flatten() {
                        let key = v;
                        if let Some(list) = mtp.get_mut(&key) {
                            if list.last() != Some(&id) {
                                list.push(id);
                            }
                        } else {
                            mtp.insert(key, vec![id]);
                        }
                    }
                }
            }
        } else {
            for (a, id) in arrow_downcast!(arr, Int64Array)
                .iter()
                .zip(arrow_downcast!(data.column(0), UInt32Array).iter())
            {
                if let (Some(v), Some(id)) = (a, id) {
                    let key = v;
                    if let Some(list) = mtp.get_mut(&key) {
                        if list.last() != Some(&id) {
                            list.push(id);
                        }
                    } else {
                        mtp.insert(key, vec![id]);
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
        FieldType::I64
    }

    fn mget_internal_id(&self, _pk_filter: &RwLock<RoaringBitmap>, _column: &ArrayRef) -> Vec<u32> {
        vec![]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PkWriter for NumI64 {
    fn write_pk(
        &self,
        data: &RecordBatch,
        _info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<HashSet<u32>> {
        let mut cur_dels = HashSet::new();

        let pk = data.column_by_name(self.name()).ok_or_else(|| {
            CoreError::Internal(format!(
                "field:{:?} not found in recordbatch",
                self.field.name()
            ))
        })?;

        let mut mtp: HashMap<i64, Vec<u32>> = HashMap::new();
        let internal_id_array = arrow_downcast!(data.column(0), UInt32Array);

        for (a, id) in arrow_downcast!(pk, Int64Array)
            .iter()
            .zip(internal_id_array.iter())
        {
            if let (Some(v), Some(id)) = (a, id) {
                let key = v;
                if let Some(list) = mtp.get_mut(&key) {
                    if !list.is_empty() {
                        cur_dels.extend(list.clone());
                    }
                    list[0] = id;
                } else {
                    mtp.insert(key, vec![id]);
                }
            }
        }

        let _lock = lock.write().unwrap();

        let indexs = self.indexs.read().unwrap();

        for (k, _ids) in mtp.iter() {
            if let Some(prev_ids) = indexs.memory_get_ref(k) {
                let prev = prev_ids.read().unwrap();
                cur_dels.extend(prev.iter().copied());
            }
        }

        drop(indexs);

        let mut indexs = self.indexs.write().unwrap();

        for (k, ids) in mtp {
            indexs.insert(k, ids);
        }

        Ok(cur_dels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::field::FieldOption;

    #[test]
    fn test_tree_writer_reader() {
        use mem_btree::persist::{TreeReader, TreeWriter};
        use roaring::RoaringBitmap;
        use std::path::{Path, PathBuf};
        use std::sync::Arc;

        let persist_path = "/tmp/test_tree_direct";
        let _ = std::fs::remove_dir_all(persist_path);

        // 1. 写入数据
        {
            let writer = TreeWriter::new(PathBuf::from(persist_path), 128, 8); // i64 = 8 bytes
            let serializer = I64RoaringSerializer::default();

            let data = vec![
                Arc::new((
                    10i64,
                    RoaringBitmap::from_sorted_iter([1, 2, 3].iter().copied()).unwrap(),
                    None,
                )),
                Arc::new((
                    20i64,
                    RoaringBitmap::from_sorted_iter([4, 5, 6].iter().copied()).unwrap(),
                    None,
                )),
            ];

            writer
                .persist::<i64, RoaringBitmap, RoaringBitmap>(
                    2,
                    Box::new(serializer),
                    data.into_iter(),
                )
                .expect("Failed to persist");
        }

        // 2. 检查文件是否创建
        assert!(Path::new(persist_path).join("node").exists());
        assert!(Path::new(persist_path).join("data").exists());

        // 3. 读取数据
        {
            use mem_btree::persist::TreeReader;

            let reader: TreeReader<i64, RoaringBitmap> = TreeReader::new(
                Path::new(persist_path),
                Box::new(I64RoaringSerializer::default()),
            )
            .expect("Failed to open reader");

            println!("Reader len: {}", reader.len());

            // 读取第一个值
            let bitmap1 = reader.get(&10);
            println!("bitmap1: {:?}", bitmap1);
            if let Some(bitmap) = bitmap1 {
                println!("bitmap1 len: {}", bitmap.len());
                assert_eq!(bitmap.len(), 3);
                assert!(bitmap.contains(1));
                assert!(bitmap.contains(2));
                assert!(bitmap.contains(3));
            } else {
                panic!("bitmap1 is None");
            }

            // 读取第二个值
            let bitmap2 = reader.get(&20).expect("Should find key 20");
            assert_eq!(bitmap2.len(), 3);
            assert!(bitmap2.contains(4));
            assert!(bitmap2.contains(5));
            assert!(bitmap2.contains(6));

            // 读取不存在的键
            assert!(reader.get(&30).is_none());
        }

        // 清理
        let _ = std::fs::remove_dir_all(persist_path);
    }

    #[test]
    fn test_num_i64_persist_and_read() {
        // 1. 创建一个内存中的 NumI64 索引
        let field = FieldOption::I64 {
            name: "age".to_string(),
            index: true,
        };

        let num_i64 = NumI64::new(&field);

        // 2. 写入一些测试数据
        {
            let mut indexs = num_i64.indexs.write().unwrap();
            indexs.insert(18, vec![1, 2, 3]);
            indexs.insert(25, vec![4, 5]);
            indexs.insert(30, vec![6, 7, 8, 9]);
            indexs.insert(45, vec![10]);
        }

        // 3. 验证内存索引
        {
            let indexs = num_i64.indexs.read().unwrap();
            assert_eq!(indexs.len(), 4);
            let bitmap = indexs.get_bitmap(&25).unwrap();
            assert_eq!(bitmap.len(), 2);
            assert!(bitmap.contains(4));
            assert!(bitmap.contains(5));
        }

        // 4. 持久化到磁盘
        let persist_path = "/tmp/test_num_i64_persist";
        let _ = std::fs::remove_dir_all(persist_path);

        let disk_num_i64 = num_i64.persist(persist_path).unwrap();

        // 5. 验证磁盘索引
        {
            let indexs = disk_num_i64.indexs.read().unwrap();
            assert_eq!(indexs.len(), 4);

            // 测试读取单个值
            let bitmap = indexs.get_bitmap(&30).unwrap();
            assert_eq!(bitmap.len(), 4);
            assert!(bitmap.contains(6));
            assert!(bitmap.contains(7));
            assert!(bitmap.contains(8));
            assert!(bitmap.contains(9));

            // 测试读取不存在的键
            assert!(indexs.get_bitmap(&100).is_none());
        }

        // 6. 清理
        let _ = std::fs::remove_dir_all(persist_path);

        println!("✅ NumI64 persist and read test passed!");
    }

    #[test]
    fn test_serializer_compatibility() {
        // 测试新的 WriteSerializer + ReadSerializer 模式
        use mem_btree::persist::TreeWriter;
        use std::path::Path;

        let persist_path = "/tmp/test_i64_serializer";
        let _ = std::fs::remove_dir_all(persist_path);

        // 1. 使用 WriteSerializer 写入数据
        {
            let serializer = I64RoaringSerializer::default();
            let writer = TreeWriter::new(std::path::PathBuf::from(persist_path), 128, 8);

            let data = vec![
                Arc::new((
                    10i64,
                    RoaringBitmap::from_sorted_iter([1, 2, 3].iter().copied()).unwrap(),
                    None,
                )),
                Arc::new((
                    20i64,
                    RoaringBitmap::from_sorted_iter([4, 5, 6].iter().copied()).unwrap(),
                    None,
                )),
                Arc::new((
                    30i64,
                    RoaringBitmap::from_sorted_iter([7, 8, 9].iter().copied()).unwrap(),
                    None,
                )),
            ];

            writer
                .persist::<i64, RoaringBitmap, RoaringBitmap>(
                    3,
                    Box::new(serializer),
                    data.into_iter(),
                )
                .expect("Failed to persist");
        }

        // 2. 使用 ReadSerializer 读取数据
        {
            use mem_btree::persist::TreeReader;

            let reader: TreeReader<i64, RoaringBitmap> = TreeReader::new(
                Path::new(persist_path),
                Box::new(I64RoaringSerializer::default()),
            )
            .expect("Failed to open reader");

            assert_eq!(reader.len(), 3);

            // 读取第一个值
            let bitmap = reader.get(&10).expect("Key 10 should exist");
            assert_eq!(bitmap.len(), 3);
            assert!(bitmap.contains(1));
            assert!(bitmap.contains(2));
            assert!(bitmap.contains(3));

            // 读取第二个值
            let bitmap = reader.get(&20).expect("Key 20 should exist");
            assert_eq!(bitmap.len(), 3);
            assert!(bitmap.contains(4));
            assert!(bitmap.contains(5));

            // 读取不存在的键
            assert!(reader.get(&100).is_none());
        }

        // 3. 清理
        let _ = std::fs::remove_dir_all(persist_path);

        println!("✅ Serializer compatibility test passed!");
    }
}
