use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use datafusion::arrow::array::{ArrayRef, ListArray, RecordBatch, StringArray, UInt32Array};
use roaring::RoaringBitmap;

use crate::{
    arrow_downcast,
    partition::WriteInfo,
    schema::field::{FieldOption, FieldType},
    segment::field_store::{IndexReader, IndexWriter, InvertedIndex, PkWriter},
    utils::error::{CoreError, CoreResult},
};

pub struct Keyword {
    field: FieldOption,
    indexs: RwLock<super::InvertedIndex<String>>,
}

impl Clone for Keyword {
    fn clone(&self) -> Self {
        Self {
            field: self.field.clone(),
            indexs: RwLock::new(self.indexs.read().unwrap().clone()),
        }
    }
}

impl Keyword {
    pub fn new(field: &FieldOption) -> Self {
        Self {
            field: field.clone(),
            indexs: RwLock::new(InvertedIndex::new_memory(64)),
        }
    }

    /// Create a Keyword from disk-based inverted index
    pub fn from_disk(field: &FieldOption, field_path: &str) -> CoreResult<Self> {
        match field {
            FieldOption::Keyword { .. } => {
                // 使用 persist_option 获取 zstd_level
                let zstd_level = field.zstd_level();

                // Create disk-based Keyword
                let inverted_index = InvertedIndex::new_disk(
                    &field_path,
                    super::StringRoaringSerializer::new(zstd_level),
                )?;

                Ok(Self {
                    field: field.clone(),
                    indexs: RwLock::new(inverted_index),
                })
            }
            _ => {
                return Err(CoreError::Internal(
                    "FieldOption must be Keyword".to_string(),
                ));
            }
        }
    }

    /// Persist the in-memory index to disk and return a new Keyword with disk-based index
    pub fn persist(&self, path: &str) -> CoreResult<Self> {
        use super::StringRoaringSerializer;
        use mem_btree::persist::TreeWriter;

        // 获取持久化配置
        let persist_opt = self.field.persist_option();
        let zstd_level = persist_opt.zstd_level;
        let chunk_size = persist_opt.chunk_size;

        // 1. Extract the memory index
        let memory_index = self.indexs.read().unwrap();
        let btree = match &*memory_index {
            InvertedIndex::Memory(tree) => tree,
            InvertedIndex::Disk(_) => {
                return Err(CoreError::Internal("Cannot persist disk index".to_string()));
            }
        };

        // 2. Convert BTree<String, Vec<u32>> to Vec of (String, RoaringBitmap) wrapped in Arc
        let len = btree.len();
        let iter = btree.iter().map(|item| {
            let (key, value_lock, _ttl) = &*item;
            let ids = value_lock.read().unwrap();
            let bitmap = RoaringBitmap::from_sorted_iter(ids.iter().copied()).unwrap();
            Arc::new((key.clone(), bitmap, None))
        });

        // 3. Persist to disk with configured parameters
        let serializer = StringRoaringSerializer::new(zstd_level);
        let writer = TreeWriter::new(
            std::path::PathBuf::from(path),
            chunk_size,
            0, // key_len (0 means variable-length keys)
        );

        writer
            .persist::<String, RoaringBitmap, RoaringBitmap>(len, Box::new(serializer), iter)
            .map_err(|e| CoreError::IOError(e.to_string()))?;

        // 4. Create new Keyword with disk index
        let disk_index = InvertedIndex::new_disk(path, StringRoaringSerializer::default())?;

        Ok(Self {
            field: self.field.clone(),
            indexs: RwLock::new(disk_index),
        })
    }

    /// 根据 case_sensitive 配置处理字符串
    /// 如果不区分大小写，返回小写版本；否则返回原字符串
    #[inline]
    fn normalize_string(&self, s: &str) -> String {
        if self.field.case_sensitive() {
            s.to_string()
        } else {
            s.to_lowercase()
        }
    }

    /// 查询关键词对应的文档 ID 集合
    /// 会根据 case_sensitive 配置自动处理查询字符串
    pub fn query(&self, key: &str) -> Option<RoaringBitmap> {
        let normalized_key = self.normalize_string(key);
        let indexs = self.indexs.read().unwrap();
        indexs.get_bitmap(&normalized_key)
    }
}

impl IndexWriter for Keyword {
    fn write(&self, data: &RecordBatch, start_id: u32) -> CoreResult<()> {
        let Some(arr) = data.column_by_name(&self.field.name()) else {
            return Ok(());
        };

        let mut mtp: HashMap<String, Vec<u32>> = HashMap::new();

        if self.field.is_array() {
            // 对于数组字段,使用start_id + row_idx生成文档ID
            for (row_idx, a) in arrow_downcast!(arr, ListArray).iter().enumerate() {
                if let Some(a) = a {
                    let id = start_id + row_idx as u32;
                    for v in arrow_downcast!(a, StringArray).iter().filter_map(|v| v) {
                        let normalized_key = self.normalize_string(v);
                        if let Some(list) = mtp.get_mut(&normalized_key) {
                            if list.last() != Some(&id) {
                                list.push(id);
                            }
                        } else {
                            mtp.insert(normalized_key, vec![id]);
                        }
                    }
                }
            }
        } else {
            // 对于普通字段,使用start_id + row_idx生成文档ID
            for (row_idx, a) in arrow_downcast!(arr, StringArray).iter().enumerate() {
                if let Some(v) = a {
                    let id = start_id + row_idx as u32;
                    let normalized_key = self.normalize_string(v);
                    if let Some(list) = mtp.get_mut(&normalized_key) {
                        if list.last() != Some(&id) {
                            list.push(id);
                        }
                    } else {
                        mtp.insert(normalized_key, vec![id]);
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

    fn mget_internal_id(&self, column: &ArrayRef) -> Vec<u32> {
        use datafusion::arrow::array::Array;

        // BloomFilter 已经在 Segment::mget_internal_id 中用于快速过滤 segment 了
        // 这里直接从倒排索引查询文档 ID

        // 将 column 转换为 StringArray
        let string_array = arrow_downcast!(column, StringArray);

        let mut result_ids = Vec::new();
        let indexs = self.indexs.read().unwrap();

        // 对每个主键值查询其内部 ID
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                continue;
            }

            // 标准化字符串（根据 case_sensitive 配置）
            let key = self.normalize_string(string_array.value(i));

            // 在倒排索引中查找这个 key
            if let Some(bitmap) = indexs.get_bitmap(&key) {
                // 将结果添加到返回列表（这里的 bitmap 存储的就是文档 ID）
                result_ids.extend(bitmap.iter());
            }
        }

        result_ids
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

        let pk = data.column_by_name(self.field.name()).ok_or_else(|| {
            CoreError::Internal(format!(
                "field:{:?} not found in recordbatch",
                self.field.name()
            ))
        })?;

        let mut mtp: HashMap<String, Vec<u32>> = HashMap::new();

        // set mtp and cur_dels
        let internal_id_array = arrow_downcast!(data.column(0), UInt32Array);

        for (a, id) in arrow_downcast!(pk, StringArray)
            .iter()
            .zip(internal_id_array.iter())
        {
            if let (Some(v), Some(id)) = (a, id) {
                let normalized_key = self.normalize_string(v);
                if let Some(list) = mtp.get_mut(&normalized_key) {
                    if !list.is_empty() {
                        cur_dels.extend(list.clone());
                    }
                    list[0] = id;
                } else {
                    mtp.insert(normalized_key, vec![id]);
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
        if let Some(WriteInfo(segments_and_ids)) = info {
            for (segment, ids) in segments_and_ids {
                segment.mark_del(ids);
            }
        }

        *self.indexs.write().unwrap() = indexs;

        Ok(cur_dels)
    }
}

impl IndexReader for Keyword {
    fn name(&self) -> &str {
        self.field.name()
    }

    fn field_type(&self) -> FieldType {
        FieldType::Keyword
    }

    fn query(&self, value: &datafusion::scalar::ScalarValue) -> Option<RoaringBitmap> {
        use datafusion::scalar::ScalarValue;

        match value {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                let normalized_key = self.normalize_string(s);
                let indexs = self.indexs.read().unwrap();
                indexs.get_bitmap(&normalized_key)
            }
            _ => None,
        }
    }

    fn range(
        &self,
        _start: &datafusion::scalar::ScalarValue,
        _start_inclusive: bool,
        _end: &datafusion::scalar::ScalarValue,
        _end_inclusive: bool,
    ) -> Option<RoaringBitmap> {
        // Keyword fields don't support range queries
        // String comparison ranges are typically not useful for keyword fields
        None
    }
}
