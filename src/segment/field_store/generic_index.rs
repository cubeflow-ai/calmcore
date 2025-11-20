/// 通用的索引字段实现
///
/// 通过泛型统一了 Keyword, NumI64, NumF64, NumU32, NumI32 等的实现
///
/// # 设计思路
///
/// 所有这些字段类型的核心逻辑都是：
/// 1. 维护一个 K -> Vec<doc_id> 的倒排索引
/// 2. 支持内存和磁盘两种存储方式
/// 3. 支持持久化操作
///
/// 通过 `IndexKey` trait 抽象了不同类型的特定行为
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    hash::Hash,
    marker::PhantomData,
    sync::{Arc, RwLock},
};

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use roaring::RoaringBitmap;

use crate::{
    partition::WriteInfo,
    schema::field::{FieldOption, FieldType},
    segment::field_store::{IndexReader, IndexWriter, InvertedIndex, PkWriter},
    utils::error::{CoreError, CoreResult},
};

/// 索引键需要实现的 trait
///
/// 这个 trait 定义了一个类型如何作为索引的键
pub trait IndexKey: Clone + Ord + Hash + Eq + Send + Sync + 'static {
    /// 序列化器类型（关联类型）
    type Serializer: mem_btree::persist::ReadSerializer<Self, RoaringBitmap>
        + mem_btree::persist::WriteSerializer<Self, RoaringBitmap>
        + Clone
        + 'static;

    /// 从 Arrow ArrayRef 中提取值的迭代器
    /// 返回 (row_index, key_value) 的迭代器
    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_>;

    /// 从 ScalarValue 转换为 Self
    fn from_scalar(value: &datafusion::scalar::ScalarValue) -> Option<Self>;

    /// 创建序列化器实例
    fn new_serializer(zstd_level: i32) -> Self::Serializer;

    /// 获取键的固定长度（0 表示可变长度）
    fn key_len() -> usize;

    /// 标准化键（例如 String 的大小写转换）
    ///
    /// 每个类型都必须实现此方法
    /// - 数字类型：直接返回 *self（Copy trait）
    /// - String：根据 case_sensitive 返回原值或小写版本
    fn normalize(&self, case_sensitive: bool) -> Self;
    /// 是否支持范围查询
    fn supports_range() -> bool {
        true
    }
}

/// 泛型索引字段
///
/// K: 索引的键类型（String, i64, f64, u32, i32 等）
pub struct GenericIndexedField<K: IndexKey> {
    field: FieldOption,
    indexs: RwLock<InvertedIndex<K>>,
    _phantom: PhantomData<K>,
}

impl<K: IndexKey> Clone for GenericIndexedField<K> {
    fn clone(&self) -> Self {
        Self {
            field: self.field.clone(),
            indexs: RwLock::new(self.indexs.read().unwrap().clone()),
            _phantom: PhantomData,
        }
    }
}

impl<K: IndexKey> GenericIndexedField<K> {
    /// 创建新的内存索引
    pub fn new(field: &FieldOption) -> Self {
        Self {
            field: field.clone(),
            indexs: RwLock::new(InvertedIndex::new_memory(64)),
            _phantom: PhantomData,
        }
    }

    /// 从磁盘加载索引
    pub fn from_disk(field: &FieldOption, field_path: &str) -> CoreResult<Self> {
        let zstd_level = field.zstd_level();
        let inverted_index = InvertedIndex::new_disk(field_path, K::new_serializer(zstd_level))?;

        Ok(Self {
            field: field.clone(),
            indexs: RwLock::new(inverted_index),
            _phantom: PhantomData,
        })
    }

    /// 持久化到磁盘
    pub fn persist(&self, path: &str) -> CoreResult<Self> {
        use mem_btree::persist::TreeWriter;

        let persist_opt = self.field.persist_option();
        let zstd_level = persist_opt.zstd_level;
        let chunk_size = persist_opt.chunk_size;

        // 1. 提取内存索引
        let memory_index = self.indexs.read().unwrap();
        let btree = match &*memory_index {
            InvertedIndex::Memory(tree) => tree,
            InvertedIndex::Disk(_) => {
                return Err(CoreError::Internal("Cannot persist disk index".to_string()));
            }
        };

        // 2. 转换为 (K, RoaringBitmap) 的迭代器
        let len = btree.len();
        let iter = btree.iter().map(|item| {
            let (key, value_lock, _ttl) = &*item;
            let ids = value_lock.read().unwrap();
            let bitmap = RoaringBitmap::from_sorted_iter(ids.iter().copied()).unwrap();
            Arc::new((key.clone(), bitmap, None))
        });

        // 3. 持久化
        let serializer = K::new_serializer(zstd_level);
        let writer = TreeWriter::new(std::path::PathBuf::from(path), chunk_size);

        writer
            .persist::<K, RoaringBitmap, RoaringBitmap>(len, Box::new(serializer), None, iter)
            .map_err(|e| CoreError::IOError(e.to_string()))?;

        // 4. 创建磁盘索引
        let disk_index = InvertedIndex::new_disk(path, K::new_serializer(zstd_level))?;

        Ok(Self {
            field: self.field.clone(),
            indexs: RwLock::new(disk_index),
            _phantom: PhantomData,
        })
    }

    /// 查询（用于普通字段）
    #[allow(dead_code)]
    pub fn query(&self, key: &K) -> Option<RoaringBitmap> {
        let normalized_key = key.normalize(self.field.case_sensitive());
        let indexs = self.indexs.read().unwrap();
        indexs.get_bitmap(&normalized_key)
    }
}

impl<K: IndexKey> IndexWriter for GenericIndexedField<K> {
    fn write(&self, data: &RecordBatch, start_id: u32) -> CoreResult<()> {
        let Some(arr) = data.column_by_name(&self.field.name()) else {
            return Ok(());
        };

        let mut mtp: HashMap<K, Vec<u32>> = HashMap::new();

        // 提取键值对并构建临时索引
        for (row_idx, key) in K::extract_from_array(arr) {
            let id = start_id + row_idx as u32;
            let normalized_key = (&key).normalize(self.field.case_sensitive());

            if let Some(list) = mtp.get_mut(&normalized_key) {
                if list.last() != Some(&id) {
                    list.push(id);
                }
            } else {
                mtp.insert(normalized_key, vec![id]);
            }
        }

        // 批量更新索引
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
        self.field.field_type()
    }

    fn mget_internal_id(&self, column: &ArrayRef) -> Vec<u32> {
        let mut result_ids = Vec::new();
        let indexs = self.indexs.read().unwrap();

        for (_row_idx, key) in K::extract_from_array(column) {
            let normalized_key = (&key).normalize(self.field.case_sensitive());
            if let Some(bitmap) = indexs.get_bitmap(&normalized_key) {
                result_ids.extend(bitmap.iter());
            }
        }

        result_ids
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<K: IndexKey> PkWriter for GenericIndexedField<K> {
    fn write_pk(
        &self,
        data: &RecordBatch,
        start_id: u32,
        info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<HashSet<u32>> {
        use crate::arrow_downcast;
        use datafusion::arrow::array::UInt32Array;

        let mut cur_dels = HashSet::new();

        let pk = data.column_by_name(self.field.name()).ok_or_else(|| {
            CoreError::Internal(format!(
                "field:{:?} not found in recordbatch",
                self.field.name()
            ))
        })?;

        let mut mtp: HashMap<K, Vec<u32>> = HashMap::new();

        // 构建主键映射并收集需要删除的旧文档
        // 使用start_id + row_idx 生成文档ID
        for (row_idx, key) in K::extract_from_array(pk) {
            let id = start_id + row_idx as u32;
            let normalized_key = (&key).normalize(self.field.case_sensitive());
            if let Some(list) = mtp.get_mut(&normalized_key) {
                if !list.is_empty() {
                    cur_dels.extend(list.clone());
                }
                list[0] = id;
            } else {
                mtp.insert(normalized_key, vec![id]);
            }
        }

        let _lock = lock.write().unwrap();

        // 更新索引并收集旧值
        let mut indexs = self.indexs.read().unwrap().clone();
        for (k, ids) in mtp {
            if let Some(old) = indexs.insert(k, ids) {
                cur_dels.extend(old.1.read().unwrap().iter());
            }
        }

        // 标记其他 segment 中的删除
        if let Some(WriteInfo(segments_and_ids)) = info {
            for (segment, ids) in segments_and_ids {
                segment.mark_del(ids);
            }
        }

        *self.indexs.write().unwrap() = indexs;

        Ok(cur_dels)
    }
}

impl<K: IndexKey> IndexReader for GenericIndexedField<K> {
    fn name(&self) -> &str {
        self.field.name()
    }

    fn field_type(&self) -> FieldType {
        self.field.field_type()
    }

    fn query(&self, value: &datafusion::scalar::ScalarValue) -> Option<RoaringBitmap> {
        K::from_scalar(value).and_then(|key| {
            let normalized_key = (&key).normalize(self.field.case_sensitive());
            let indexs = self.indexs.read().unwrap();
            indexs.get_bitmap(&normalized_key)
        })
    }

    fn range(
        &self,
        start: &datafusion::scalar::ScalarValue,
        start_inclusive: bool,
        end: &datafusion::scalar::ScalarValue,
        end_inclusive: bool,
    ) -> Option<RoaringBitmap> {
        if !K::supports_range() {
            return None;
        }

        // 处理 NULL 作为无界的情况
        let start_key = if matches!(start, datafusion::scalar::ScalarValue::Null) {
            None
        } else {
            Some(K::from_scalar(start)?)
        };

        let end_key = if matches!(end, datafusion::scalar::ScalarValue::Null) {
            None
        } else {
            Some(K::from_scalar(end)?)
        };

        let indexs = self.indexs.read().unwrap();
        let result = indexs.range_query(
            start_key.as_ref(),
            start_inclusive,
            end_key.as_ref(),
            end_inclusive,
        );
        Some(result)
    }

    fn range_union(
        &self,
        start: &datafusion::scalar::ScalarValue,
        start_inclusive: bool,
        end: &datafusion::scalar::ScalarValue,
        end_inclusive: bool,
    ) -> Option<RoaringBitmap> {
        if !K::supports_range() {
            return None;
        }

        // 处理 NULL 作为无界的情况
        let start_key = if matches!(start, datafusion::scalar::ScalarValue::Null) {
            None
        } else {
            Some(K::from_scalar(start)?)
        };

        let end_key = if matches!(end, datafusion::scalar::ScalarValue::Null) {
            None
        } else {
            Some(K::from_scalar(end)?)
        };

        let indexs = self.indexs.read().unwrap();
        // 尝试使用底层 mem_btree 的 range_union() 优化
        // 如果不支持(内存索引)，返回 None 让调用方回退到 range()
        indexs.range_union(
            start_key.as_ref(),
            start_inclusive,
            end_key.as_ref(),
            end_inclusive,
        )
    }
}

// 注意：类型别名已在 mod.rs 中定义并导出
