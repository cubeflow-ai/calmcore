pub mod field_store;

// Re-export field_store types that are used publicly
pub use field_store::{IndexReader, IndexWriter, RowDataStore};

use crate::{
    partition::WriteInfo,
    schema::{field::FieldOption, Schema},
    segment::field_store::{
        BooleanField, F32Field, F64Field, I16Field, I32Field, I64Field, I8Field, KeywordField,
        PkWriter, U16Field, U32Field, U64Field, U8Field,
    },
    utils::error::{CoreError, CoreResult},
};
use bloomfilter::Bloom;
use datafusion::arrow::{
    self as arrow,
    array::{ArrayRef, RecordBatch},
};
use itertools::Itertools;
use roaring::RoaringBitmap;
use std::{
    collections::HashMap,
    io::Read,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, RwLock,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

/// 控制非主键字段的索引写入模式
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub enum FieldIndexMode {
    Sync,
    Async,
}

pub struct Segment {
    pub start: u64,
    doc_id_gen: AtomicU32,
    max_doc_id: AtomicU32,              // 当前已分配的最大文档ID (用于计算end)
    persisted: AtomicBool,              // 是否已持久化到磁盘
    created_at: Instant,                // Segment 创建时间（用于时间阈值判断）
    pk_bloomfilter: RwLock<Bloom<u32>>, // 使用真正的 BloomFilter
    deleted: RwLock<RoaringBitmap>,
    fields: RwLock<Vec<Box<dyn IndexWriter>>>, // 改为 RwLock 以支持替换为 Disk 版本
    field_index: HashMap<String, usize>,
    schema: Arc<Schema>,
    row_data: RwLock<RowDataStore>, // Memory/Disk 内部区分
    base_path: Option<String>,      // 持久化后的基础路径
}

impl Segment {
    pub fn new(start: u64, schema: Arc<Schema>) -> Self {
        let mut fields: Vec<Box<dyn IndexWriter>> = Vec::new();

        for field_opt in &schema.fields {
            match field_opt {
                FieldOption::Keyword { .. } => {
                    let keyword = KeywordField::new(field_opt);
                    fields.push(Box::new(keyword) as Box<dyn IndexWriter>);
                }
                FieldOption::I8 { .. } => {
                    let field = I8Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::I16 { .. } => {
                    let field = I16Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::I32 { .. } => {
                    let field = I32Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::I64 { .. } => {
                    let num_i64 = I64Field::new(field_opt);
                    fields.push(Box::new(num_i64) as Box<dyn IndexWriter>);
                }
                FieldOption::U8 { .. } => {
                    let field = U8Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::U16 { .. } => {
                    let field = U16Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::U32 { .. } => {
                    let field = U32Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::U64 { .. } => {
                    let field = U64Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::F32 { .. } => {
                    let field = F32Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::F64 { .. } => {
                    let num_f64 = F64Field::new(field_opt);
                    fields.push(Box::new(num_f64) as Box<dyn IndexWriter>);
                }
                FieldOption::Boolean { .. } => {
                    let field = BooleanField::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::Timestamp { .. } => {
                    let field = field_store::TimestampField::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
            }
        }

        let field_index = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().to_string(), i))
            .collect();

        // 创建 BloomFilter: 预估每个 segment 最多存储的文档数，误判率 1%
        let expected_items = schema.persist_policy.max_docs_per_segment as usize;
        let bloom = Bloom::new_for_fp_rate(expected_items, 0.01)
            .map_err(|e| CoreError::Internal(format!("Failed to create bloom filter: {}", e)))
            .unwrap();

        Self {
            start,
            doc_id_gen: AtomicU32::new(0),
            max_doc_id: AtomicU32::new(0), // 初始没有文档,所以max_doc_id=0(无效值)
            persisted: AtomicBool::new(false),
            created_at: Instant::now(),
            pk_bloomfilter: RwLock::new(bloom),
            deleted: RwLock::default(),
            fields: RwLock::new(fields),
            field_index,
            schema,
            row_data: RwLock::new(RowDataStore::new_memory(32)),
            base_path: None,
        }
    }

    /// Create a segment from an external Parquet file
    ///
    /// This method creates a segment that references an external Parquet file
    /// without copying the data. It builds indexes for the data in memory while
    /// the actual row data remains in the Parquet file.
    ///
    /// # Arguments
    /// * `start` - Starting document ID for this segment
    /// * `end` - Ending document ID for this segment (inclusive)
    /// * `parquet_path` - Path to the Parquet file
    /// * `data` - RecordBatch containing the data (used to build indexes)
    /// * `schema` - Schema for the segment
    ///
    /// # Returns
    /// A new Segment that references the Parquet file
    pub fn from_parquet(
        start: u64,
        end: u64,
        parquet_path: &str,
        data: &RecordBatch,
        schema: Arc<Schema>,
    ) -> CoreResult<Self> {
        let num_rows = data.num_rows() as u32;

        if num_rows == 0 {
            return Err(CoreError::InvalidParam(
                "Cannot create segment from empty data".to_string(),
            ));
        }

        println!("  🔧 Creating segment from Parquet with {} rows", num_rows);

        // 1. Create empty field indexes
        let mut fields: Vec<Box<dyn IndexWriter>> = Vec::new();

        for field_opt in &schema.fields {
            match field_opt {
                FieldOption::Keyword { .. } => {
                    let keyword = KeywordField::new(field_opt);
                    fields.push(Box::new(keyword) as Box<dyn IndexWriter>);
                }
                FieldOption::I8 { .. } => {
                    let field = I8Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::I16 { .. } => {
                    let field = I16Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::I32 { .. } => {
                    let field = I32Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::I64 { .. } => {
                    let num_i64 = I64Field::new(field_opt);
                    fields.push(Box::new(num_i64) as Box<dyn IndexWriter>);
                }
                FieldOption::U8 { .. } => {
                    let field = U8Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::U16 { .. } => {
                    let field = U16Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::U32 { .. } => {
                    let field = U32Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::U64 { .. } => {
                    let field = U64Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::F32 { .. } => {
                    let field = F32Field::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::F64 { .. } => {
                    let num_f64 = F64Field::new(field_opt);
                    fields.push(Box::new(num_f64) as Box<dyn IndexWriter>);
                }
                FieldOption::Boolean { .. } => {
                    let field = BooleanField::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::Timestamp { .. } => {
                    let field = field_store::TimestampField::new(field_opt);
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
            }
        }

        // 2. Build field index
        let field_index: HashMap<String, usize> = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().to_string(), i))
            .collect();

        // 3. Create BloomFilter and build indexes
        let expected_items = num_rows as usize;
        let mut bloom = Bloom::new_for_fp_rate(expected_items, 0.01)
            .map_err(|e| CoreError::Internal(format!("Failed to create bloom filter: {}", e)))?;

        // 4. Build indexes for all fields
        for field in fields.iter() {
            if let Err(e) = field.write(data, 0) {
                println!(
                    "  ⚠️  Warning: Failed to build index for field {}: {:?}",
                    field.name(),
                    e
                );
            } else {
                println!("    ✓ Built index for field: {}", field.name());
            }
        }

        // 5. Build primary key bloom filter if needed
        if let Some(pk_name) = &schema.primary_key {
            if let Some(&pk_idx) = field_index.get(pk_name) {
                use datafusion::arrow::array::Array;
                if let Some(pk_column) = data
                    .column(pk_idx)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                {
                    for i in 0..pk_column.len() {
                        if let Some(pk_value) = pk_column
                            .value(i)
                            .as_bytes()
                            .get(0..std::cmp::min(pk_column.value(i).len(), 32))
                        {
                            use std::hash::{Hash, Hasher};
                            let mut hasher = std::collections::hash_map::DefaultHasher::new();
                            pk_value.hash(&mut hasher);
                            let hash = hasher.finish() as u32;
                            bloom.set(&hash);
                        }
                    }
                    println!("    ✓ Built bloom filter for primary key: {}", pk_name);
                }
            }
        }

        // 6. Create RowDataStore that references the Parquet file
        let row_data = RowDataStore::new_parquet(parquet_path)?;

        // Calculate doc_id_gen and max_doc_id
        let max_doc_id_relative = (end - start) as u32;
        let doc_id_gen = max_doc_id_relative + 1;

        println!(
            "  ✓ Segment created: doc_id_gen={}, max_doc_id={}",
            doc_id_gen, max_doc_id_relative
        );

        Ok(Self {
            start,
            doc_id_gen: AtomicU32::new(doc_id_gen),
            max_doc_id: AtomicU32::new(max_doc_id_relative),
            persisted: AtomicBool::new(false), // 索引还未持久化，需要持久化
            created_at: Instant::now(),
            pk_bloomfilter: RwLock::new(bloom),
            deleted: RwLock::default(),
            fields: RwLock::new(fields),
            field_index,
            schema,
            row_data: RwLock::new(row_data),
            base_path: Some(parquet_path.to_string()),
        })
    }

    pub fn write(
        &self,
        data: &RecordBatch,
        pk_hash: Option<Vec<u32>>,
        info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<Vec<u32>> {
        // 🔒 检查是否已持久化 - 持久化后的 Segment 不允许写入
        if self.persisted.load(Ordering::Relaxed) {
            return Err(CoreError::Internal(
                "Cannot write to persisted segment".to_string(),
            ));
        }

        // 直接使用原始数据,不添加 _internal_id 列
        let num_rows = data.num_rows() as u32;
        // generate auto-increment id for tracking
        let start_id = self.doc_id_gen.load(Ordering::Relaxed);

        let result = (start_id..start_id + num_rows).collect_vec();

        // 使用原始数据,不需要添加id列
        let new_data = data.clone(); // 更新doc_id_gen和max_doc_id
        let new_gen = start_id + num_rows;
        self.doc_id_gen.store(new_gen, Ordering::Relaxed);

        // max_doc_id是最后一个文档的ID (不是下一个可用ID)
        if num_rows > 0 {
            let last_doc_id = start_id + num_rows - 1;
            self.max_doc_id.store(last_doc_id, Ordering::Relaxed);
        }

        if let Some(index) = self
            .schema
            .primary_key
            .as_ref()
            .and_then(|k| self.field_index.get(k))
        {
            let fields = self.fields.read().unwrap();
            let pk_field: &dyn IndexWriter = &*fields[*index];
            self.write_pk(pk_field, &new_data, start_id, pk_hash, info, lock)?;
        }

        // 使用 RowDataStore 的 put 方法存储 RecordBatch
        self.row_data
            .write()
            .unwrap()
            .put(start_id, new_data.clone());

        let pk_name = self.schema.primary_key.as_deref();
        let fields = self.fields.read().unwrap();
        for f in fields.iter() {
            if let Some(pk) = pk_name {
                if f.name() == pk {
                    continue;
                }
            }
            if let Err(e) = f.write(&new_data, start_id) {
                log::error!("index write field {:?} failed: {:?}", f.name(), e);
            }
        }

        Ok(result)
    }

    fn write_pk(
        &self,
        pk_field: &dyn IndexWriter,
        data: &RecordBatch,
        start_id: u32,
        pk_hash: Option<Vec<u32>>,
        info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<()> {
        use crate::schema::field::FieldType;

        match pk_field.field_type() {
            FieldType::Keyword => {
                let pk_writer = pk_field
                    .as_any()
                    .downcast_ref::<KeywordField>()
                    .ok_or_else(|| {
                        CoreError::Internal(format!(
                            "field:{:?} field_type:{:?} does not implement PkWriter",
                            pk_field.name(),
                            pk_field.field_type()
                        ))
                    })?;

                let del = pk_writer.write_pk(data, start_id, info, lock)?;
                if !del.is_empty() {
                    self.deleted.write().unwrap().extend(del.iter());
                }

                // 将主键哈希值插入 BloomFilter
                if let Some(hashes) = pk_hash {
                    let mut bloom = self.pk_bloomfilter.write().unwrap();
                    for hash in hashes {
                        bloom.set(&hash);
                    }
                }
            }
            FieldType::U32 => {
                let pk_writer = pk_field
                    .as_any()
                    .downcast_ref::<U32Field>()
                    .ok_or_else(|| {
                        CoreError::Internal(format!(
                            "field:{:?} field_type:{:?} does not implement PkWriter",
                            pk_field.name(),
                            pk_field.field_type()
                        ))
                    })?;

                let del = pk_writer.write_pk(data, start_id, info, lock)?;
                if !del.is_empty() {
                    self.deleted.write().unwrap().extend(del.iter());
                }

                // 将主键哈希值插入 BloomFilter
                if let Some(hashes) = pk_hash {
                    let mut bloom = self.pk_bloomfilter.write().unwrap();
                    for hash in hashes {
                        bloom.set(&hash);
                    }
                }
            }
            FieldType::U64 => {
                let pk_writer = pk_field
                    .as_any()
                    .downcast_ref::<U64Field>()
                    .ok_or_else(|| {
                        CoreError::Internal(format!(
                            "field:{:?} field_type:{:?} does not implement PkWriter",
                            pk_field.name(),
                            pk_field.field_type()
                        ))
                    })?;

                let del = pk_writer.write_pk(data, start_id, info, lock)?;
                if !del.is_empty() {
                    self.deleted.write().unwrap().extend(del.iter());
                }

                // 将主键哈希值插入 BloomFilter
                if let Some(hashes) = pk_hash {
                    let mut bloom = self.pk_bloomfilter.write().unwrap();
                    for hash in hashes {
                        bloom.set(&hash);
                    }
                }
            }
            FieldType::I32 => {
                let pk_writer = pk_field
                    .as_any()
                    .downcast_ref::<I32Field>()
                    .ok_or_else(|| {
                        CoreError::Internal(format!(
                            "field:{:?} field_type:{:?} does not implement PkWriter",
                            pk_field.name(),
                            pk_field.field_type()
                        ))
                    })?;

                let del = pk_writer.write_pk(data, start_id, info, lock)?;
                if !del.is_empty() {
                    self.deleted.write().unwrap().extend(del.iter());
                }

                // 将主键哈希值插入 BloomFilter
                if let Some(hashes) = pk_hash {
                    let mut bloom = self.pk_bloomfilter.write().unwrap();
                    for hash in hashes {
                        bloom.set(&hash);
                    }
                }
            }
            FieldType::I64 => {
                let pk_writer = pk_field
                    .as_any()
                    .downcast_ref::<I64Field>()
                    .ok_or_else(|| {
                        CoreError::Internal(format!(
                            "field:{:?} field_type:{:?} does not implement PkWriter",
                            pk_field.name(),
                            pk_field.field_type()
                        ))
                    })?;

                let del = pk_writer.write_pk(data, start_id, info, lock)?;
                if !del.is_empty() {
                    self.deleted.write().unwrap().extend(del.iter());
                }

                // 将主键哈希值插入 BloomFilter
                if let Some(hashes) = pk_hash {
                    let mut bloom = self.pk_bloomfilter.write().unwrap();
                    for hash in hashes {
                        bloom.set(&hash);
                    }
                }
            }
            _ => {
                return Err(CoreError::InvalidParam(format!(
                    "field:{:?} type not support pk: {:?}",
                    pk_field.name(),
                    pk_field.field_type()
                )))
            }
        }

        Ok(())
    }

    /// Get internal ids by primary key hash and primary key column
    /// if not found, return None, never return empty vector
    /// internal id is the row number in the segment + start
    pub fn mget_internal_id(&self, pk_hash: &[u32], column: &ArrayRef) -> Option<Vec<u32>> {
        // 使用 BloomFilter 进行预过滤，快速判断主键是否可能存在于当前 segment
        let bloom = self.pk_bloomfilter.read().unwrap();
        let active = pk_hash
            .iter()
            .enumerate()
            .filter(|(_, v)| bloom.check(v))
            .map(|(i, _)| i as u32) // 转换为 u32
            .collect_vec();
        drop(bloom); // 提前释放 RwLock

        // not found any id in this segment
        if active.is_empty() {
            return None;
        }

        // 使用 arrow::compute::take 根据索引过滤 column
        let indices = arrow::array::UInt32Array::from(active);
        let filtered_column = arrow::compute::take(column.as_ref(), &indices, None)
            .map_err(|e| CoreError::Internal(format!("Failed to take column: {}", e)))
            .ok()?;

        let index = *self
            .field_index
            .get(self.schema.primary_key.as_ref().unwrap())
            .unwrap();

        let ids = {
            let fields = self.fields.read().unwrap();
            let ids = fields[index].mget_internal_id(&filtered_column);

            let del_guard = self.deleted.read().unwrap();

            if del_guard.is_empty() {
                ids
            } else {
                ids.into_iter()
                    .filter_map(|v| if del_guard.contains(v) { None } else { Some(v) })
                    .collect_vec()
            }
        };

        if ids.is_empty() {
            None
        } else {
            Some(ids)
        }
    }

    pub fn mark_del(&self, ids: Vec<u32>) {
        self.deleted.write().unwrap().extend(ids);
    }

    pub(crate) fn total_count(&self) -> u64 {
        // Use doc_id_gen as the total count (works for both active and frozen segments)
        let total = self.doc_id_gen.load(Ordering::Relaxed) as u64;
        total - self.deleted.read().unwrap().len()
    }

    /// Get a single document by internal doc_id
    /// Returns None if document doesn't exist or is deleted
    ///
    /// This implements lazy loading - only loads the RecordBatch containing the requested doc
    pub fn get_document(&self, doc_id: u32) -> Option<RecordBatch> {
        use datafusion::arrow::compute::filter_record_batch;

        // Check if document is deleted
        if self.deleted.read().unwrap().contains(doc_id) {
            return None;
        }

        // Find which RecordBatch contains this doc_id using floor lookup
        // BTree keys are the first doc_id in each batch
        // e.g., keys: [1, 100, 200], query 50 -> finds batch at key 1
        let row_data = self.row_data.read().unwrap();

        // Use floor to find the batch containing this doc_id
        let (start_id, batch) = row_data.floor(&doc_id)?;

        // Calculate row index within the batch
        // doc_id = start_id + row_idx
        let row_idx = doc_id.checked_sub(start_id)? as usize;

        // Check if row_idx is within bounds
        if row_idx >= batch.num_rows() {
            return None;
        }

        // Extract just this row
        let mask: Vec<bool> = (0..batch.num_rows()).map(|i| i == row_idx).collect();
        let mask_array = arrow::array::BooleanArray::from(mask);

        filter_record_batch(&batch, &mask_array).ok()
    }
    /// Get multiple documents by internal doc_ids
    /// Returns a RecordBatch containing only the requested documents
    /// Deleted documents are filtered out
    pub fn get_documents(&self, doc_ids: &[u32]) -> CoreResult<Option<RecordBatch>> {
        use datafusion::arrow::compute::concat_batches;

        let deleted = self.deleted.read().unwrap();

        // Collect individual document batches
        let mut doc_batches: Vec<RecordBatch> = Vec::new();
        let mut schema_ref: Option<Arc<arrow::datatypes::Schema>> = None;

        for doc_id in doc_ids {
            if deleted.contains(*doc_id) {
                continue;
            }

            if let Some(doc_batch) = self.get_document(*doc_id) {
                if schema_ref.is_none() {
                    schema_ref = Some(doc_batch.schema());
                }
                doc_batches.push(doc_batch);
            }
        }

        if doc_batches.is_empty() {
            return Ok(None);
        }

        // Merge all document batches
        let schema = schema_ref.unwrap();
        let merged = concat_batches(&schema, &doc_batches)
            .map_err(|e| CoreError::Internal(format!("Failed to merge batches: {}", e)))?;

        Ok(Some(merged))
    }

    /// Scan all documents in this segment (with optional filter)
    /// Returns an iterator-like result with lazy loading
    /// Deleted documents are automatically filtered out
    pub fn scan_documents(&self) -> CoreResult<Vec<RecordBatch>> {
        let deleted = self.deleted.read().unwrap();
        let row_data = self.row_data.read().unwrap();

        // Collect all non-deleted batches
        let mut batches: Vec<RecordBatch> = Vec::new();

        // Iterate through all batches in row_data
        if let Some(iter) = row_data.iter() {
            for (start_id, batch) in iter {
                // Apply deleted filter to this batch
                let filtered_batch = self.filter_deleted_from_batch(start_id, &batch, &deleted);
                if filtered_batch.num_rows() > 0 {
                    batches.push(filtered_batch);
                }
            }
        }

        Ok(batches)
    }

    /// Filter deleted rows from a RecordBatch
    /// start_id is the first doc_id in the batch
    fn filter_deleted_from_batch(
        &self,
        start_id: u32,
        batch: &RecordBatch,
        deleted: &RoaringBitmap,
    ) -> RecordBatch {
        use datafusion::arrow::compute::filter_record_batch;

        if deleted.is_empty() {
            return batch.clone();
        }

        // Build boolean mask: true = keep, false = filter out
        // doc_id = start_id + row_idx
        let mask: Vec<bool> = (0..batch.num_rows())
            .map(|row_idx| {
                let doc_id = start_id + row_idx as u32;
                !deleted.contains(doc_id)
            })
            .collect();

        // Create boolean array for filtering
        let mask_array = arrow::array::BooleanArray::from(mask);

        // Filter the batch
        filter_record_batch(batch, &mask_array).unwrap_or_else(|_| batch.clone())
    }

    /// Persist segment to disk and convert to Frozen state
    ///
    /// Two-phase commit process:
    /// 1. Write to temp directory: segment-{start}-{end}_tmp/
    /// 2. Atomic rename: segment-{start}-{end}_tmp → segment-{start}-{end}
    /// 3. Replace historical deleted files back to their segments
    ///
    /// Directory structure (during persist):
    /// ```
    /// segment-{start}-{end}_tmp/    # Temporary directory
    ///   ├── field-{name}/           # Inverted indexes
    ///   ├── deleted                 # Current segment's deletes
    ///   ├── deleted_seg_{s1}_{e1}   # Snapshot of segment deletes
    ///   └── rowdata/                # Row data
    ///
    /// After rename and file replacement:
    /// segment-{start}-{end}/        # Final directory
    ///   ├── field-{name}/
    ///   ├── deleted                 # Only this segment's deletes
    ///   └── rowdata/
    /// segment-{s1}-{e1}/deleted ← replaced by deleted_seg_{s1}_{e1}
    /// ```
    ///
    /// # Arguments
    /// * `history_segments` - Previous frozen segments (for snapshot their deleted bitmaps)
    ///
    /// # Returns
    /// Returns Ok(()) on success.
    pub fn recover_from_disk(segment_path: &str, schema: Arc<Schema>) -> CoreResult<Self> {
        println!("Loading frozen segment from: {}", segment_path);

        // 1. Load fields
        let start = std::time::Instant::now();
        let mut fields: Vec<Box<dyn IndexWriter>> = Vec::new();

        for field_opt in &schema.fields {
            let field_name = field_opt.name();
            let field_path = format!("{}/field-{}", segment_path, field_name);

            match field_opt {
                FieldOption::Keyword { .. } => {
                    // Create disk-based Keyword
                    let keyword = KeywordField::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(keyword) as Box<dyn IndexWriter>);
                }
                FieldOption::I8 { .. } => {
                    let field = I8Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::I16 { .. } => {
                    let field = I16Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::I32 { .. } => {
                    let field = I32Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::I64 { .. } => {
                    let num_i64 = I64Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(num_i64) as Box<dyn IndexWriter>);
                }
                FieldOption::U8 { .. } => {
                    let field = U8Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::U16 { .. } => {
                    let field = U16Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::U32 { .. } => {
                    let field = U32Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::U64 { .. } => {
                    let field = U64Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::F32 { .. } => {
                    let field = F32Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::F64 { .. } => {
                    let num_f64 = F64Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(num_f64) as Box<dyn IndexWriter>);
                }
                FieldOption::Boolean { .. } => {
                    let field = BooleanField::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::Timestamp { .. } => {
                    let field = field_store::TimestampField::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
            }
        }

        println!("  Fields loaded in {:?}", start.elapsed());

        // 2. Load bloomfilter
        let pk_path = format!("{}/pk_bloomfilter", segment_path);
        let deleted_path = format!("{}/deleted", segment_path);
        let row_data_path = format!("{}/row_data", segment_path);

        let pk_bloomfilter = if std::path::Path::new(&pk_path).exists() {
            let buffer = std::fs::read(&pk_path)
                .map_err(|e| CoreError::IOError(format!("Failed to read pk_bloomfilter: {}", e)))?;

            // Use bloomfilter's built-in deserialization
            Bloom::from_bytes(buffer).map_err(|e| {
                CoreError::IOError(format!("Failed to deserialize bloom filter: {}", e))
            })?
        } else {
            // 如果不存在，创建一个空的 BloomFilter
            let expected_items = schema.persist_policy.max_docs_per_segment as usize;
            Bloom::new_for_fp_rate(expected_items, 0.01)
                .map_err(|e| CoreError::Internal(format!("Failed to create bloom filter: {}", e)))?
        };

        let deleted = if std::path::Path::new(&deleted_path).exists() {
            let mut buffer = Vec::new();
            std::fs::File::open(&deleted_path)
                .map_err(|e| CoreError::IOError(e.to_string()))?
                .read_to_end(&mut buffer)
                .map_err(|e| CoreError::IOError(e.to_string()))?;
            RoaringBitmap::deserialize_from(&buffer[..])
                .map_err(|e| CoreError::IOError(e.to_string()))?
        } else {
            RoaringBitmap::new()
        };

        // 3. Get start ID and doc count from path
        let file_name = std::path::Path::new(segment_path)
            .file_name()
            .ok_or_else(|| CoreError::IOError("Invalid segment path".to_string()))?
            .to_str()
            .ok_or_else(|| CoreError::IOError("Invalid segment path".to_string()))?;

        let mut parts = file_name.split('-');
        parts.next(); // Skip "segment"
        let start = parts
            .next()
            .ok_or_else(|| CoreError::IOError("Invalid segment path".to_string()))?
            .parse::<u64>()
            .map_err(|e| CoreError::IOError(e.to_string()))?;
        let end = parts
            .next()
            .ok_or_else(|| CoreError::IOError("Invalid segment path".to_string()))?
            .parse::<u64>()
            .map_err(|e| CoreError::IOError(e.to_string()))?;

        let doc_count = (end - start) as u32;

        // 4. Create field name to index map
        let field_index = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().to_string(), i))
            .collect();

        // 5. Load row data (check for Parquet format first, then BTree format)
        let parquet_path = format!("{}/rowdata.parquet", row_data_path);
        let row_data = if std::path::Path::new(&parquet_path).exists() {
            // Parquet file format
            RowDataStore::new_parquet(&parquet_path)?
        } else {
            RowDataStore::new_memory(32)
        };

        Ok(Self {
            start,
            doc_id_gen: AtomicU32::new(doc_count),
            max_doc_id: AtomicU32::new(doc_count),
            persisted: AtomicBool::new(true),
            created_at: Instant::now(), // 加载时使用当前时间（已持久化的 segment 不关心年龄）
            pk_bloomfilter: RwLock::new(pk_bloomfilter),
            deleted: RwLock::new(deleted),
            fields: RwLock::new(fields),
            field_index,
            schema,
            row_data: RwLock::new(row_data),
            base_path: Some(segment_path.to_string()),
        })
    }

    pub fn persist(
        &self,
        base_dir: &str,
        _segment_id: u64, // Deprecated: now using start-end range
        history_segments: &[(u64, Arc<Segment>)],
    ) -> CoreResult<()> {
        // 🔒 设置持久化标记,防止持久化过程中有新的写入
        // 使用 compare_exchange 确保只有第一次调用会成功
        if self
            .persisted
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            // 如果已经持久化，返回成功（幂等操作）
            log::info!("[Segment] Already persisted, skipping");
            return Ok(());
        }

        log::info!("[Segment] persist start - write lock acquired");

        // Calculate segment ID range: start_id to end_id (inclusive)
        // end_id 是最后一个文档的ID,不是下一个可用ID
        let start_id = self.start;
        let end_id = self.start + self.max_doc_id.load(Ordering::Relaxed) as u64;

        // Phase 1: Write to temporary directory
        let segment_tmp_path = format!("{}/segment-{}-{}_tmp", base_dir, start_id, end_id);
        let segment_path = format!("{}/segment-{}-{}", base_dir, start_id, end_id);

        // Clean up any existing temp directory (from previous failed persist)
        let _ = std::fs::remove_dir_all(&segment_tmp_path);

        std::fs::create_dir_all(&segment_tmp_path)
            .map_err(|e| CoreError::IOError(format!("Failed to create temp segment dir: {}", e)))?;

        log::debug!("Persisting segment to: {} (temp)", segment_tmp_path);

        // 1. Persist each field's inverted index (to temp directory)
        let start = std::time::Instant::now();
        let new_fields = {
            let fields = self.fields.read().unwrap();
            let mut new_fields: Vec<Box<dyn IndexWriter>> = Vec::new();

            for field in fields.iter() {
                let field_name = field.name();
                let field_path = format!("{}/field-{}", segment_tmp_path, field_name);

                // Try to downcast and persist based on field type
                if let Some(keyword) = field.as_any().downcast_ref::<KeywordField>() {
                    let disk_field = keyword.persist(&field_path)?;
                    new_fields.push(Box::new(disk_field) as Box<dyn IndexWriter>);
                } else if let Some(num_i8) = field.as_any().downcast_ref::<I8Field>() {
                    let disk_field = num_i8.persist(&field_path)?;
                    new_fields.push(Box::new(disk_field) as Box<dyn IndexWriter>);
                } else if let Some(num_i16) = field.as_any().downcast_ref::<I16Field>() {
                    let disk_field = num_i16.persist(&field_path)?;
                    new_fields.push(Box::new(disk_field) as Box<dyn IndexWriter>);
                } else if let Some(num_i32) = field.as_any().downcast_ref::<I32Field>() {
                    let disk_field = num_i32.persist(&field_path)?;
                    new_fields.push(Box::new(disk_field) as Box<dyn IndexWriter>);
                } else if let Some(num_i64) = field.as_any().downcast_ref::<I64Field>() {
                    let disk_field = num_i64.persist(&field_path)?;
                    new_fields.push(Box::new(disk_field) as Box<dyn IndexWriter>);
                } else if let Some(num_u8) = field.as_any().downcast_ref::<U8Field>() {
                    let disk_field = num_u8.persist(&field_path)?;
                    new_fields.push(Box::new(disk_field) as Box<dyn IndexWriter>);
                } else if let Some(num_u16) = field.as_any().downcast_ref::<U16Field>() {
                    let disk_field = num_u16.persist(&field_path)?;
                    new_fields.push(Box::new(disk_field) as Box<dyn IndexWriter>);
                } else if let Some(num_u32) = field.as_any().downcast_ref::<U32Field>() {
                    let disk_field = num_u32.persist(&field_path)?;
                    new_fields.push(Box::new(disk_field) as Box<dyn IndexWriter>);
                } else if let Some(num_u64) = field.as_any().downcast_ref::<U64Field>() {
                    let disk_field = num_u64.persist(&field_path)?;
                    new_fields.push(Box::new(disk_field) as Box<dyn IndexWriter>);
                } else if let Some(num_f32) = field.as_any().downcast_ref::<F32Field>() {
                    let disk_field = num_f32.persist(&field_path)?;
                    new_fields.push(Box::new(disk_field) as Box<dyn IndexWriter>);
                } else if let Some(num_f64) = field.as_any().downcast_ref::<F64Field>() {
                    let disk_field = num_f64.persist(&field_path)?;
                    new_fields.push(Box::new(disk_field) as Box<dyn IndexWriter>);
                } else if let Some(boolean) = field.as_any().downcast_ref::<BooleanField>() {
                    let disk_field = boolean.persist(&field_path)?;
                    new_fields.push(Box::new(disk_field) as Box<dyn IndexWriter>);
                } else if let Some(timestamp) =
                    field.as_any().downcast_ref::<field_store::TimestampField>()
                {
                    let disk_field = timestamp.persist(&field_path)?;
                    new_fields.push(Box::new(disk_field) as Box<dyn IndexWriter>);
                } else {
                    log::error!(
                        "⚠️ Warning: Field '{}' has unsupported type for persist, skipping",
                        field_name
                    );
                    // Skip unsupported fields but continue with others
                    continue;
                }
            }

            new_fields
        }; // Release read lock here

        // 注意：暂时不替换 fields，等 rename 成功后再替换
        // 否则 rename 失败后无法重试（fields 已经是 Disk 类型）

        // 2. Persist pk_bloomfilter (to temp directory)
        {
            let pk_path = format!("{}/pk_bloomfilter", segment_tmp_path);
            let pk_bloom = self.pk_bloomfilter.read().unwrap();

            // Use bloomfilter's built-in serialization (includes header with k_num and seed)
            let buffer = pk_bloom.to_bytes();

            std::fs::write(&pk_path, buffer).map_err(|e| {
                CoreError::IOError(format!("Failed to write pk_bloomfilter file: {}", e))
            })?;
        }

        // 3. Persist deleted bitmap (current segment, to temp directory)
        {
            let deleted_path = format!("{}/deleted", segment_tmp_path);
            let deleted = self.deleted.read().unwrap();

            let mut file = std::fs::File::create(&deleted_path)
                .map_err(|e| CoreError::IOError(format!("Failed to create deleted file: {}", e)))?;

            deleted.serialize_into(&mut file).map_err(|e| {
                CoreError::IOError(format!("Failed to serialize deleted bitmap: {}", e))
            })?;
        }

        // 3.5. Persist historical segment deletes (snapshot at this moment, to temp directory)
        let history_start = std::time::Instant::now();
        let mut total_history_deletes = 0;
        let mut history_snapshot: Vec<(u64, u64, RoaringBitmap)> = Vec::new(); // (start, end, bitmap)

        for (_seg_id, segment) in history_segments {
            let history_deleted = segment.deleted.read().unwrap().clone();

            if history_deleted.is_empty() {
                continue; // Skip empty deletes
            }

            let hist_start = segment.start;
            let hist_end = segment.start + segment.doc_id_gen.load(Ordering::Relaxed) as u64;

            let history_path = format!(
                "{}/deleted_seg_{}_{}",
                segment_tmp_path, hist_start, hist_end
            );
            let mut file = std::fs::File::create(&history_path).map_err(|e| {
                CoreError::IOError(format!("Failed to create history deleted file: {}", e))
            })?;

            history_deleted.serialize_into(&mut file).map_err(|e| {
                CoreError::IOError(format!("Failed to serialize history deleted bitmap: {}", e))
            })?;

            total_history_deletes += history_deleted.len();
            history_snapshot.push((hist_start, hist_end, history_deleted));
        }

        if total_history_deletes > 0 {
            println!(
                "  Historical deletes persisted ({} entries from {} segments) in {:?}",
                total_history_deletes,
                history_segments.len(),
                history_start.elapsed()
            );
        }

        // 4. Persist row_data (skip for external Parquet references)
        let is_external_parquet = {
            let row_data = self.row_data.read().unwrap();
            matches!(*row_data, RowDataStore::Parquet(_))
        };

        if !is_external_parquet {
            // Only persist row_data for Memory/Disk segments
            let rowdata_path = format!("{}/rowdata", segment_tmp_path);
            std::fs::create_dir_all(&rowdata_path)
                .map_err(|e| CoreError::IOError(format!("Failed to create rowdata dir: {}", e)))?;

            // Clone row_data (fast - microseconds due to pointer implementation)
            let row_data_clone = self.row_data.read().unwrap().clone();
            let deleted = self.deleted.read().unwrap().clone();

            // Persist row data using TreeWriter
            self.persist_row_data(&rowdata_path, row_data_clone, &deleted)?;
        } else {
            // For external Parquet, save the reference path in metadata
            println!("  Skipping row_data persist (external Parquet reference)");
        }

        // 5. Save segment metadata (to temp directory)
        let meta_path = format!("{}/meta.json", segment_tmp_path);
        // 使用 Unix 毫秒时间戳持久化创建时间，便于外部展示
        let created_ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let meta = if is_external_parquet {
            serde_json::json!({
                "start": self.start,
                "doc_id_gen": self.doc_id_gen.load(Ordering::Relaxed),
                "max_doc_id": self.max_doc_id.load(Ordering::Relaxed),
                "created_ts_ms": created_ts_ms,
                "external_parquet": self.base_path.clone(),
            })
        } else {
            serde_json::json!({
                "start": self.start,
                "doc_id_gen": self.doc_id_gen.load(Ordering::Relaxed),
                "max_doc_id": self.max_doc_id.load(Ordering::Relaxed),
                "created_ts_ms": created_ts_ms,
            })
        };
        std::fs::write(&meta_path, meta.to_string())
            .map_err(|e| CoreError::IOError(format!("Failed to write segment meta: {}", e)))?;

        // Phase 2: Atomic rename (only after all data is written)
        if let Err(e) = std::fs::rename(&segment_tmp_path, &segment_path) {
            // 持久化失败，重置标记允许重试
            self.persisted.store(false, Ordering::SeqCst);
            return Err(CoreError::IOError(format!(
                "Failed to rename temp directory: {}. Temp dir preserved for recovery.",
                e
            )));
        }

        // 🔑 关键：只有 rename 成功后才替换 fields 和 row_data
        // 这样 rename 失败时还可以重试
        *self.fields.write().unwrap() = new_fields;

        // Phase 3: Replace historical deleted files back to their segments
        if !history_snapshot.is_empty() {
            println!("  Phase 3: Replacing historical deleted files");

            for (hist_start, hist_end, _deleted_bitmap) in &history_snapshot {
                let source_path =
                    format!("{}/deleted_seg_{}_{}", segment_path, hist_start, hist_end);
                let target_path =
                    format!("{}/segment-{}-{}/deleted", base_dir, hist_start, hist_end);

                // Replace (overwrite) the target deleted file
                if let Err(e) = std::fs::rename(&source_path, &target_path) {
                    // Log error but continue with other files
                    log::error!(
                        "Warning: Failed to replace deleted file for segment {}-{}: {}",
                        hist_start,
                        hist_end,
                        e
                    );
                } else {
                    log::info!("    Replaced segment-{}-{}/deleted ✓", hist_start, hist_end);
                }
            }
        }

        // Phase 4: Replace in-memory row_data with disk-based reader (skip for external Parquet)
        if !is_external_parquet {
            let rowdata_path = format!("{}/rowdata", segment_path);
            let parquet_path = format!("{}/rowdata.parquet", rowdata_path);
            let disk_row_data = if std::path::Path::new(&parquet_path).exists() {
                // Parquet file format
                RowDataStore::new_parquet(&parquet_path)?
            } else {
                return Err(CoreError::IOError(format!(
                    "Parquet file not found: {}",
                    parquet_path
                )));
            };
            *self.row_data.write().unwrap() = disk_row_data;
        } else {
            println!("  Keeping external Parquet reference (no row_data replacement)");
        }

        // 标记已在开始时设置，这里不需要重复设置
        // self.persisted 已经是 true

        log::debug!("Segment persist completed in {:?}", start.elapsed());

        Ok(())
    }

    /// Load a frozen segment from disk
    /// Uses new naming format: segment-{start_id}-{end_id}
    pub fn load_frozen(
        base_dir: &str,
        start_id: u64,
        end_id: u64,
        schema: Arc<Schema>,
    ) -> CoreResult<Self> {
        log::debug!(
            "🔍 [DEBUG load_frozen] base_dir: {} start_id: {}, end_id: {}",
            base_dir,
            start_id,
            end_id
        );
        let segment_path = format!("{}/segment-{}-{}", base_dir, start_id, end_id);
        log::debug!("🔍 [DEBUG load_frozen] segment_path: {}", segment_path);

        if !std::path::Path::new(&segment_path).exists() {
            return Err(CoreError::NotExisted(format!(
                "Segment path not found: {}",
                segment_path
            )));
        }

        log::info!("Loading frozen segment from: {}", segment_path);

        // 1. Load fields
        let start = std::time::Instant::now();
        let mut fields: Vec<Box<dyn IndexWriter>> = Vec::new();

        for field_opt in &schema.fields {
            let field_name = field_opt.name();
            let field_path = format!("{}/field-{}", segment_path, field_name);

            match field_opt {
                FieldOption::Keyword { .. } => {
                    // Create disk-based Keyword
                    let keyword = KeywordField::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(keyword) as Box<dyn IndexWriter>);
                }
                FieldOption::I8 { .. } => {
                    let field = I8Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::I16 { .. } => {
                    let field = I16Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::I32 { .. } => {
                    let field = I32Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::I64 { .. } => {
                    let num_i64 = I64Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(num_i64) as Box<dyn IndexWriter>);
                }
                FieldOption::U8 { .. } => {
                    let field = U8Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::U16 { .. } => {
                    let field = U16Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::U32 { .. } => {
                    let field = U32Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::U64 { .. } => {
                    let field = U64Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::F32 { .. } => {
                    let field = F32Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::F64 { .. } => {
                    let num_f64 = F64Field::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(num_f64) as Box<dyn IndexWriter>);
                }
                FieldOption::Boolean { .. } => {
                    let field = BooleanField::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
                FieldOption::Timestamp { .. } => {
                    let field = field_store::TimestampField::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(field) as Box<dyn IndexWriter>);
                }
            }
        }
        println!("  Fields loaded in {:?}", start.elapsed());

        // 2. Load pk_bloomfilter
        let pk_path = format!("{}/pk_bloomfilter", segment_path);
        let pk_bloomfilter = if std::path::Path::new(&pk_path).exists() {
            let buffer = std::fs::read(&pk_path)
                .map_err(|e| CoreError::IOError(format!("Failed to read pk_bloomfilter: {}", e)))?;

            // Use bloomfilter's built-in deserialization
            Bloom::from_bytes(buffer).map_err(|e| {
                CoreError::IOError(format!("Failed to deserialize bloom filter: {}", e))
            })?
        } else {
            // 如果不存在，创建一个空的 BloomFilter
            let expected_items = schema.persist_policy.max_docs_per_segment as usize;
            Bloom::new_for_fp_rate(expected_items, 0.01)
                .map_err(|e| CoreError::Internal(format!("Failed to create bloom filter: {}", e)))?
        };

        // 3. Load deleted bitmap (only this segment's deleted file)
        let deleted_start = std::time::Instant::now();
        let deleted_path = format!("{}/deleted", segment_path);
        let deleted = if std::path::Path::new(&deleted_path).exists() {
            let file = std::fs::File::open(&deleted_path)
                .map_err(|e| CoreError::IOError(format!("Failed to open deleted file: {}", e)))?;

            RoaringBitmap::deserialize_from(file).map_err(|e| {
                CoreError::IOError(format!("Failed to deserialize deleted bitmap: {}", e))
            })?
        } else {
            RoaringBitmap::new()
        };
        println!(
            "  Deleted bitmap loaded ({} entries) in {:?}",
            deleted.len(),
            deleted_start.elapsed()
        );

        // 3. Calculate doc_id_gen and max_doc_id from segment range
        // end_id 是最后一个文档的ID (inclusive)
        // doc_id_gen 是下一个可用ID的相对偏移 = (end_id - start_id) + 1
        let max_doc_id_relative = (end_id - start_id) as u32;
        let doc_id_gen = max_doc_id_relative + 1;
        println!(
            "  Segment range: {}-{} (inclusive), doc_id_gen: {}, max_doc_id_relative: {}",
            start_id, end_id, doc_id_gen, max_doc_id_relative
        );

        // 4. Build field index
        let field_index = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().to_string(), i))
            .collect();

        // 5. Load row_data and created_at from disk (check meta.json first)
        let meta_path = format!("{}/meta.json", segment_path);
        let (external_parquet_path, created_ts_ms_opt) =
            if std::path::Path::new(&meta_path).exists() {
                let meta_content = std::fs::read_to_string(&meta_path).ok();
                let meta_json = meta_content
                    .and_then(|content| serde_json::from_str::<serde_json::Value>(&content).ok());

                let external = meta_json
                    .as_ref()
                    .and_then(|meta| meta["external_parquet"].as_str().map(String::from));
                let created_ts_ms = meta_json
                    .as_ref()
                    .and_then(|meta| meta["created_ts_ms"].as_u64());

                (external, created_ts_ms)
            } else {
                (None, None)
            };

        let (row_data, base_path) = if let Some(external_path) = external_parquet_path {
            println!(
                "  Loading row_data from external Parquet: {}",
                external_path
            );
            (
                RowDataStore::new_parquet(&external_path)?,
                Some(external_path),
            )
        } else {
            let row_data_path = format!("{}/rowdata", segment_path);
            let parquet_path = format!("{}/rowdata.parquet", row_data_path);
            log::debug!(
                "  🔍 [DEBUG] Checking Parquet path: {} Path exists: {}",
                parquet_path,
                std::path::Path::new(&parquet_path).exists()
            );
            if std::path::Path::new(&parquet_path).exists() {
                log::info!("  Loading row_data from Parquet: {}", parquet_path);
                (
                    RowDataStore::new_parquet(&parquet_path)?,
                    Some(segment_path.clone()),
                )
            } else {
                println!("  No row_data found, creating empty store");
                (RowDataStore::new_memory(32), Some(segment_path.clone()))
            }
        };

        // 使用 meta 中的 created_ts_ms 恢复逻辑创建时间；如果没有就使用当前时间
        let created_at = if let Some(created_ts_ms) = created_ts_ms_opt {
            let now_instant = Instant::now();
            let now_ts_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let age_ms = now_ts_ms.saturating_sub(created_ts_ms);
            now_instant
                .checked_sub(std::time::Duration::from_millis(age_ms))
                .unwrap_or(now_instant)
        } else {
            Instant::now()
        };

        let segment = Self {
            start: start_id,
            doc_id_gen: AtomicU32::new(doc_id_gen),
            max_doc_id: AtomicU32::new(max_doc_id_relative),
            persisted: AtomicBool::new(true), // 从磁盘加载 = 已持久化
            created_at,
            pk_bloomfilter: RwLock::new(pk_bloomfilter),
            deleted: RwLock::new(deleted),
            fields: RwLock::new(fields),
            field_index,
            schema,
            row_data: RwLock::new(row_data),
            base_path,
        };

        println!("Frozen segment loaded in {:?}", start.elapsed());

        Ok(segment)
    }

    /// Check if segment is persisted to disk
    pub fn is_persisted(&self) -> bool {
        self.persisted.load(Ordering::Relaxed)
    }

    /// Get the parquet file path if this segment references an external file
    pub fn get_parquet_path(&self) -> Option<&str> {
        self.base_path.as_deref()
    }

    /// Get segment base path (if persisted)
    pub fn base_path(&self) -> Option<String> {
        self.base_path.clone()
    }

    /// Check if this segment references an external data file (e.g., Parquet)
    /// Returns true if the segment uses an external file instead of internal storage
    pub fn is_external_reference(&self) -> bool {
        let row_data = self.row_data.read().unwrap();
        matches!(*row_data, RowDataStore::Parquet(_))
    }

    /// Get the path of the external data file if this segment references one
    /// Returns None if the segment uses internal storage
    pub fn get_external_data_path(&self) -> Option<String> {
        self.base_path.clone()
    }

    /// Get current doc count in this segment
    pub fn doc_count(&self) -> u32 {
        self.doc_id_gen.load(Ordering::Relaxed)
    }

    /// Get segment creation time as duration since process start
    pub fn created_since_start(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Get segment age (time since creation)
    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Get the next doc ID (start + doc_count)
    pub fn next_doc_id(&self) -> u64 {
        self.start + self.doc_count() as u64
    }

    /// Get the count of deleted documents
    pub fn deleted_count(&self) -> u64 {
        self.deleted.read().unwrap().len()
    }

    /// Get the fields (IndexWriter) vector
    /// This is used by PartitionTableProvider to convert IndexWriter to IndexReader
    pub fn get_fields(&self) -> std::sync::RwLockReadGuard<'_, Vec<Box<dyn IndexWriter>>> {
        self.fields.read().unwrap()
    }

    /// Get cloned IndexReaders from this segment
    /// Returns HashMap of field_name -> Box<dyn IndexReader>
    /// This is fast because InvertedIndex is clone-friendly (Arc internally)
    pub fn get_index_readers(&self) -> std::collections::HashMap<String, Box<dyn IndexReader>> {
        use std::collections::HashMap;

        let fields = self.fields.read().unwrap();
        let mut readers = HashMap::new();

        for field_writer in fields.iter() {
            // 🔧 Convert to lowercase to match schema field names (which are also lowercase)
            let name = field_writer.name().to_lowercase();

            // Downcast and clone the concrete type
            if let Some(keyword) = field_writer.as_any().downcast_ref::<KeywordField>() {
                readers.insert(name, Box::new(keyword.clone()) as Box<dyn IndexReader>);
            } else if let Some(num_i64) = field_writer.as_any().downcast_ref::<I64Field>() {
                readers.insert(name, Box::new(num_i64.clone()) as Box<dyn IndexReader>);
            } else if let Some(num_f64) = field_writer.as_any().downcast_ref::<F64Field>() {
                readers.insert(name, Box::new(num_f64.clone()) as Box<dyn IndexReader>);
            } else if let Some(num_u32) = field_writer.as_any().downcast_ref::<U32Field>() {
                readers.insert(name, Box::new(num_u32.clone()) as Box<dyn IndexReader>);
            } else if let Some(num_u64) = field_writer.as_any().downcast_ref::<U64Field>() {
                readers.insert(name, Box::new(num_u64.clone()) as Box<dyn IndexReader>);
            } else if let Some(num_i32) = field_writer.as_any().downcast_ref::<I32Field>() {
                readers.insert(name, Box::new(num_i32.clone()) as Box<dyn IndexReader>);
            } else if let Some(num_i8) = field_writer.as_any().downcast_ref::<I8Field>() {
                readers.insert(name, Box::new(num_i8.clone()) as Box<dyn IndexReader>);
            } else if let Some(num_i16) = field_writer.as_any().downcast_ref::<I16Field>() {
                readers.insert(name, Box::new(num_i16.clone()) as Box<dyn IndexReader>);
            } else if let Some(num_u8) = field_writer.as_any().downcast_ref::<U8Field>() {
                readers.insert(name, Box::new(num_u8.clone()) as Box<dyn IndexReader>);
            } else if let Some(num_u16) = field_writer.as_any().downcast_ref::<U16Field>() {
                readers.insert(name, Box::new(num_u16.clone()) as Box<dyn IndexReader>);
            } else if let Some(num_f32) = field_writer.as_any().downcast_ref::<F32Field>() {
                readers.insert(name, Box::new(num_f32.clone()) as Box<dyn IndexReader>);
            } else if let Some(boolean) = field_writer.as_any().downcast_ref::<BooleanField>() {
                readers.insert(name, Box::new(boolean.clone()) as Box<dyn IndexReader>);
            } else if let Some(timestamp) = field_writer
                .as_any()
                .downcast_ref::<field_store::TimestampField>()
            {
                readers.insert(name, Box::new(timestamp.clone()) as Box<dyn IndexReader>);
            }
        }

        readers
    }

    /// Query a field by exact value using the segment's index
    /// Returns bitmap of matching document IDs
    pub fn query_field(
        &self,
        field_name: &str,
        value: &datafusion::scalar::ScalarValue,
    ) -> Option<RoaringBitmap> {
        let fields = self.fields.read().unwrap();
        let index = self.field_index.get(field_name)?;
        let field_writer = fields.get(*index)?;

        // Downcast to concrete type and call IndexReader::query
        if let Some(keyword) = field_writer.as_any().downcast_ref::<KeywordField>() {
            <KeywordField as IndexReader>::query(keyword, value)
        } else if let Some(num_i64) = field_writer.as_any().downcast_ref::<I64Field>() {
            <I64Field as IndexReader>::query(num_i64, value)
        } else if let Some(num_f64) = field_writer.as_any().downcast_ref::<F64Field>() {
            <F64Field as IndexReader>::query(num_f64, value)
        } else if let Some(num_u32) = field_writer.as_any().downcast_ref::<U32Field>() {
            <U32Field as IndexReader>::query(num_u32, value)
        } else if let Some(num_u64) = field_writer.as_any().downcast_ref::<U64Field>() {
            <U64Field as IndexReader>::query(num_u64, value)
        } else if let Some(num_i32) = field_writer.as_any().downcast_ref::<I32Field>() {
            <I32Field as IndexReader>::query(num_i32, value)
        } else if let Some(num_i8) = field_writer.as_any().downcast_ref::<I8Field>() {
            <I8Field as IndexReader>::query(num_i8, value)
        } else if let Some(num_i16) = field_writer.as_any().downcast_ref::<I16Field>() {
            <I16Field as IndexReader>::query(num_i16, value)
        } else if let Some(num_u8) = field_writer.as_any().downcast_ref::<U8Field>() {
            <U8Field as IndexReader>::query(num_u8, value)
        } else if let Some(num_u16) = field_writer.as_any().downcast_ref::<U16Field>() {
            <U16Field as IndexReader>::query(num_u16, value)
        } else if let Some(num_f32) = field_writer.as_any().downcast_ref::<F32Field>() {
            <F32Field as IndexReader>::query(num_f32, value)
        } else if let Some(boolean) = field_writer.as_any().downcast_ref::<BooleanField>() {
            <BooleanField as IndexReader>::query(boolean, value)
        } else if let Some(timestamp) = field_writer
            .as_any()
            .downcast_ref::<field_store::TimestampField>()
        {
            <field_store::TimestampField as IndexReader>::query(timestamp, value)
        } else {
            None
        }
    }

    /// Range query on a field using the segment's index
    /// Returns bitmap of matching document IDs
    pub fn query_field_range(
        &self,
        field_name: &str,
        start: &datafusion::scalar::ScalarValue,
        start_inclusive: bool,
        end: &datafusion::scalar::ScalarValue,
        end_inclusive: bool,
    ) -> Option<RoaringBitmap> {
        let fields = self.fields.read().unwrap();
        let index = self.field_index.get(field_name)?;
        let field_writer = fields.get(*index)?;

        // Downcast to concrete type and call IndexReader::range
        if let Some(keyword) = field_writer.as_any().downcast_ref::<KeywordField>() {
            <KeywordField as IndexReader>::range(
                keyword,
                start,
                start_inclusive,
                end,
                end_inclusive,
            )
        } else if let Some(num_i64) = field_writer.as_any().downcast_ref::<I64Field>() {
            <I64Field as IndexReader>::range(num_i64, start, start_inclusive, end, end_inclusive)
        } else if let Some(num_f64) = field_writer.as_any().downcast_ref::<F64Field>() {
            <F64Field as IndexReader>::range(num_f64, start, start_inclusive, end, end_inclusive)
        } else if let Some(num_u32) = field_writer.as_any().downcast_ref::<U32Field>() {
            <U32Field as IndexReader>::range(num_u32, start, start_inclusive, end, end_inclusive)
        } else if let Some(num_u64) = field_writer.as_any().downcast_ref::<U64Field>() {
            <U64Field as IndexReader>::range(num_u64, start, start_inclusive, end, end_inclusive)
        } else if let Some(num_i32) = field_writer.as_any().downcast_ref::<I32Field>() {
            <I32Field as IndexReader>::range(num_i32, start, start_inclusive, end, end_inclusive)
        } else if let Some(num_i8) = field_writer.as_any().downcast_ref::<I8Field>() {
            <I8Field as IndexReader>::range(num_i8, start, start_inclusive, end, end_inclusive)
        } else if let Some(num_i16) = field_writer.as_any().downcast_ref::<I16Field>() {
            <I16Field as IndexReader>::range(num_i16, start, start_inclusive, end, end_inclusive)
        } else if let Some(num_u8) = field_writer.as_any().downcast_ref::<U8Field>() {
            <U8Field as IndexReader>::range(num_u8, start, start_inclusive, end, end_inclusive)
        } else if let Some(num_u16) = field_writer.as_any().downcast_ref::<U16Field>() {
            <U16Field as IndexReader>::range(num_u16, start, start_inclusive, end, end_inclusive)
        } else if let Some(num_f32) = field_writer.as_any().downcast_ref::<F32Field>() {
            <F32Field as IndexReader>::range(num_f32, start, start_inclusive, end, end_inclusive)
        } else if let Some(timestamp) = field_writer
            .as_any()
            .downcast_ref::<field_store::TimestampField>()
        {
            <field_store::TimestampField as IndexReader>::range(
                timestamp,
                start,
                start_inclusive,
                end,
                end_inclusive,
            )
        } else {
            None
        }
    }

    /// Check if a field has an index
    pub fn has_field_index(&self, field_name: &str) -> bool {
        self.field_index.contains_key(field_name)
    }

    /// Get all indexed field names
    pub fn get_indexed_fields(&self) -> Vec<String> {
        self.field_index.keys().cloned().collect()
    }

    /// Get a clone of the deleted bitmap
    pub fn get_deleted(&self) -> RoaringBitmap {
        self.deleted.read().unwrap().clone()
    }

    /// Get a clone of the row data
    pub fn get_row_data(&self) -> RowDataStore {
        self.row_data.read().unwrap().clone()
    }

    /// Persist row_data to disk using Parquet format
    /// Reorganizes data into fixed-size RowGroups (1000 docs per RowGroup)
    /// Each RowGroup can be read independently for efficient querying
    /// Deleted documents have their values (except internal_id) set to NULL to save space
    fn persist_row_data(
        &self,
        rowdata_path: &str,
        row_data: RowDataStore,
        deleted: &RoaringBitmap,
    ) -> CoreResult<()> {
        use datafusion::arrow::compute::concat_batches;
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::basic::Compression;
        use datafusion::parquet::file::properties::WriterProperties;

        // Extract memory BTree from RowDataStore
        let memory_tree = match row_data {
            RowDataStore::Memory(tree) => tree,
            RowDataStore::Parquet(_) => {
                return Err(CoreError::Internal(
                    "Cannot persist parquet row_data".to_string(),
                ));
            }
        };

        // Step 1: Collect all batches and their starting internal_ids
        let mut all_batches: Vec<(u32, RecordBatch)> = Vec::new();
        let mut total_rows: usize = 0;

        for item in memory_tree.iter() {
            let (key, batch, _ttl) = &*item;
            // Process batch to set deleted rows to NULL (use key as offset/internal_id)
            let processed_batch = self.process_batch_for_deleted(batch, deleted, *key);
            total_rows += processed_batch.num_rows();
            all_batches.push((*key, processed_batch));
        }

        // If no data, return early
        if all_batches.is_empty() {
            println!("    No data to persist");
            return Ok(());
        }

        // Build a flat array of (internal_id, row_index) for all rows
        let mut row_internal_ids: Vec<u32> = Vec::with_capacity(total_rows);
        let mut merged_batch_data: Vec<datafusion::arrow::array::RecordBatch> = Vec::new();

        for (start_id, batch) in &all_batches {
            merged_batch_data.push(batch.clone());
            let num_rows = batch.num_rows() as u32;
            for i in 0..num_rows {
                row_internal_ids.push(start_id + i);
            }
        }

        // Merge all batches into one large batch
        if merged_batch_data.is_empty() {
            println!("    No data to persist");
            return Ok(());
        }

        let schema = merged_batch_data[0].schema();
        let merged_batch = concat_batches(&schema, &merged_batch_data)
            .map_err(|e| CoreError::Internal(format!("Failed to merge batches: {}", e)))?;

        // Report deleted documents stats
        if !deleted.is_empty() {
            println!(
                "    Processed {} deleted documents (values set to NULL)",
                deleted.len()
            );
        }

        // Step 2: Reorganize into fixed-size batches (1000 docs per RowGroup)
        const BATCH_SIZE: usize = 1000;
        let total_docs = merged_batch.num_rows();
        let mut reorganized_batches: Vec<(u32, RecordBatch)> = Vec::new();

        let mut row_group_keys: Vec<u32> = Vec::new();

        for chunk_start in (0..total_docs).step_by(BATCH_SIZE) {
            let chunk_end = (chunk_start + BATCH_SIZE).min(total_docs);

            // Get the first internal_id as the batch key
            let batch_key = row_internal_ids[chunk_start];
            row_group_keys.push(batch_key);

            // Slice the batch for this chunk
            let chunk_batch = merged_batch.slice(chunk_start, chunk_end - chunk_start);

            reorganized_batches.push((batch_key, chunk_batch));
        }

        let batch_count = reorganized_batches.len();
        println!(
            "    Reorganized into {} RowGroups ({}~{} docs per RowGroup)",
            batch_count,
            if total_docs < BATCH_SIZE {
                total_docs
            } else {
                BATCH_SIZE
            },
            BATCH_SIZE
        );

        // Step 3: Persist to Parquet file
        let parquet_file_path = format!("{}/rowdata.parquet", rowdata_path);
        let file = std::fs::File::create(&parquet_file_path)
            .map_err(|e| CoreError::IOError(format!("Failed to create parquet file: {}", e)))?;

        // Configure Parquet writer
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_max_row_group_size(BATCH_SIZE)
            .set_key_value_metadata(Some(vec![
                datafusion::parquet::file::metadata::KeyValue::new(
                    "row_group_keys".to_string(),
                    Some(serde_json::to_string(&row_group_keys).map_err(|e| {
                        CoreError::Internal(format!("Failed to serialize row_group_keys: {}", e))
                    })?),
                ),
            ]))
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| CoreError::IOError(format!("Failed to create ArrowWriter: {}", e)))?;

        // Write each batch as a RowGroup
        for (_key, batch) in reorganized_batches {
            writer
                .write(&batch)
                .map_err(|e| CoreError::IOError(format!("Failed to write batch: {}", e)))?;
        }

        writer
            .close()
            .map_err(|e| CoreError::IOError(format!("Failed to close writer: {}", e)))?;

        println!("    Persisted to Parquet: {}", parquet_file_path);

        Ok(())
    }

    /// Mark an entire RecordBatch as deleted (set all columns except internal_id to NULL)
    #[allow(dead_code)]
    fn mark_batch_as_deleted(&self, batch: &RecordBatch) -> RecordBatch {
        use datafusion::arrow::array::{make_array, ArrayData, ArrayRef};
        use datafusion::arrow::buffer::NullBuffer;
        use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};

        let num_rows = batch.num_rows();

        // Create nullable schema
        let old_schema = batch.schema();
        let new_fields: Vec<Field> = old_schema
            .fields()
            .iter()
            .map(|f| {
                if f.is_nullable() {
                    f.as_ref().clone()
                } else {
                    Field::new(f.name(), f.data_type().clone(), true)
                }
            })
            .collect();
        let new_schema = Arc::new(ArrowSchema::new(new_fields));

        // Create columns with all NULLs (except internal_id)
        let new_columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(col_idx, col)| {
                // Keep internal_id column as-is
                if col_idx == 0 {
                    return col.clone();
                }

                // All other columns: set to NULL
                let null_buffer = NullBuffer::from(vec![false; num_rows]);
                let array_data = col.to_data();
                let new_data = ArrayData::builder(array_data.data_type().clone())
                    .len(array_data.len())
                    .buffers(array_data.buffers().to_vec())
                    .nulls(Some(null_buffer))
                    .child_data(array_data.child_data().to_vec())
                    .build()
                    .expect("Failed to build array data");

                make_array(new_data)
            })
            .collect();

        RecordBatch::try_new(new_schema, new_columns).expect("Failed to create deleted batch")
    }

    /// Process RecordBatch to handle deleted documents
    /// Sets deleted document rows' columns to None to save disk space
    /// start_id: the internal_id of the first row in this batch
    #[allow(dead_code)]
    fn process_batch_for_deleted(
        &self,
        batch: &RecordBatch,
        deleted: &RoaringBitmap,
        start_id: u32,
    ) -> RecordBatch {
        use datafusion::arrow::array::{make_array, ArrayData, ArrayRef};
        use datafusion::arrow::buffer::NullBuffer;
        use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};

        // If no deletions, return batch as-is
        if deleted.is_empty() {
            return batch.clone();
        }

        let num_rows = batch.num_rows() as u32;

        // Check which rows in this batch are deleted
        let mut has_deleted = false;
        for row_idx in 0..num_rows {
            if deleted.contains(start_id + row_idx) {
                has_deleted = true;
                break;
            }
        }

        // If no rows in this batch are deleted, return as-is
        if !has_deleted {
            return batch.clone();
        }

        // Create nullable schema (all non-nullable fields become nullable)
        let old_schema = batch.schema();
        let new_fields: Vec<Field> = old_schema
            .fields()
            .iter()
            .map(|f| {
                if f.is_nullable() {
                    f.as_ref().clone()
                } else {
                    Field::new(f.name(), f.data_type().clone(), true)
                }
            })
            .collect();
        let new_schema = Arc::new(ArrowSchema::new(new_fields));

        // Create new columns with deleted rows set to NULL
        let num_rows_usize = batch.num_rows();
        let new_columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| {
                // Build null buffer: true = valid, false = null
                let mut null_builder = vec![true; num_rows_usize];
                for row_idx in 0..num_rows {
                    if deleted.contains(start_id + row_idx) {
                        null_builder[row_idx as usize] = false;
                    }
                }

                // Create null buffer
                let null_buffer = NullBuffer::from(null_builder);

                // Create new array data with null buffer
                let array_data = col.to_data();
                let new_data = ArrayData::builder(array_data.data_type().clone())
                    .len(array_data.len())
                    .buffers(array_data.buffers().to_vec())
                    .nulls(Some(null_buffer))
                    .child_data(array_data.child_data().to_vec())
                    .build()
                    .expect("Failed to build array data");

                make_array(new_data)
            })
            .collect();

        RecordBatch::try_new(new_schema, new_columns)
            .expect("Failed to create batch with deleted rows")
    }
    /// Merge multiple RecordBatches into one, handling deleted documents
    /// Deleted documents have all non-nullable columns converted to nullable and set to None  
    #[allow(dead_code)]
    fn merge_records_with_deletes(
        &self,
        records: &[(u32, RecordBatch)],
        _deleted: &RoaringBitmap,
        schema: &Arc<arrow::datatypes::Schema>,
    ) -> CoreResult<RecordBatch> {
        use datafusion::arrow::array::{make_array, ArrayRef, MutableArrayData};

        // Collect all rows from all batches
        let mut all_columns: Vec<Vec<ArrayRef>> = vec![Vec::new(); schema.fields().len()];
        let mut total_rows = 0;

        for (_internal_id, batch) in records {
            let num_rows = batch.num_rows();

            for (col_idx, column) in batch.columns().iter().enumerate() {
                all_columns[col_idx].push(column.clone());
            }

            total_rows += num_rows;
        }

        // Merge columns
        let mut merged_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        for (col_idx, _field) in schema.fields().iter().enumerate() {
            let column_arrays: &Vec<ArrayRef> = &all_columns[col_idx];

            if column_arrays.is_empty() {
                return Err(CoreError::Internal("No column arrays found".to_string()));
            }

            // Use MutableArrayData for efficient concatenation
            let array_data: Vec<_> = column_arrays.iter().map(|a| a.to_data()).collect();
            let mut mutable = MutableArrayData::new(array_data.iter().collect(), false, total_rows);

            for (i, data) in array_data.iter().enumerate() {
                mutable.extend(i, 0, data.len());
            }

            let merged_array = make_array(mutable.freeze());
            merged_columns.push(merged_array);
        }

        RecordBatch::try_new(schema.clone(), merged_columns)
            .map_err(|e| CoreError::Internal(format!("Failed to create merged batch: {}", e)))
    }
}
