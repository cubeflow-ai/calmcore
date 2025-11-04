mod field_store;

use crate::{
    partition::WriteInfo,
    schema::{field::FieldOption, Schema},
    segment::field_store::{
        keyword::Keyword, IndexWriter, InvertedIndex, PkWriter, RowDataStore,
        U32RecordBatchSerializer,
    },
    utils::error::{CoreError, CoreResult},
};
use arrow::{
    array::{ArrayRef, RecordBatch, UInt32Array},
    datatypes::{DataType, Field, SchemaRef},
};
use bloomfilter::Bloom;
use itertools::Itertools;
use roaring::RoaringBitmap;
use std::{
    collections::HashMap,
    io::Read,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, RwLock,
    },
    time::Instant,
};

/// 控制非主键字段的索引写入模式
#[derive(Clone, Copy, Debug)]
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
                    let keyword = Keyword::new(field_opt);
                    fields.push(Box::new(keyword) as Box<dyn IndexWriter>);
                }
                FieldOption::I32 { name, index } => todo!(),
                FieldOption::I64 { name, index } => todo!(),
                FieldOption::U32 { name, index } => todo!(),
                FieldOption::F32 { name, index } => todo!(),
                FieldOption::F64 { name, index } => todo!(),
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
        let bloom = Bloom::new_for_fp_rate(expected_items, 0.01);

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

    pub fn write(
        &self,
        data: &RecordBatch,
        pk_hash: Option<Vec<u32>>,
        info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<Vec<u32>> {
        // add id column
        let mut columns = Vec::with_capacity(data.schema().fields().len() + 1);
        columns.push(Field::new("_internal_id", DataType::UInt32, false));
        columns.extend(data.schema().flattened_fields().into_iter().cloned());
        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(columns));

        let num_rows = data.num_rows() as u32;
        // generate auto-increment id column
        let start_id = self.doc_id_gen.load(Ordering::Relaxed);
        // concatenate auto-increment id column to RecordBatch (insert into the first column)
        let old_columns = data.columns();
        let mut columns = Vec::with_capacity(old_columns.len() + 1);

        let result = (start_id..start_id + num_rows).collect_vec();

        columns.push(Arc::new(UInt32Array::from_iter_values(result.iter().cloned())) as ArrayRef);
        columns.extend_from_slice(old_columns);

        let new_data = RecordBatch::try_new(arrow_schema, columns).unwrap();

        // 更新doc_id_gen和max_doc_id
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
            let pk_field = &fields[*index];
            self.write_pk(pk_field, &new_data, pk_hash, info, lock)?;
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
            if let Err(e) = f.write(&new_data) {
                eprintln!("index write field {:?} failed: {:?}", f.name(), e);
            }
        }

        Ok(result)
    }

    fn write_pk(
        &self,
        pk_field: &Box<dyn IndexWriter>,
        data: &RecordBatch,
        pk_hash: Option<Vec<u32>>,
        info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<()> {
        use crate::schema::field::FieldType;

        match pk_field.field_type() {
            FieldType::Keyword => {
                let pk_writer = pk_field.as_any().downcast_ref::<Keyword>().ok_or_else(|| {
                    CoreError::Internal(format!(
                        "field:{:?} field_type:{:?} does not implement PkWriter",
                        pk_field.name(),
                        pk_field.field_type()
                    ))
                })?;

                let del = pk_writer.write_pk(data, info, lock)?;
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
        let active = pk_hash.iter().any(|v| bloom.check(v));
        drop(bloom);

        // not found any id in this segment
        if !active {
            return None;
        }

        let index = *self
            .field_index
            .get(self.schema.primary_key.as_ref().unwrap())
            .unwrap();

        let ids = {
            let fields = self.fields.read().unwrap();
            let ids = fields[index].mget_internal_id(column);

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
        total - self.deleted.read().unwrap().len() as u64
    }

    /// Get a single document by internal doc_id
    /// Returns None if document doesn't exist or is deleted
    ///
    /// This implements lazy loading - only loads the RecordBatch containing the requested doc
    pub fn get_document(&self, doc_id: u32) -> Option<RecordBatch> {
        use arrow::compute::filter_record_batch;

        // Check if document is deleted
        if self.deleted.read().unwrap().contains(doc_id) {
            return None;
        }

        // Find which RecordBatch contains this doc_id using floor lookup
        // BTree keys are the first doc_id in each batch
        // e.g., keys: [1, 100, 200], query 50 -> finds batch at key 1
        let row_data = self.row_data.read().unwrap();

        // Use floor to find the batch containing this doc_id
        let (_start_id, batch) = row_data.floor(&doc_id)?;

        // Verify this batch contains our doc_id and extract the row
        let internal_ids =
            arrow::array::cast::as_primitive_array::<arrow::datatypes::UInt32Type>(batch.column(0));

        for (row_idx, id) in internal_ids.values().iter().enumerate() {
            if *id == doc_id {
                // Found it! Extract just this row
                let mask: Vec<bool> = (0..batch.num_rows()).map(|i| i == row_idx).collect();
                let mask_array = arrow::array::BooleanArray::from(mask);

                return filter_record_batch(&batch, &mask_array).ok();
            }
        }

        None
    }
    /// Get multiple documents by internal doc_ids
    /// Returns a RecordBatch containing only the requested documents
    /// Deleted documents are filtered out
    pub fn get_documents(&self, doc_ids: &[u32]) -> CoreResult<Option<RecordBatch>> {
        use arrow::compute::concat_batches;

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
        let doc_count = self.doc_id_gen.load(Ordering::Relaxed);

        for doc_id in 0..doc_count {
            if deleted.contains(doc_id) {
                continue;
            }

            if let Some(batch) = row_data.get(&doc_id) {
                // Apply deleted filter to this batch
                let filtered_batch = self.filter_deleted_from_batch(&batch, &deleted);
                batches.push(filtered_batch);
            }
        }

        Ok(batches)
    }

    /// Filter deleted rows from a RecordBatch
    fn filter_deleted_from_batch(
        &self,
        batch: &RecordBatch,
        deleted: &RoaringBitmap,
    ) -> RecordBatch {
        use arrow::compute::filter_record_batch;

        if deleted.is_empty() {
            return batch.clone();
        }

        // Get internal_id column (first column)
        let internal_id_col = batch.column(0);
        let internal_ids =
            arrow::array::cast::as_primitive_array::<arrow::datatypes::UInt32Type>(internal_id_col);

        // Build boolean mask: true = keep, false = filter out
        let mask: Vec<bool> = internal_ids
            .values()
            .iter()
            .map(|id| !deleted.contains(*id))
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
                    let keyword = Keyword::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(keyword) as Box<dyn IndexWriter>);
                }
                FieldOption::I32 { name, index } => todo!(),
                FieldOption::I64 { name, index } => todo!(),
                FieldOption::U32 { name, index } => todo!(),
                FieldOption::F32 { name, index } => todo!(),
                FieldOption::F64 { name, index } => todo!(),
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

            // 解析格式: [k_num(4 bytes)][sip_keys(4*16 bytes)][bitmap_len(8 bytes)][bitmap]
            let mut offset = 0;
            let k_num = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as u32;
            offset += 4;

            let mut sip_keys = [(0u64, 0u64); 2];
            for i in 0..2 {
                let k0_bytes: [u8; 8] = buffer[offset..offset + 8]
                    .try_into()
                    .map_err(|_| CoreError::IOError("Invalid bloom filter format".to_string()))?;
                let k0 = u64::from_le_bytes(k0_bytes);
                offset += 8;

                let k1_bytes: [u8; 8] = buffer[offset..offset + 8]
                    .try_into()
                    .map_err(|_| CoreError::IOError("Invalid bloom filter format".to_string()))?;
                let k1 = u64::from_le_bytes(k1_bytes);
                offset += 8;

                sip_keys[i] = (k0, k1);
            }

            let bitmap_len_bytes: [u8; 8] = buffer[offset..offset + 8]
                .try_into()
                .map_err(|_| CoreError::IOError("Invalid bloom filter format".to_string()))?;
            let bitmap_len = u64::from_le_bytes(bitmap_len_bytes) as usize;
            offset += 8;

            let bitmap = buffer[offset..offset + bitmap_len].to_vec();

            Bloom::from_existing(&bitmap, (bitmap_len * 8) as u64, k_num, sip_keys)
        } else {
            // 如果不存在，创建一个空的 BloomFilter
            let expected_items = schema.persist_policy.max_docs_per_segment as usize;
            Bloom::new_for_fp_rate(expected_items, 0.01)
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

        // 5. Load row data
        let row_data = if std::path::Path::new(&row_data_path).exists() {
            RowDataStore::new_disk(&row_data_path, U32RecordBatchSerializer::default())?
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
        // 注意：不再检查状态，允许多次持久化（幂等操作）
        // 如果 segment 已经持久化，fields 和 row_data 已经是 Disk 变体，会直接跳过

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

        println!("Persisting segment to: {} (temp)", segment_tmp_path);

        // 1. Persist each field's inverted index (to temp directory)
        let start = std::time::Instant::now();
        let new_fields = {
            let fields = self.fields.read().unwrap();
            let mut new_fields: Vec<Box<dyn IndexWriter>> = Vec::new();

            for field in fields.iter() {
                let field_name = field.name();
                let field_path = format!("{}/field-{}", segment_tmp_path, field_name);

                println!("  Persisting field: {}", field_name);

                // Try to downcast and persist based on field type
                if let Some(keyword) = field
                    .as_any()
                    .downcast_ref::<field_store::keyword::Keyword>()
                {
                    let disk_field = keyword.persist(&field_path)?;
                    new_fields.push(Box::new(disk_field) as Box<dyn IndexWriter>);
                } else {
                    return Err(CoreError::Internal(format!(
                        "Unsupported field type for persist: {}",
                        field_name
                    )));
                }
            }

            new_fields
        }; // Release read lock here

        // Now acquire write lock
        *self.fields.write().unwrap() = new_fields;
        println!("  Fields persisted in {:?}", start.elapsed());

        // 2. Persist pk_bloomfilter (to temp directory)
        let pk_start = std::time::Instant::now();
        {
            let pk_path = format!("{}/pk_bloomfilter", segment_tmp_path);
            let pk_bloom = self.pk_bloomfilter.read().unwrap();

            // BloomFilter 可以转换为字节数组
            let bitmap = pk_bloom.bitmap();
            let k_num = pk_bloom.number_of_hash_functions();
            let sip_keys = pk_bloom.sip_keys();

            // 简单格式: [k_num(4 bytes)][sip_keys(4*16 bytes)][bitmap_len(8 bytes)][bitmap]
            let mut buffer = Vec::new();
            buffer.extend_from_slice(&(k_num as u32).to_le_bytes());

            // 序列化两个 sip_key 对
            for &(k0, k1) in &sip_keys {
                buffer.extend_from_slice(&k0.to_le_bytes());
                buffer.extend_from_slice(&k1.to_le_bytes());
            }

            buffer.extend_from_slice(&(bitmap.len() as u64).to_le_bytes());
            buffer.extend_from_slice(&bitmap);

            std::fs::write(&pk_path, buffer).map_err(|e| {
                CoreError::IOError(format!("Failed to write pk_bloomfilter file: {}", e))
            })?;

            println!(
                "  PK bloomfilter persisted ({} bits, {} KB) in {:?}",
                pk_bloom.number_of_bits(),
                bitmap.len() / 1024,
                pk_start.elapsed()
            );
        }

        // 3. Persist deleted bitmap (current segment, to temp directory)
        let deleted_start = std::time::Instant::now();
        {
            let deleted_path = format!("{}/deleted", segment_tmp_path);
            let deleted = self.deleted.read().unwrap();

            let mut file = std::fs::File::create(&deleted_path)
                .map_err(|e| CoreError::IOError(format!("Failed to create deleted file: {}", e)))?;

            deleted.serialize_into(&mut file).map_err(|e| {
                CoreError::IOError(format!("Failed to serialize deleted bitmap: {}", e))
            })?;

            println!(
                "  Deleted bitmap persisted ({} entries) in {:?}",
                deleted.len(),
                deleted_start.elapsed()
            );
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

        // 4. Persist row_data using BTree disk format (to temp directory)
        let rowdata_start = std::time::Instant::now();
        {
            let rowdata_path = format!("{}/rowdata", segment_tmp_path);
            std::fs::create_dir_all(&rowdata_path)
                .map_err(|e| CoreError::IOError(format!("Failed to create rowdata dir: {}", e)))?;

            // Clone row_data (fast - microseconds due to pointer implementation)
            let row_data_clone = self.row_data.read().unwrap().clone();
            let deleted = self.deleted.read().unwrap().clone();

            // Persist row data using TreeWriter
            self.persist_row_data(&rowdata_path, row_data_clone, &deleted)?;
        };

        println!("  Row data persisted in {:?}", rowdata_start.elapsed());

        // 5. Save segment metadata (to temp directory)
        let meta_path = format!("{}/meta.json", segment_tmp_path);
        let meta = serde_json::json!({
            "start": self.start,
            "doc_id_gen": self.doc_id_gen.load(Ordering::Relaxed),
            "max_doc_id": self.max_doc_id.load(Ordering::Relaxed),
        });
        std::fs::write(&meta_path, meta.to_string())
            .map_err(|e| CoreError::IOError(format!("Failed to write segment meta: {}", e)))?;

        println!("  All data written to temp directory");

        // Phase 2: Atomic rename
        println!(
            "  Phase 2: Atomic rename {} → {}",
            segment_tmp_path, segment_path
        );
        std::fs::rename(&segment_tmp_path, &segment_path).map_err(|e| {
            CoreError::IOError(format!(
                "Failed to rename temp directory: {}. Temp dir preserved for recovery.",
                e
            ))
        })?;
        println!("  Rename completed ✓");

        // Phase 3: Replace historical deleted files back to their segments
        if !history_snapshot.is_empty() {
            println!("  Phase 3: Replacing historical deleted files");
            let replace_start = std::time::Instant::now();

            for (hist_start, hist_end, _deleted_bitmap) in &history_snapshot {
                let source_path =
                    format!("{}/deleted_seg_{}_{}", segment_path, hist_start, hist_end);
                let target_path =
                    format!("{}/segment-{}-{}/deleted", base_dir, hist_start, hist_end);

                // Replace (overwrite) the target deleted file
                if let Err(e) = std::fs::rename(&source_path, &target_path) {
                    // Log error but continue with other files
                    eprintln!(
                        "Warning: Failed to replace deleted file for segment {}-{}: {}",
                        hist_start, hist_end, e
                    );
                    eprintln!("  File will be retried on next startup");
                } else {
                    println!("    Replaced segment-{}-{}/deleted ✓", hist_start, hist_end);
                }
            }

            println!(
                "  Historical files replaced in {:?}",
                replace_start.elapsed()
            );
        }

        // Phase 4: Replace in-memory row_data with disk-based reader
        println!("  Phase 4: Replacing memory row_data with disk reader");
        let replace_start = std::time::Instant::now();
        {
            let rowdata_path = format!("{}/rowdata", segment_path);
            let disk_row_data =
                RowDataStore::new_disk(&rowdata_path, U32RecordBatchSerializer::default())?;
            *self.row_data.write().unwrap() = disk_row_data;
        }
        println!(
            "  Row_data replaced with disk reader in {:?}",
            replace_start.elapsed()
        );

        // 标记为已持久化
        self.persisted.store(true, Ordering::Relaxed);

        println!("Segment persist completed in {:?}", start.elapsed());

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
        use crate::segment::field_store::InvertedIndex;

        let segment_path = format!("{}/segment-{}-{}", base_dir, start_id, end_id);

        if !std::path::Path::new(&segment_path).exists() {
            return Err(CoreError::NotExisted(format!(
                "Segment path not found: {}",
                segment_path
            )));
        }

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
                    let keyword = Keyword::from_disk(field_opt, &field_path)?;
                    fields.push(Box::new(keyword) as Box<dyn IndexWriter>);
                }
                FieldOption::I32 { .. } => {
                    todo!()
                }
                FieldOption::I64 { .. } => {
                    todo!()
                }
                FieldOption::U32 { .. } => {
                    todo!()
                }
                FieldOption::F32 { .. } => {
                    todo!()
                }
                FieldOption::F64 { .. } => {
                    todo!()
                }
            }
        }
        println!("  Fields loaded in {:?}", start.elapsed());

        // 2. Load pk_bloomfilter
        let pk_start = std::time::Instant::now();
        let pk_path = format!("{}/pk_bloomfilter", segment_path);
        let pk_bloomfilter = if std::path::Path::new(&pk_path).exists() {
            let buffer = std::fs::read(&pk_path)
                .map_err(|e| CoreError::IOError(format!("Failed to read pk_bloomfilter: {}", e)))?;

            // 解析格式: [k_num(4 bytes)][sip_keys(4*16 bytes)][bitmap_len(8 bytes)][bitmap]
            let mut offset = 0;
            let k_num = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as u32;
            offset += 4;

            let mut sip_keys = [(0u64, 0u64); 2];
            for i in 0..2 {
                let k0_bytes: [u8; 8] = buffer[offset..offset + 8]
                    .try_into()
                    .map_err(|_| CoreError::IOError("Invalid bloom filter format".to_string()))?;
                let k0 = u64::from_le_bytes(k0_bytes);
                offset += 8;

                let k1_bytes: [u8; 8] = buffer[offset..offset + 8]
                    .try_into()
                    .map_err(|_| CoreError::IOError("Invalid bloom filter format".to_string()))?;
                let k1 = u64::from_le_bytes(k1_bytes);
                offset += 8;

                sip_keys[i] = (k0, k1);
            }

            let bitmap_len_bytes: [u8; 8] = buffer[offset..offset + 8]
                .try_into()
                .map_err(|_| CoreError::IOError("Invalid bloom filter format".to_string()))?;
            let bitmap_len = u64::from_le_bytes(bitmap_len_bytes) as usize;
            offset += 8;

            let bitmap = buffer[offset..offset + bitmap_len].to_vec();

            Bloom::from_existing(&bitmap, (bitmap_len * 8) as u64, k_num, sip_keys)
        } else {
            // 如果不存在，创建一个空的 BloomFilter
            let expected_items = schema.persist_policy.max_docs_per_segment as usize;
            Bloom::new_for_fp_rate(expected_items, 0.01)
        };
        println!(
            "  PK bloomfilter loaded ({} bits, {} KB) in {:?}",
            pk_bloomfilter.number_of_bits(),
            pk_bloomfilter.bitmap().len() / 1024,
            pk_start.elapsed()
        );

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

        // 5. Load row_data from disk
        let row_data_path = format!("{}/rowdata", segment_path);
        let row_data = if std::path::Path::new(&row_data_path).exists() {
            println!("  Loading row_data from disk: {}", row_data_path);
            RowDataStore::new_disk(&row_data_path, U32RecordBatchSerializer::default())?
        } else {
            println!("  No row_data found, creating empty store");
            RowDataStore::new_memory(32)
        };

        let segment = Self {
            start: start_id,
            doc_id_gen: AtomicU32::new(doc_id_gen),
            max_doc_id: AtomicU32::new(max_doc_id_relative),
            persisted: AtomicBool::new(true), // 从磁盘加载 = 已持久化
            created_at: Instant::now(),       // 加载时使用当前时间
            pk_bloomfilter: RwLock::new(pk_bloomfilter),
            deleted: RwLock::new(deleted),
            fields: RwLock::new(fields),
            field_index,
            schema,
            row_data: RwLock::new(row_data),
            base_path: Some(segment_path),
        };

        println!("Frozen segment loaded in {:?}", start.elapsed());

        Ok(segment)
    }

    /// Check if segment is persisted to disk
    pub fn is_persisted(&self) -> bool {
        self.persisted.load(Ordering::Relaxed)
    }

    /// Get segment base path (if persisted)
    pub fn base_path(&self) -> Option<String> {
        self.base_path.clone()
    }

    /// Get current doc count in this segment
    pub fn doc_count(&self) -> u32 {
        self.doc_id_gen.load(Ordering::Relaxed)
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
        self.deleted.read().unwrap().len() as u64
    }

    /// Persist row_data to disk using BTree format (same as keyword index)
    /// Reorganizes data into fixed-size batches (default 100 docs per batch)
    /// Deleted documents are marked as NULL in the batch
    fn persist_row_data(
        &self,
        rowdata_path: &str,
        row_data: RowDataStore,
        deleted: &RoaringBitmap,
    ) -> CoreResult<()> {
        use arrow::compute::concat_batches;
        use mem_btree::persist::TreeWriter;

        // Extract memory BTree from RowDataStore
        let memory_tree = match row_data {
            RowDataStore::Memory(tree) => tree,
            RowDataStore::Disk(_) => {
                return Err(CoreError::Internal(
                    "Cannot persist disk row_data".to_string(),
                ));
            }
        };

        println!("  Reorganizing row data into fixed-size batches...");

        // Step 1: Collect all batches (avoid extracting individual rows)
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for item in memory_tree.iter() {
            let (_key, batch, _ttl) = &*item;
            all_batches.push(batch.clone());
        }

        // If no data, return early
        if all_batches.is_empty() {
            println!("    No data to persist");
            return Ok(());
        }

        // Merge all batches into one large batch
        let schema = all_batches[0].schema();
        let merged_batch = concat_batches(&schema, &all_batches)
            .map_err(|e| CoreError::Internal(format!("Failed to merge batches: {}", e)))?;

        println!(
            "    Total rows in merged batch: {}",
            merged_batch.num_rows()
        );

        // Step 2: Reorganize into fixed-size batches (1000 docs per batch for better performance)
        const BATCH_SIZE: usize = 1000;
        let total_docs = merged_batch.num_rows();
        let mut reorganized_batches: Vec<(u32, RecordBatch)> = Vec::new();

        // Get internal IDs array
        let internal_ids = arrow::array::cast::as_primitive_array::<arrow::datatypes::UInt32Type>(
            merged_batch.column(0),
        );

        for chunk_start in (0..total_docs).step_by(BATCH_SIZE) {
            let chunk_end = (chunk_start + BATCH_SIZE).min(total_docs);

            // Get the first doc_id as the batch key
            let batch_key = internal_ids.value(chunk_start);

            // Slice the batch for this chunk
            let chunk_batch = merged_batch.slice(chunk_start, chunk_end - chunk_start);

            reorganized_batches.push((batch_key, chunk_batch));
        }

        let batch_count = reorganized_batches.len();
        println!(
            "    Reorganized into {} batches ({}~{} docs per batch)",
            batch_count,
            if total_docs < BATCH_SIZE {
                total_docs
            } else {
                BATCH_SIZE
            },
            BATCH_SIZE
        );

        // Step 3: Persist reorganized batches to disk
        let iter = reorganized_batches
            .into_iter()
            .map(|(key, batch)| Arc::new((key, batch, None)));

        // Persist to disk using TreeWriter
        let serializer = U32RecordBatchSerializer::default();
        let writer = TreeWriter::new(
            std::path::PathBuf::from(rowdata_path),
            128, // chunk_size for BTree nodes
            0,   // key_len (0 for u32 keys)
        );

        writer
            .persist::<u32, RecordBatch, RecordBatch>(batch_count, Box::new(serializer), iter)
            .map_err(|e| CoreError::IOError(e.to_string()))?;

        Ok(())
    }

    /// Mark an entire RecordBatch as deleted (set all columns except internal_id to NULL)
    fn mark_batch_as_deleted(&self, batch: &RecordBatch) -> RecordBatch {
        use arrow::array::{make_array, ArrayData, ArrayRef};
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::{Field, Schema as ArrowSchema};

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
    #[allow(dead_code)]
    fn process_batch_for_deleted(
        &self,
        batch: &RecordBatch,
        deleted: &RoaringBitmap,
    ) -> RecordBatch {
        use arrow::array::{make_array, ArrayData, ArrayRef};
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::{Field, Schema as ArrowSchema};

        // If no deletions, return batch as-is
        if deleted.is_empty() {
            return batch.clone();
        }

        // Get the internal_id column (first column)
        let internal_id_col = batch.column(0);
        let internal_ids =
            arrow::array::cast::as_primitive_array::<arrow::datatypes::UInt32Type>(internal_id_col);

        // Check which rows in this batch are deleted
        let mut has_deleted = false;
        for id in internal_ids.values() {
            if deleted.contains(*id) {
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
        let num_rows = batch.num_rows();
        let new_columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(col_idx, col)| {
                // Build null buffer: true = valid, false = null
                let mut null_builder = vec![true; num_rows];
                for (row_idx, id) in internal_ids.values().iter().enumerate() {
                    if deleted.contains(*id) {
                        null_builder[row_idx] = false;
                    }
                }

                // If this is the internal_id column, keep it as-is (don't null it out)
                if col_idx == 0 {
                    return col.clone();
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
        use arrow::array::{make_array, ArrayRef, MutableArrayData};

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
