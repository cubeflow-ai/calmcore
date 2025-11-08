use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
};

use datafusion::arrow::{self as arrow, array::RecordBatch};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use tokio::sync::mpsc;

use crate::{
    schema::Schema,
    segment::{self, Segment},
    utils::{
        self, arrow_utils,
        error::{self, CoreError, CoreResult},
    },
};

pub struct WriteInfo(pub Vec<(Arc<Segment>, Vec<u32>)>);

/// 持久化通知回调
pub type PersistNotifyCallback = mpsc::UnboundedSender<u64>;

pub struct Partition {
    pub id: u64,
    current_segment: RwLock<Segment>,
    frozen_segments: RwLock<Vec<(u64, Arc<Segment>)>>, // (seg_id, segment)
    base_dir: PathBuf,
    schema: Arc<Schema>,
    pub arrow_schema: Arc<datafusion::arrow::datatypes::Schema>,
    read_lock: RwLock<()>,
    write_lock: Mutex<()>,
    segment_id_counter: AtomicU64,
    persist_notify: PersistNotifyCallback,
}

impl Partition {
    pub fn new(
        id: u64,
        base_dir: PathBuf,
        schema: Schema,
        persist_notify: mpsc::UnboundedSender<u64>,
    ) -> Self {
        let schema = Arc::new(schema);
        let arrow_schema = schema.to_arrow_schema();
        Partition {
            id,
            base_dir,
            schema: schema.clone(),
            arrow_schema,
            current_segment: RwLock::new(Segment::new(0, schema)),
            frozen_segments: RwLock::new(vec![]),
            read_lock: RwLock::new(()),
            write_lock: Mutex::new(()),
            segment_id_counter: AtomicU64::new(0),
            persist_notify,
        }
    }

    pub fn upsert_json(&self, data: &[serde_json::Value]) -> CoreResult<Vec<u64>> {
        // 将 JSON 数据转换为 RecordBatch
        let batch = arrow_utils::json_to_record_batch(data, self.arrow_schema.clone())?;
        // 调用现有的 upsert 方法
        self.upsert(batch)
    }

    /// 通过主键查询，返回 RecordBatch
    pub fn get_by_pk(&self, pk_values: &[&str]) -> CoreResult<Option<RecordBatch>> {
        // 检查是否有主键
        let _pk_name = self.schema.primary_key.as_ref().ok_or_else(|| {
            CoreError::InvalidParam("Schema does not have a primary key".to_string())
        })?;

        if pk_values.is_empty() {
            return Ok(None);
        }

        // 构建查询用的主键数组
        use datafusion::arrow::array::{ArrayRef, StringArray};

        let pk_array = Arc::new(StringArray::from(
            pk_values.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
        )) as ArrayRef;

        // 计算主键 hash
        let pk_hash = arrow_utils::array_to_hash(&pk_array);

        // 在所有 segments 中查找
        let _read_guard = self.read_lock.read().unwrap();

        let mut found_segments = Vec::new();
        let mut found_internal_ids = Vec::new();

        // 先查询 current segment
        {
            let current_segment = self.current_segment.read().unwrap();
            if let Some(ids) = current_segment.mget_internal_id(&pk_hash, &pk_array) {
                if !ids.is_empty() {
                    // 需要克隆 segment 的 Arc 引用
                    // 因为 current_segment 被 RwLock 保护，我们不能直接克隆它
                    // 所以我们收集内部 ID，稍后再读取数据
                    found_internal_ids.push((true, ids)); // true 表示是 current segment
                }
            }
        }

        // 查询 frozen segments
        let frozen_segments = self.frozen_segments.read().unwrap();
        for (_, segment) in frozen_segments.iter() {
            if let Some(ids) = segment.mget_internal_id(&pk_hash, &pk_array) {
                if !ids.is_empty() {
                    found_segments.push(segment.clone());
                    found_internal_ids.push((false, ids)); // false 表示是 frozen segment
                }
            }
        }
        drop(frozen_segments);

        // 如果没找到任何记录
        if found_internal_ids.is_empty() {
            return Ok(None);
        }

        // 从各个 segment 中读取数据并合并
        let mut all_batches = Vec::new();

        // 处理 current segment 的结果
        for (is_current, ids) in found_internal_ids.iter() {
            if *is_current {
                // 从 current segment 读取
                let current_segment = self.current_segment.read().unwrap();
                if let Some(batch) = current_segment.get_documents(ids)? {
                    all_batches.push(batch);
                }
            }
        }

        // 处理 frozen segments 的结果
        let mut frozen_idx = 0;
        for (is_current, ids) in found_internal_ids.iter() {
            if !*is_current {
                // 从对应的 frozen segment 读取
                if frozen_idx < found_segments.len() {
                    if let Some(batch) = found_segments[frozen_idx].get_documents(ids)? {
                        all_batches.push(batch);
                    }
                    frozen_idx += 1;
                }
            }
        }

        // 合并所有 RecordBatch
        if all_batches.is_empty() {
            return Ok(None);
        }

        if all_batches.len() == 1 {
            return Ok(Some(all_batches.into_iter().next().unwrap()));
        }

        // 多个 batch 需要合并
        let schema = all_batches[0].schema();
        let merged = arrow::compute::concat_batches(&schema, &all_batches)
            .map_err(|e| CoreError::Internal(format!("Failed to merge batches: {}", e)))?;

        Ok(Some(merged))
    }

    pub fn upsert(&self, data: RecordBatch) -> CoreResult<Vec<u64>> {
        match self.schema.primary_key.as_ref() {
            Some(pk) => {
                let column = data.column_by_name(pk).ok_or_else(|| {
                    CoreError::InvalidParam(format!("primary key:{:?} doesn't exist in data", pk))
                })?;

                let pk_hash = arrow_utils::array_to_hash(column);

                let _write_guard = self.write_lock.lock().unwrap();
                let segments = self.frozen_segments.read().unwrap().clone();

                let dels: Vec<(Arc<Segment>, Vec<u32>)> = segments
                    .par_iter()
                    .filter_map(|(_, segment)| {
                        segment
                            .mget_internal_id(pk_hash.as_ref(), column)
                            .and_then(|ids| Some((segment.clone(), ids)))
                    })
                    .collect();

                self.write(&data, Some(pk_hash), Some(WriteInfo(dels)))
            }
            None => self.write(&data, None, None),
        }
    }

    /// Flush current active segment to frozen list (non-blocking write)
    /// The segment becomes immediately readable but not yet persisted to disk
    pub fn flush(&self, check: bool) -> CoreResult<u64> {
        let mut current = self.current_segment.write().unwrap();

        if check && current.doc_count() < self.schema.persist_policy.max_docs_per_segment {
            return Ok(0);
        }

        // 1. Get current segment info
        let (seg_id, next_start, total_count) = {
            // Check if current segment has data
            if current.total_count() == 0 {
                return Ok(0);
            }

            let seg_id = self.segment_id_counter.fetch_add(1, Ordering::SeqCst);

            (seg_id, current.next_doc_id(), current.total_count())
        };

        println!("Flushing segment {} with {} records", seg_id, total_count);

        // 2. Replace current with new segment (this is the only blocking part)
        let old_segment = {
            let new_segment = Segment::new(next_start, self.schema.clone());
            std::mem::replace(&mut *current, new_segment)
        };

        // 3. Move old segment to frozen list (immediately readable)
        let old_segment_arc = Arc::new(old_segment);
        {
            let mut segments = self.frozen_segments.write().unwrap();
            segments.push((seg_id, old_segment_arc.clone()));
        }

        // 4. flush notify Engine to check for persist
        self.persist_notify.send(self.id);

        Ok(seg_id)
    }

    /// Get list of segments that need to be persisted (not yet persisted)
    /// 这些 segment 还在内存中，需要持久化以释放内存
    pub fn get_unpersisted_segments(&self) -> Vec<(u64, Arc<Segment>)> {
        let segments = self.frozen_segments.read().unwrap();
        segments
            .iter()
            .filter(|(_, segment)| !segment.is_persisted())
            .map(|(seg_id, segment)| (*seg_id, segment.clone()))
            .collect()
    }

    /// 自动持久化所有未持久化的 segments
    /// 这个方法应该被定期调用以释放内存
    pub fn persist_unpersisted_segments(&self) -> CoreResult<Vec<u64>> {
        let unpersisted = self.get_unpersisted_segments();

        if unpersisted.is_empty() {
            return Ok(vec![]);
        }

        println!(
            "发现 {} 个未持久化的 segments，开始持久化...",
            unpersisted.len()
        );

        let mut persisted_ids = Vec::new();
        for (seg_id, _) in unpersisted {
            match self.persist_segment(seg_id) {
                Ok(_) => {
                    persisted_ids.push(seg_id);
                    println!("  Segment {} 持久化成功", seg_id);
                }
                Err(e) => {
                    eprintln!("  Segment {} 持久化失败: {:?}", seg_id, e);
                }
            }
        }

        println!("持久化完成，共 {} 个 segments", persisted_ids.len());
        Ok(persisted_ids)
    }

    /// Persist a specific segment to disk by segment ID
    pub fn persist_segment(&self, seg_id: u64) -> CoreResult<()> {
        let base_dir = self
            .base_dir
            .to_str()
            .ok_or_else(|| CoreError::Internal("Invalid base_dir path".to_string()))?;

        let partition_path = format!("{}/partition-{}", base_dir, self.id);
        std::fs::create_dir_all(&partition_path)
            .map_err(|e| CoreError::IOError(format!("Failed to create partition dir: {}", e)))?;

        // Find the frozen segment
        let segments = self.frozen_segments.read().unwrap();
        let (_, segment) = segments
            .iter()
            .find(|(id, _)| *id == seg_id)
            .ok_or_else(|| CoreError::Internal(format!("Segment {} not found", seg_id)))?;

        println!("Persisting segment {} to disk", seg_id);

        // Collect all frozen segments as history (for snapshot their deleted bitmaps)
        let history_segments: Vec<(u64, Arc<Segment>)> = segments
            .iter()
            .filter(|(id, _)| *id != seg_id) // Exclude current segment
            .map(|(id, seg)| (*id, seg.clone()))
            .collect();

        // Persist the segment with history deletes snapshot
        segment.persist(&partition_path, seg_id, &history_segments)?;

        println!("Segment {} persisted successfully", seg_id);

        Ok(())
    }

    /// Recover from incomplete persist operations
    ///
    /// This handles two scenarios:
    /// 1. Incomplete temp directories (segment-X_tmp): Remove them
    /// 2. Incomplete file replacements (deleted_seg_X in segment directories): Complete them
    fn recover_incomplete_persists(partition_path: &str) -> CoreResult<()> {
        if let Ok(entries) = std::fs::read_dir(partition_path) {
            for entry in entries.flatten() {
                let entry_path = entry.path();
                let dir_name = entry.file_name();
                let dir_name_str = dir_name.to_string_lossy();

                // 1. Remove incomplete temp directories
                if dir_name_str.ends_with("_tmp") {
                    println!("  Removing incomplete temp directory: {}", dir_name_str);
                    let _ = std::fs::remove_dir_all(&entry_path);
                    continue;
                }

                // 2. Complete file replacements for completed segments
                if dir_name_str.starts_with("segment-") && !dir_name_str.ends_with("_tmp") {
                    if !entry_path.is_dir() {
                        continue;
                    }

                    // Look for deleted_seg_* files that need to be moved
                    if let Ok(seg_entries) = std::fs::read_dir(&entry_path) {
                        for seg_entry in seg_entries.flatten() {
                            let file_name = seg_entry.file_name();
                            let file_name_str = file_name.to_string_lossy();

                            if file_name_str.starts_with("deleted_seg_") {
                                // Extract target segment ID
                                if let Some(target_seg_id_str) =
                                    file_name_str.strip_prefix("deleted_seg_")
                                {
                                    println!(
                                        "  Completing file replacement: {} → segment-{}/deleted",
                                        file_name_str, target_seg_id_str
                                    );

                                    let source_path = seg_entry.path();
                                    let target_path = format!(
                                        "{}/segment-{}/deleted",
                                        partition_path, target_seg_id_str
                                    );

                                    // Replace (overwrite) the target file
                                    if let Err(e) = std::fs::rename(&source_path, &target_path) {
                                        eprintln!(
                                            "    Warning: Failed to complete replacement: {}",
                                            e
                                        );
                                    } else {
                                        println!("    Completed ✓");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Load a partition from disk
    pub fn load(
        id: u64,
        base_dir: PathBuf,
        schema: Schema,
        persist_notify: PersistNotifyCallback,
    ) -> CoreResult<Self> {
        let schema = Arc::new(schema);
        let base_dir_str = base_dir
            .to_str()
            .ok_or_else(|| CoreError::Internal("Invalid base_dir path".to_string()))?;

        println!("\n=== Loading Partition {} from {} ===", id, base_dir_str);

        // 1. Find all segment directories
        let partition_path = format!("{}/partition-{}", base_dir_str, id);
        if !std::path::Path::new(&partition_path).exists() {
            // No persisted data, create new partition
            return Ok(Self::new(
                id,
                base_dir,
                schema.as_ref().clone(),
                persist_notify,
            ));
        }

        // 1.5. Crash recovery: Clean up incomplete persists and complete file replacements
        println!("Checking for incomplete persists...");
        Self::recover_incomplete_persists(&partition_path)?;

        // Scan for segment directories with new format: segment-{start}-{end}
        let mut segment_ranges: Vec<(u64, u64)> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&partition_path) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let name = entry.file_name();
                    let name_str = name.to_str().unwrap_or("");

                    // Skip temporary directories
                    if name_str.ends_with("_tmp") {
                        continue;
                    }

                    // Parse segment-{start}-{end}
                    if let Some(range_str) = name_str.strip_prefix("segment-") {
                        let parts: Vec<&str> = range_str.split('-').collect();
                        if parts.len() == 2 {
                            if let (Ok(start), Ok(end)) =
                                (parts[0].parse::<u64>(), parts[1].parse::<u64>())
                            {
                                segment_ranges.push((start, end));
                            }
                        }
                    }
                }
            }
        }

        segment_ranges.sort();
        println!(
            "Found {} frozen segments: {:?}",
            segment_ranges.len(),
            segment_ranges
        );

        // 2. Load all frozen segments (all are already persisted)
        let mut frozen_segments = Vec::new();
        for (start_id, end_id) in &segment_ranges {
            let segment =
                Segment::load_frozen(&partition_path, *start_id, *end_id, schema.clone())?;
            // Use start_id as the key
            frozen_segments.push((*start_id, Arc::new(segment)));
        }

        // 3. Calculate next start offset (no more segment_id_counter)
        let next_start = segment_ranges
            .last()
            .map(|(_start, end)| *end) // Next segment starts where last ended
            .unwrap_or(0);

        println!("Next doc_id start: {}", next_start);

        // 4. Create new active segment
        let current_segment = Segment::new(next_start, schema.clone());

        // segment_id_counter is deprecated, but keep field for compatibility
        // It's no longer used since segments are identified by their start_id
        let deprecated_counter = next_start; // Use start_id as counter for now

        let arrow_schema = schema.to_arrow_schema();
        Ok(Partition {
            id: id as u64,
            base_dir,
            schema,
            arrow_schema,
            current_segment: RwLock::new(current_segment),
            frozen_segments: RwLock::new(frozen_segments),
            read_lock: RwLock::new(()),
            write_lock: Mutex::new(()),
            segment_id_counter: AtomicU64::new(deprecated_counter),
            persist_notify,
        })
    }

    fn write(
        &self,
        data: &RecordBatch,
        pk_hash: Option<Vec<u32>>,
        info: Option<WriteInfo>,
    ) -> CoreResult<Vec<u64>> {
        let current_segment = self.current_segment.read().unwrap();

        // 写入数据
        let result = current_segment
            .write(data, pk_hash, info, &self.read_lock)
            .map(|ids| {
                ids.into_iter()
                    .map(|v| v as u64 + current_segment.start)
                    .collect()
            });

        // 写入后轻量级检查：是否需要 flush（自动执行）
        // 只检查文档数，不检查时间（时间检查交给 Engine 定期任务）
        let should_flush =
            current_segment.doc_count() >= self.schema.persist_policy.max_docs_per_segment;

        // 释放读锁
        drop(current_segment);

        if should_flush {
            if let Err(e) = self.flush(true) {
                log::error!("flush has error :[{:?}]", e);
            }
        }

        result
    }

    pub fn total_count(&self) -> u64 {
        let count: u64 = self
            .frozen_segments
            .read()
            .unwrap()
            .iter()
            .map(|(_, segment)| segment.total_count())
            .sum();
        count + self.current_segment.read().unwrap().total_count()
    }

    pub fn segment_count(&self) -> usize {
        self.frozen_segments.read().unwrap().len()
    }

    /// 获取 Partition ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get read-only access to the current segment
    /// Used by PartitionTableProvider for query execution
    pub fn get_current_segment(&self) -> std::sync::RwLockReadGuard<'_, Segment> {
        self.current_segment.read().unwrap()
    }

    /// Get read-only access to the frozen segments
    /// Used by PartitionTableProvider for query execution
    pub fn get_frozen_segments(&self) -> std::sync::RwLockReadGuard<'_, Vec<(u64, Arc<Segment>)>> {
        self.frozen_segments.read().unwrap()
    }

    /// 持久化 Partition 的所有数据（同步操作）
    ///
    /// 执行步骤：
    /// 1. Flush 当前活跃 segment
    /// 2. 持久化所有未持久化的 segments
    ///
    /// 调用后保证：所有数据已写入磁盘
    pub fn persist_all(&self) -> CoreResult<()> {
        println!("[Partition {}] Starting persist_all...", self.id);

        // 1. Flush 当前活跃 segment
        let current_count = self.current_segment.read().unwrap().doc_count();
        if current_count > 0 {
            println!(
                "[Partition {}] Flushing current segment ({} records)",
                self.id, current_count
            );
            self.flush(false)?;
        }

        // 2. 持久化所有未持久化的 segments
        let unpersisted = self.get_unpersisted_segments();
        if unpersisted.is_empty() {
            println!("[Partition {}] All segments already persisted", self.id);
            return Ok(());
        }

        println!(
            "[Partition {}] Persisting {} segments...",
            self.id,
            unpersisted.len()
        );

        self.persist_unpersisted_segments()?;

        // 3. 验证
        let remaining = self.get_unpersisted_segments();
        if remaining.is_empty() {
            println!("[Partition {}] ✅ All data persisted", self.id);
            Ok(())
        } else {
            Err(CoreError::Internal(format!(
                "Partition {} still has {} unpersisted segments",
                self.id,
                remaining.len()
            )))
        }
    }
}
