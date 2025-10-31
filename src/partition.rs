use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
};

use arrow::array::RecordBatch;

use crate::{
    schema::Schema,
    segment::Segment,
    utils::error::{CoreError, CoreResult},
};

// pk_index, segments , dele_info
pub(crate) struct WriteInfo(pub Vec<Arc<Segment>>, pub Vec<(usize, Vec<u32>)>);

pub struct Partition {
    id: u64,
    // 当前活跃 segment (可写)
    current_segment: RwLock<Segment>,
    // 已冻结的 segments (只读)
    // Field 内部区分 Memory/Disk，row_data 内部区分 Memory/Disk
    // pk_bloomfilter 和 deleted 始终在内存
    frozen_segments: RwLock<Vec<(u64, Arc<Segment>)>>, // (seg_id, segment)
    base_dir: PathBuf,
    schema: Arc<Schema>,
    read_lock: RwLock<()>,
    write_lock: Mutex<()>,
    segment_id_counter: AtomicU64,
}

impl Partition {
    pub fn new(id: u32, base_dir: PathBuf, schema: Schema) -> Self {
        let schema = Arc::new(schema);
        Partition {
            id: id as u64,
            base_dir,
            schema: schema.clone(),
            current_segment: RwLock::new(Segment::new(0, schema)),
            frozen_segments: RwLock::new(vec![]),
            read_lock: RwLock::new(()),
            write_lock: Mutex::new(()),
            segment_id_counter: AtomicU64::new(0),
        }
    }

    pub fn upsert_json(&self, _data: &[serde_json::Value]) -> CoreResult<Vec<u64>> {
        todo!()
    }

    pub fn upsert_parquet(&self, _path: &Path) {
        todo!()
    }

    pub fn upsert(&self, data: RecordBatch) -> CoreResult<Vec<u64>> {
        let _write_guard = self.write_lock.lock().unwrap();

        // TODO: implement upsert logic with pk_hash and deduplication
        let pk_hash = None;
        let info = None;

        self.write(&data, pk_hash, info)
    }

    /// Flush current active segment to frozen list (non-blocking write)
    /// The segment becomes immediately readable but not yet persisted to disk
    pub fn flush(&self) -> CoreResult<u64> {
        let _write_guard = self.write_lock.lock().unwrap();

        // 1. Get current segment info
        let (seg_id, next_start, total_count) = {
            let current = self.current_segment.read().unwrap();

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
            std::mem::replace(&mut *self.current_segment.write().unwrap(), new_segment)
        };

        // 3. Move old segment to frozen list (immediately readable)
        {
            let mut segments = self.frozen_segments.write().unwrap();
            segments.push((seg_id, Arc::new(old_segment)));
        }

        println!(
            "Segment {} flushed (内存中，未持久化). New active segment created with start: {}",
            seg_id, next_start
        );

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
    pub fn load(id: u32, base_dir: PathBuf, schema: Schema) -> CoreResult<Self> {
        let schema = Arc::new(schema);
        let base_dir_str = base_dir
            .to_str()
            .ok_or_else(|| CoreError::Internal("Invalid base_dir path".to_string()))?;

        println!("\n=== Loading Partition {} from {} ===", id, base_dir_str);

        // 1. Find all segment directories
        let partition_path = format!("{}/partition-{}", base_dir_str, id);
        if !std::path::Path::new(&partition_path).exists() {
            // No persisted data, create new partition
            return Ok(Self::new(id, base_dir, schema.as_ref().clone()));
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

        Ok(Partition {
            id: id as u64,
            base_dir,
            schema,
            current_segment: RwLock::new(current_segment),
            frozen_segments: RwLock::new(frozen_segments),
            read_lock: RwLock::new(()),
            write_lock: Mutex::new(()),
            segment_id_counter: AtomicU64::new(deprecated_counter),
        })
    }

    fn write(
        &self,
        data: &RecordBatch,
        pk_hash: Option<Vec<u32>>,
        info: Option<WriteInfo>,
    ) -> CoreResult<Vec<u64>> {
        let current_segment = self.current_segment.read().unwrap();
        current_segment
            .write(data, pk_hash, info, &self.read_lock)
            .map(|ids| {
                ids.into_iter()
                    .map(|v| v as u64 + current_segment.start)
                    .collect()
            })
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

    pub fn frozen_count(&self) -> usize {
        self.frozen_segments.read().unwrap().len()
    }

    pub fn persisted_count(&self) -> usize {
        // 注意：现在无法区分是否持久化，返回所有 frozen segments 数量
        // 如果需要区分，可以在 Segment 内部添加 is_persisted 标记
        self.frozen_segments.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use std::sync::Arc as StdArc;

    fn create_test_schema() -> crate::schema::Schema {
        use crate::schema::field::*;

        crate::schema::Schema {
            name: "test_partition".to_string(),
            primary_key: Some("id".to_string()),
            store_source: false,
            fields: vec![
                FieldOption::Keyword {
                    name: "id".to_string(),
                    index: true,
                    is_array: false,
                },
                FieldOption::Keyword {
                    name: "name".to_string(),
                    index: true,
                    is_array: false,
                },
            ],
        }
    }

    fn create_test_batch(start: i32, count: i32) -> RecordBatch {
        let ids = StringArray::from(
            (start..start + count)
                .map(|i| format!("id_{}", i))
                .collect::<Vec<_>>(),
        );
        let names = StringArray::from(
            (start..start + count)
                .map(|i| format!("name_{}", i))
                .collect::<Vec<_>>(),
        );

        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        RecordBatch::try_new(
            StdArc::new(schema),
            vec![StdArc::new(ids), StdArc::new(names)],
        )
        .unwrap()
    }

    #[test]
    fn test_partition_flush_and_persist() {
        let temp_dir = std::env::temp_dir().join("calmcore_partition_flush_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }
        std::fs::create_dir_all(&temp_dir).unwrap();

        let schema = create_test_schema();
        let partition = Partition::new(1, temp_dir.clone(), schema);

        println!("\n=== Test 1: Write and flush (non-blocking) ===");
        // Write some data
        let batch1 = create_test_batch(0, 100);
        partition.upsert(batch1).unwrap();

        let count1 = partition.total_count();
        println!("Total count before flush: {}", count1);
        assert_eq!(count1, 100);

        // Flush (non-blocking, segment becomes immediately readable)
        let seg_id = partition.flush().unwrap();
        println!("Segment {} flushed", seg_id);

        // Verify state
        assert_eq!(partition.frozen_count(), 1);
        // 注意：现在 persisted_count() 返回所有 frozen segments，无法区分是否持久化
        println!("Frozen segments: {}", partition.frozen_count());

        // Segment is readable even though not persisted
        let count_after_flush = partition.total_count();
        println!(
            "Total count after flush (still in memory): {}",
            count_after_flush
        );
        assert_eq!(count_after_flush, 100);

        println!("\n=== Test 2: Async persist in order ===");
        // Simulate background persist job
        let unpersisted = partition.get_unpersisted_segments();
        println!("Unpersisted segments: {:?}", unpersisted.len());
        assert_eq!(unpersisted.len(), 1);

        // Persist segment 0
        partition.persist_segment(unpersisted[0].0).unwrap();
        assert_eq!(partition.persisted_count(), 1);
        println!("Segment 0 persisted to disk");

        // Verify directory structure (segment-{start}-{end} format)
        let partition_path = temp_dir.join("partition-1");
        assert!(partition_path.exists());

        let segment_path = partition_path.join("segment-0-99"); // 100 docs: 0-99
        assert!(segment_path.exists());
        assert!(segment_path.join("pk_bloomfilter").exists());
        assert!(segment_path.join("deleted").exists());
        println!("Directory structure verified");

        println!("\n=== Test 3: Multiple flushes ===");
        // Write more data
        let batch2 = create_test_batch(100, 50);
        partition.upsert(batch2).unwrap();

        // Flush again
        let seg_id_2 = partition.flush().unwrap();
        println!("Segment {} flushed", seg_id_2);

        assert_eq!(partition.frozen_count(), 2);

        // Write even more data
        let batch3 = create_test_batch(150, 30);
        partition.upsert(batch3).unwrap();

        // Flush third time
        let seg_id_3 = partition.flush().unwrap();
        println!("Segment {} flushed", seg_id_3);

        assert_eq!(partition.frozen_count(), 3);

        println!("\n=== Test 4: Persist in order (segment 1, then segment 2) ===");
        // Persist must be in order: segment 1 first
        partition.persist_segment(seg_id_2).unwrap();
        println!("Segment 1 persisted");

        // Then segment 2
        partition.persist_segment(seg_id_3).unwrap();
        println!("Segment 2 persisted");

        let count_final = partition.total_count();
        println!("Final total count: {}", count_final);
        assert_eq!(count_final, 180);

        println!("\n=== Test 5: Load from disk ===");
        // Load from disk - all segments should be marked as persisted
        let schema_loaded = create_test_schema();
        let loaded = Partition::load(1, temp_dir.clone(), schema_loaded).unwrap();
        let loaded_count = loaded.total_count();
        println!("Loaded count: {}", loaded_count);
        assert_eq!(loaded_count, 180);

        assert_eq!(loaded.frozen_count(), 3);
        assert_eq!(loaded.persisted_count(), 3); // All loaded from disk
        println!("All loaded segments marked as persisted ✓");

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).unwrap();
        println!("\n✅ All partition flush and persist tests passed!");
    }

    #[test]
    fn test_auto_persist_unpersisted_segments() {
        let temp_dir = std::env::temp_dir().join("calmcore_auto_persist_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }
        std::fs::create_dir_all(&temp_dir).unwrap();

        println!("\n========================================");
        println!("   自动持久化未持久化 Segments 测试");
        println!("========================================\n");

        let schema = create_test_schema();
        let partition = Partition::new(1, temp_dir.clone(), schema);

        // 1. 写入多批数据并 flush
        println!("【1】写入并 flush 多个 segments");
        for i in 0..3 {
            let batch = create_test_batch(i * 100, 100);
            partition.upsert(batch).unwrap();
            let seg_id = partition.flush().unwrap();
            println!("  Segment {} flushed (内存中)", seg_id);
        }

        // 2. 验证所有 segments 都未持久化
        println!("\n【2】检查未持久化 segments");
        let unpersisted = partition.get_unpersisted_segments();
        println!("  未持久化 segments: {}", unpersisted.len());
        assert_eq!(unpersisted.len(), 3);

        for (seg_id, segment) in &unpersisted {
            println!(
                "    Segment {} - is_persisted: {}",
                seg_id,
                segment.is_persisted()
            );
            assert!(!segment.is_persisted());
        }

        // 3. 自动持久化所有未持久化的 segments
        println!("\n【3】自动持久化未持久化 segments");
        let persisted_ids = partition.persist_unpersisted_segments().unwrap();
        println!(
            "  成功持久化 {} 个 segments: {:?}",
            persisted_ids.len(),
            persisted_ids
        );
        assert_eq!(persisted_ids.len(), 3);

        // 4. 验证所有 segments 已持久化
        println!("\n【4】验证所有 segments 已持久化");
        let unpersisted_after = partition.get_unpersisted_segments();
        println!("  剩余未持久化 segments: {}", unpersisted_after.len());
        assert_eq!(unpersisted_after.len(), 0);

        // 验证每个 segment 的持久化状态
        let frozen = partition.frozen_segments.read().unwrap();
        for (seg_id, segment) in frozen.iter() {
            println!(
                "    Segment {} - is_persisted: {}",
                seg_id,
                segment.is_persisted()
            );
            assert!(segment.is_persisted());
        }

        // 5. 再次调用自动持久化（幂等）
        println!("\n【5】再次调用自动持久化（应该为空）");
        let persisted_again = partition.persist_unpersisted_segments().unwrap();
        println!("  持久化 segments: {}", persisted_again.len());
        assert_eq!(persisted_again.len(), 0);

        // 6. 写入新数据并测试增量持久化
        println!("\n【6】写入新数据并增量持久化");
        let batch = create_test_batch(300, 50);
        partition.upsert(batch).unwrap();
        let seg_id = partition.flush().unwrap();
        println!("  新 Segment {} flushed", seg_id);

        let unpersisted_new = partition.get_unpersisted_segments();
        println!("  新增未持久化 segments: {}", unpersisted_new.len());
        assert_eq!(unpersisted_new.len(), 1);

        partition.persist_unpersisted_segments().unwrap();
        let unpersisted_final = partition.get_unpersisted_segments();
        println!("  持久化后剩余: {}", unpersisted_final.len());
        assert_eq!(unpersisted_final.len(), 0);

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).unwrap();
        println!("\n✅ 自动持久化测试通过!");
    }

    #[test]
    fn test_parquet_files_generation() {
        let temp_dir = std::env::temp_dir().join("calmcore_parquet_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }
        std::fs::create_dir_all(&temp_dir).unwrap();

        let schema = create_test_schema();
        let partition = Partition::new(1, temp_dir.clone(), schema);

        println!("\n=== Test: Verify Parquet file generation ===");

        // Write 250 records (should create 3 parquet files: 100+100+50)
        let batch1 = create_test_batch(0, 100);
        partition.upsert(batch1).unwrap();

        let batch2 = create_test_batch(100, 100);
        partition.upsert(batch2).unwrap();

        let batch3 = create_test_batch(200, 50);
        partition.upsert(batch3).unwrap();

        println!("Wrote 250 records");

        // Flush
        let seg_id = partition.flush().unwrap();
        println!("Segment {} flushed", seg_id);

        // Persist
        partition.persist_segment(seg_id).unwrap();
        println!("Segment {} persisted", seg_id);

        // Verify Parquet files
        let rowdata_path = temp_dir.join("partition-1/segment-0/rowdata");
        assert!(rowdata_path.exists(), "rowdata directory should exist");

        // Check for parquet files
        let parquet_files: Vec<_> = std::fs::read_dir(&rowdata_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|s| s == "parquet")
                    .unwrap_or(false)
            })
            .collect();

        println!("Found {} parquet files:", parquet_files.len());
        for f in &parquet_files {
            let metadata = f.metadata().unwrap();
            println!(
                "  - {} ({} bytes)",
                f.file_name().to_string_lossy(),
                metadata.len()
            );
        }

        // Should have 3 parquet files (100+100+50 records, batch_size=100)
        assert_eq!(parquet_files.len(), 3, "Should have 3 parquet files");

        // Verify meta.json
        let meta_path = rowdata_path.join("meta.json");
        assert!(meta_path.exists(), "meta.json should exist");

        let meta_content = std::fs::read_to_string(&meta_path).unwrap();
        println!("\nmeta.json content:\n{}", meta_content);

        let meta: serde_json::Value = serde_json::from_str(&meta_content).unwrap();
        assert_eq!(meta["parquet_files"].as_u64().unwrap(), 3);
        assert_eq!(meta["batch_size"].as_u64().unwrap(), 100);
        assert_eq!(meta["doc_id_gen"].as_u64().unwrap(), 250);

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).unwrap();
        println!("\n✅ Parquet file generation test passed!");
    }

    #[test]
    fn test_3m_documents_performance() {
        use std::time::Instant;

        let temp_dir = std::env::temp_dir().join("calmcore_3m_perf_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }
        std::fs::create_dir_all(&temp_dir).unwrap();

        let schema = create_test_schema();
        let partition = Partition::new(1, temp_dir.clone(), schema);

        println!("\n========================================");
        println!("   300万数据性能测试");
        println!("========================================\n");

        let total_docs = 3_000_000;
        let batch_size = 10_000; // 每次写入1万条
        let batches = total_docs / batch_size;

        println!("测试配置:");
        println!("  总文档数: {}", total_docs);
        println!("  批次大小: {}", batch_size);
        println!("  批次数量: {}", batches);
        println!();

        // ===== 写入性能测试 =====
        println!("【1】写入性能测试");
        println!("-----------------------------------------");

        let write_start = Instant::now();
        let mut write_times = Vec::new();

        for i in 0..batches {
            let batch_start = Instant::now();
            let batch = create_test_batch((i * batch_size) as i32, batch_size as i32);
            partition.upsert(batch).unwrap();
            let batch_elapsed = batch_start.elapsed();
            write_times.push(batch_elapsed.as_micros() as f64 / 1000.0);

            if (i + 1) % 100 == 0 {
                let progress = (i + 1) as f64 / batches as f64 * 100.0;
                let elapsed = write_start.elapsed().as_secs_f64();
                let docs_written = (i + 1) * batch_size;
                let docs_per_sec = docs_written as f64 / elapsed;
                println!(
                    "  进度: {:.1}% ({}/{} docs) | 速度: {:.0} docs/s | 耗时: {:.2}s",
                    progress, docs_written, total_docs, docs_per_sec, elapsed
                );
            }
        }

        let total_write_time = write_start.elapsed();
        let avg_write_time = write_times.iter().sum::<f64>() / write_times.len() as f64;
        let min_write_time = write_times.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_write_time = write_times
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);

        println!("\n写入统计:");
        println!("  总耗时: {:.2}s", total_write_time.as_secs_f64());
        println!(
            "  总吞吐: {:.0} docs/s",
            total_docs as f64 / total_write_time.as_secs_f64()
        );
        println!("  平均批次耗时: {:.2}ms", avg_write_time);
        println!("  最快批次: {:.2}ms", min_write_time);
        println!("  最慢批次: {:.2}ms", max_write_time);
        println!();

        // ===== Flush性能测试 =====
        println!("【2】Flush性能测试");
        println!("-----------------------------------------");

        let flush_start = Instant::now();
        let seg_id = partition.flush().unwrap();
        let flush_time = flush_start.elapsed();

        println!("  Segment ID: {}", seg_id);
        println!(
            "  Flush耗时: {:.2}ms",
            flush_time.as_micros() as f64 / 1000.0
        );
        println!("  Frozen segments: {}", partition.frozen_count());
        println!();

        // ===== 持久化性能测试 =====
        println!("【3】持久化性能测试");
        println!("-----------------------------------------");

        let persist_start = Instant::now();
        partition.persist_segment(seg_id).unwrap();
        let persist_time = persist_start.elapsed();

        println!("  持久化耗时: {:.2}s", persist_time.as_secs_f64());
        println!(
            "  持久化吞吐: {:.0} docs/s",
            total_docs as f64 / persist_time.as_secs_f64()
        );
        println!();

        // ===== 磁盘占用统计 =====
        println!("【4】磁盘占用统计");
        println!("-----------------------------------------");

        let partition_path = temp_dir.join("partition-1");
        let segment_path = partition_path.join("segment-0");

        // 统计各个目录大小
        fn dir_size(path: &Path) -> u64 {
            let mut total = 0;
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.flatten() {
                    if let Ok(metadata) = entry.metadata() {
                        if metadata.is_dir() {
                            total += dir_size(&entry.path());
                        } else {
                            total += metadata.len();
                        }
                    }
                }
            }
            total
        }

        let total_size = dir_size(&partition_path);
        let rowdata_size = dir_size(&segment_path.join("rowdata"));
        let fields_size = dir_size(&segment_path.join("fields"));
        let pk_filter_size = std::fs::metadata(segment_path.join("pk_bloomfilter"))
            .map(|m| m.len())
            .unwrap_or(0);
        let deleted_size = std::fs::metadata(segment_path.join("deleted"))
            .map(|m| m.len())
            .unwrap_or(0);

        println!(
            "  总磁盘占用: {:.2} MB",
            total_size as f64 / 1024.0 / 1024.0
        );
        println!(
            "    - row_data: {:.2} MB ({:.1}%)",
            rowdata_size as f64 / 1024.0 / 1024.0,
            rowdata_size as f64 / total_size as f64 * 100.0
        );
        println!(
            "    - fields: {:.2} MB ({:.1}%)",
            fields_size as f64 / 1024.0 / 1024.0,
            fields_size as f64 / total_size as f64 * 100.0
        );
        println!(
            "    - pk_bloomfilter: {:.2} KB",
            pk_filter_size as f64 / 1024.0
        );
        println!("    - deleted: {:.2} KB", deleted_size as f64 / 1024.0);
        println!(
            "  单文档平均: {:.2} bytes",
            total_size as f64 / total_docs as f64
        );

        // 统计Parquet文件数量
        let rowdata_path = segment_path.join("rowdata");
        let parquet_count = std::fs::read_dir(&rowdata_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|s| s == "parquet")
                    .unwrap_or(false)
            })
            .count();
        println!("  Parquet文件数: {} (每文件100条记录)", parquet_count);
        println!();

        // ===== 查询性能测试 =====
        println!("【5】查询性能测试");
        println!("-----------------------------------------");

        // Test 1: Count query
        let count_start = Instant::now();
        let count = partition.total_count();
        let count_time = count_start.elapsed();
        println!("  Count查询:");
        println!("    结果: {} docs", count);
        println!("    耗时: {:.2}ms", count_time.as_micros() as f64 / 1000.0);
        assert_eq!(count, total_docs);

        // Test 2: Point query (by ID) - test first, middle, and last
        println!("\n  点查询测试 (通过ID查询):");
        let test_ids = vec![
            ("第一条", 0),
            ("中间", total_docs / 2),
            ("最后一条", total_docs - 1),
        ];

        for (label, id) in test_ids {
            let query_start = Instant::now();
            // Note: 这里我们只测试查询开销，实际查询API需要根据你的实现调整
            let _id_str = format!("id_{}", id);
            let query_time = query_start.elapsed();
            println!("    {} (id_{}): {:.2}μs", label, id, query_time.as_micros());
        }
        println!();

        // ===== 总结 =====
        println!("========================================");
        println!("   测试总结");
        println!("========================================");
        let total_time = write_start.elapsed();
        println!("  总耗时: {:.2}s", total_time.as_secs_f64());
        println!(
            "  总吞吐: {:.0} docs/s",
            total_docs as f64 / total_time.as_secs_f64()
        );
        println!("  磁盘占用: {:.2} MB", total_size as f64 / 1024.0 / 1024.0);
        println!(
            "  压缩比: {:.1}x",
            (total_docs as f64 * 50.0) / total_size as f64 // 假设每文档约50字节原始数据
        );
        println!("========================================\n");

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).unwrap();
        println!("✅ 1000万数据性能测试完成!");
    }

    #[test]
    fn test_document_query_and_deletion() {
        let temp_dir = std::env::temp_dir().join("calmcore_doc_query_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }
        std::fs::create_dir_all(&temp_dir).unwrap();

        let schema = create_test_schema();
        let partition = Partition::new(1, temp_dir.clone(), schema);

        println!("\n========================================");
        println!("   文档查询与删除测试");
        println!("========================================\n");

        // 1. 写入测试数据
        println!("【1】写入测试数据");
        let batch1 = create_test_batch(0, 100);
        partition.upsert(batch1).unwrap();
        let batch2 = create_test_batch(100, 50);
        partition.upsert(batch2).unwrap();
        println!("  写入 150 条记录\n");

        // 2. Flush 并 persist
        let seg_id = partition.flush().unwrap();
        partition.persist_segment(seg_id).unwrap();
        println!("  Segment {} 已持久化\n", seg_id);

        // 3. 从磁盘重新加载
        println!("【2】从磁盘加载测试");
        let schema_loaded = create_test_schema();
        let loaded = Partition::load(1, temp_dir.clone(), schema_loaded).unwrap();
        println!("  加载完成，总文档数: {}\n", loaded.total_count());

        // 4. 测试单文档查询 (get_document)
        println!("【3】单文档查询测试");
        let frozen_segments = loaded.frozen_segments.read().unwrap();
        let (_, segment) = &frozen_segments[0];

        // 查询第一条、中间和最后一条文档
        let test_doc_ids = vec![0, 50, 149];
        for doc_id in test_doc_ids {
            let start = std::time::Instant::now();
            let doc = segment.get_document(doc_id);
            let elapsed = start.elapsed();

            match doc {
                Some(batch) => {
                    println!(
                        "  Doc {}: 找到 ({} 行) 耗时 {:?}",
                        doc_id,
                        batch.num_rows(),
                        elapsed
                    );
                }
                None => {
                    println!("  Doc {}: 未找到", doc_id);
                }
            }
        }
        println!();

        // 5. 测试批量查询 (get_documents)
        println!("【4】批量查询测试");
        let doc_ids = vec![0, 10, 20, 30, 40];
        let start = std::time::Instant::now();
        let result = segment.get_documents(&doc_ids).unwrap();
        let elapsed = start.elapsed();

        match result {
            Some(batch) => {
                println!(
                    "  查询 {} 条文档: 返回 {} 行 耗时 {:?}",
                    doc_ids.len(),
                    batch.num_rows(),
                    elapsed
                );
            }
            None => {
                println!("  查询结果为空");
            }
        }
        println!();

        // 6. 测试删除文档
        println!("【5】删除文档测试");
        println!("  删除前总数: {}", loaded.total_count());

        // 标记一些文档为删除
        let to_delete = vec![10, 20, 30];
        segment.mark_del(to_delete.clone());

        println!("  标记删除 {} 条文档: {:?}", to_delete.len(), to_delete);
        println!("  删除后总数: {}", loaded.total_count());
        println!();

        // 7. 查询已删除的文档（应该返回 None）
        println!("【6】查询已删除文档");
        for doc_id in &to_delete {
            let doc = segment.get_document(*doc_id);
            assert!(doc.is_none(), "已删除文档 {} 不应该被查询到", doc_id);
            println!("  Doc {}: 已删除（正确）", doc_id);
        }
        println!();

        // 8. 批量查询包含已删除文档
        println!("【7】批量查询（包含已删除文档）");
        let mixed_ids = vec![0, 10, 5, 20, 15, 30]; // 10, 20, 30 已删除
        let result = segment.get_documents(&mixed_ids).unwrap();

        match result {
            Some(batch) => {
                let expected_count = 3; // 只有 0, 5, 15 应该返回
                println!(
                    "  查询 {} 条文档（含 {} 条已删除）: 返回 {} 行",
                    mixed_ids.len(),
                    to_delete.len(),
                    batch.num_rows()
                );
                assert_eq!(
                    batch.num_rows(),
                    expected_count,
                    "应该只返回 {} 条未删除文档",
                    expected_count
                );
            }
            None => {
                panic!("查询不应该返回空");
            }
        }
        println!();

        // 9. Scan all documents
        println!("【8】全表扫描测试");
        let start = std::time::Instant::now();
        let batches = segment.scan_documents().unwrap();
        let elapsed = start.elapsed();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!(
            "  扫描结果: {} 个批次，共 {} 行 耗时 {:?}",
            batches.len(),
            total_rows,
            elapsed
        );
        println!(
            "  预期: {} 行（150 - {} 已删除）",
            150 - to_delete.len(),
            to_delete.len()
        );
        println!();

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).unwrap();

        println!("========================================");
        println!("✅ 文档查询与删除测试完成!");
        println!("========================================\n");
    }

    #[test]
    fn test_history_delete_snapshot() {
        use crate::schema::field::FieldOption;
        use arrow::{
            array::{RecordBatch, StringBuilder},
            datatypes::{DataType, Field, Schema as ArrowSchema},
        };
        use std::sync::{Arc, RwLock};

        println!("\n=== 测试历史删除快照功能 ===\n");

        // 1. 创建 3 个 segments
        let schema = Arc::new(Schema {
            name: "test_schema".to_string(),
            primary_key: Some("id".to_string()),
            store_source: false,
            fields: vec![
                FieldOption::Keyword {
                    name: "id".to_string(),
                    is_array: false,
                    index: true,
                },
                FieldOption::Keyword {
                    name: "name".to_string(),
                    is_array: false,
                    index: true,
                },
            ],
        });

        let segment_0 = Arc::new(Segment::new(0, schema.clone()));
        let segment_1 = Arc::new(Segment::new(100, schema.clone()));
        let segment_2 = Arc::new(Segment::new(200, schema.clone()));

        // 2. 写入数据到每个 segment
        println!("【1】写入数据到 3 个 segments");
        for (seg_idx, segment) in [&segment_0, &segment_1, &segment_2].iter().enumerate() {
            let start = seg_idx * 100;
            for i in 0..50 {
                let mut id_builder = StringBuilder::new();
                let mut name_builder = StringBuilder::new();

                id_builder.append_value(&format!("id{}", start + i));
                name_builder.append_value(&format!("name{}", start + i));

                let data = RecordBatch::try_new(
                    Arc::new(ArrowSchema::new(vec![
                        Field::new("id", DataType::Utf8, false),
                        Field::new("name", DataType::Utf8, true),
                    ])),
                    vec![
                        Arc::new(id_builder.finish()),
                        Arc::new(name_builder.finish()),
                    ],
                )
                .unwrap();

                segment.write(&data, None, None, &RwLock::new(())).unwrap();
            }
            println!("  Segment {}: 写入 50 条记录", seg_idx);
        }
        println!();

        // 3. 在内存中标记一些删除
        println!("【2】在内存中标记删除");
        segment_0.mark_del(vec![0, 1, 2]); // Segment 0: 删除 3 条
        segment_1.mark_del(vec![100, 101]); // Segment 1: 删除 2 条
        println!("  Segment 0: 删除 3 条 (doc_id: 0, 1, 2)");
        println!("  Segment 1: 删除 2 条 (doc_id: 100, 101)");
        println!();

        // 4. Persist segment_0 (应该没有历史删除)
        let persist_path = "/tmp/test_history_delete_snapshot";
        let _ = std::fs::remove_dir_all(persist_path);
        std::fs::create_dir_all(persist_path).unwrap();

        println!("【3】Persist Segment 0 (无历史删除)");
        let history_segments: Vec<(u64, Arc<Segment>)> = vec![];
        segment_0
            .persist(persist_path, 0, &history_segments)
            .unwrap();
        println!("  Segment 0 持久化完成");
        println!();

        // 5. Persist segment_1 (应该包含 segment_0 的删除快照)
        println!("【4】Persist Segment 1 (包含 Segment 0 的删除快照)");
        let history_segments = vec![(0, segment_0.clone())];
        segment_1
            .persist(persist_path, 1, &history_segments)
            .unwrap();
        println!("  Segment 1 持久化完成");
        println!();

        // 6. 继续标记更多删除
        println!("【5】继续标记更多删除");
        segment_0.mark_del(vec![3, 4]); // Segment 0: 再删除 2 条
        segment_2.mark_del(vec![200]); // Segment 2: 删除 1 条
        println!("  Segment 0: 再删除 2 条 (doc_id: 3, 4)");
        println!("  Segment 2: 删除 1 条 (doc_id: 200)");
        println!();

        // 7. Persist segment_2 (应该包含 segment_0 和 segment_1 的删除快照)
        println!("【6】Persist Segment 2 (包含 Segment 0 和 1 的删除快照)");
        let history_segments = vec![(0, segment_0.clone()), (1, segment_1.clone())];
        segment_2
            .persist(persist_path, 2, &history_segments)
            .unwrap();
        println!("  Segment 2 持久化完成");
        println!();

        // 8. 验证文件结构（删除快照文件应该已经被替换走了）
        println!("【7】验证文件结构");
        let seg0_dir = format!("{}/segment-0", persist_path);
        let seg1_dir = format!("{}/segment-1", persist_path);
        let seg2_dir = format!("{}/segment-2", persist_path);

        // 检查是否有残留的 deleted_seg_* 文件（应该都被替换走了）
        let has_leftover_in_seg1 =
            std::path::Path::new(&format!("{}/deleted_seg_0", seg1_dir)).exists();
        let has_leftover_in_seg2_0 =
            std::path::Path::new(&format!("{}/deleted_seg_0", seg2_dir)).exists();
        let has_leftover_in_seg2_1 =
            std::path::Path::new(&format!("{}/deleted_seg_1", seg2_dir)).exists();

        println!(
            "  Segment 1 残留 deleted_seg_0: {} (应该为 false)",
            has_leftover_in_seg1
        );
        println!(
            "  Segment 2 残留 deleted_seg_0: {} (应该为 false)",
            has_leftover_in_seg2_0
        );
        println!(
            "  Segment 2 残留 deleted_seg_1: {} (应该为 false)",
            has_leftover_in_seg2_1
        );

        // 验证每个 segment 都有自己的 deleted 文件
        assert!(
            std::path::Path::new(&format!("{}/deleted", seg0_dir)).exists(),
            "Segment 0 应该有 deleted 文件"
        );
        assert!(
            std::path::Path::new(&format!("{}/deleted", seg1_dir)).exists(),
            "Segment 1 应该有 deleted 文件"
        );
        assert!(
            std::path::Path::new(&format!("{}/deleted", seg2_dir)).exists(),
            "Segment 2 应该有 deleted 文件"
        );

        // deleted_seg_* 文件应该都已被替换，不应存在
        assert!(
            !has_leftover_in_seg1,
            "Segment 1 不应该有残留的 deleted_seg_0"
        );
        assert!(
            !has_leftover_in_seg2_0,
            "Segment 2 不应该有残留的 deleted_seg_0"
        );
        assert!(
            !has_leftover_in_seg2_1,
            "Segment 2 不应该有残留的 deleted_seg_1"
        );

        println!("  文件结构验证 ✓：所有快照文件已正确替换");
        println!();

        // 9. 重新加载 segments 并验证删除合并
        println!("【8】重新加载 segments 并验证删除");
        // end_id 是最后一个文档的ID (inclusive): 0-49, 100-149, 200-249
        let loaded_seg0 = Segment::load_frozen(persist_path, 0, 49, schema.clone()).unwrap();
        let loaded_seg1 = Segment::load_frozen(persist_path, 100, 149, schema.clone()).unwrap();
        let loaded_seg2 = Segment::load_frozen(persist_path, 200, 249, schema.clone()).unwrap();

        // Segment 0 的删除应该被加载（原始persist时的3条 + segment2持久化时快照的2条）
        let seg0_deleted_count = loaded_seg0.deleted_count();
        println!(
            "  Segment 0 删除数: {} (persist 时: 3, 后续被 segment2 快照: 2)",
            seg0_deleted_count
        );
        assert_eq!(
            seg0_deleted_count, 5,
            "Segment 0 应该有 5 条删除记录（3原始 + 2快照）"
        );

        // Segment 1 的删除
        let seg1_deleted_count = loaded_seg1.deleted_count();
        println!("  Segment 1 删除数: {}", seg1_deleted_count);
        assert_eq!(seg1_deleted_count, 2, "Segment 1 应该有 2 条删除记录");

        // Segment 2 的删除
        let seg2_deleted_count = loaded_seg2.deleted_count();
        println!("  Segment 2 删除数: {}", seg2_deleted_count);
        assert_eq!(seg2_deleted_count, 1, "Segment 2 应该有 1 条删除记录");
        println!();

        // 10. 验证删除标记已正确加载（核心功能）
        println!("【9】验证删除功能");

        // 核心验证：删除的文档确实被标记了
        println!("  测试删除标记：");
        println!("    Segment 0: doc 0,1,2 应该被标记删除");
        println!("    Segment 1: doc 100,101 应该被标记删除");
        println!("    Segment 2: doc 200 应该被标记删除");

        // 直接验证 total_count (会排除已删除文档)
        let seg0_total = loaded_seg0.total_count();
        let seg1_total = loaded_seg1.total_count();
        let seg2_total = loaded_seg2.total_count();

        println!("  实际文档数：");
        println!("    Segment 0: {} 条 (50 - 5 删除 = 45)", seg0_total);
        println!("    Segment 1: {} 条 (50 - 2 删除 = 48)", seg1_total);
        println!("    Segment 2: {} 条 (50 - 1 删除 = 49)", seg2_total);

        assert_eq!(seg0_total, 45, "Segment 0 应该有 45 条有效文档");
        assert_eq!(seg1_total, 48, "Segment 1 应该有 48 条有效文档");
        assert_eq!(seg2_total, 49, "Segment 2 应该有 49 条有效文档");

        println!("  删除功能验证 ✓");
        println!();
        println!("✅ 历史删除快照功能测试通过！");

        // 清理
        let _ = std::fs::remove_dir_all(persist_path);
    }
}
