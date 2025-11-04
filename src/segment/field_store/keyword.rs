use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use arrow::array::{ArrayRef, ListArray, RecordBatch, StringArray, UInt32Array};
use byteorder::WriteBytesExt;
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
            for (a, id) in arrow_downcast!(arr, StringArray)
                .iter()
                .zip(arrow_downcast!(data.column(0), UInt32Array).iter())
            {
                if let (Some(v), Some(id)) = (a, id) {
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
        use arrow::array::Array;

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

        let pk = data.column_by_name(self.name()).ok_or_else(|| {
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

/// 磁盘用：String -> RoaringBitmap 的序列化器
#[derive(Clone)]
pub struct StringRoaringSerializer {
    zstd_level: i32,
}

impl StringRoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }
    pub fn default() -> Self {
        Self { zstd_level: 3 }
    }
}

impl mem_btree::persist::ReadSerializer<String, RoaringBitmap> for StringRoaringSerializer {
    fn deserialize_value<'a>(
        &self,
        data: &'a [u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        super::decode_roaring_from_bytes(data)
    }

    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<String> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };
        let mut result = Vec::new();
        let mut pos = 0;
        if data_to_parse.len() < 2 {
            return result;
        }
        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        pos += 2;
        for _ in 0..key_count {
            if pos >= data_to_parse.len() {
                break;
            }
            let len = mem_btree::persist::zigzag::read_u32(&data_to_parse, &mut pos) as usize;
            if pos + len > data_to_parse.len() {
                break;
            }
            match String::from_utf8(data_to_parse[pos..pos + len].to_vec()) {
                Ok(key) => result.push(key),
                Err(_) => break,
            }
            pos += len;
        }
        result
    }
}

impl mem_btree::persist::WriteSerializer<String, RoaringBitmap> for StringRoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<String>) -> std::borrow::Cow<'a, [u8]> {
        let mut uncompressed = Vec::new();
        // 写入 key 的个数（u16 varint 更短，这里复用 BigEndian u16 保持一致性）
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");

        for key in keys {
            let key_bytes = key.as_bytes();
            mem_btree::persist::zigzag::write_u32(key_bytes.len() as u32, &mut uncompressed)
                .expect("write zigzag u32 failed");
            uncompressed.extend_from_slice(key_bytes);
        }

        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        std::borrow::Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> std::borrow::Cow<'a, [u8]> {
        std::borrow::Cow::Owned(super::encode_roaring_from_bitmap(value))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{RecordBatch, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::fs;
    use std::sync::Arc;
    use std::time::Instant;

    use super::*;

    #[test]
    #[ignore] // 使用 `cargo test keyword_10m -- --ignored --nocapture` 运行
    fn keyword_10m() {
        // 默认测试 chunk_size=256 (原先的设置)
        run_benchmark_with_chunk_size(256);
    }

    #[test]
    #[ignore]
    fn keyword_chunk_comparison() {
        println!("\n╔═══════════════════════════════════════════════════════════════╗");
        println!("║         Chunk Size Comparison Benchmark                      ║");
        println!("╚═══════════════════════════════════════════════════════════════╝\n");

        let chunk_sizes = vec![128, 256, 512, 1024];

        for (i, &chunk_size) in chunk_sizes.iter().enumerate() {
            println!(
                "\n━━━━━━━━━━━━━━ Test {}/{}: Chunk Size = {} ━━━━━━━━━━━━━━\n",
                i + 1,
                chunk_sizes.len(),
                chunk_size
            );
            run_benchmark_with_chunk_size(chunk_size);

            if i < chunk_sizes.len() - 1 {
                println!("\n⏳ Cooling down...\n");
                std::thread::sleep(std::time::Duration::from_secs(2));
            }
        }

        println!("\n╔═══════════════════════════════════════════════════════════════╗");
        println!("║              All Tests Completed                             ║");
        println!("╚═══════════════════════════════════════════════════════════════╝\n");
    }

    #[test]
    fn test_case_insensitive() {
        println!("\n=== Testing Case Insensitive Keyword ===\n");

        // 创建区分大小写的索引
        let sensitive_keyword = Keyword::new(&FieldOption::Keyword {
            name: "test".to_string(),
            is_array: false,
            index: true,
            persist_option: None,
            case_sensitive: true,
        });

        // 创建不区分大小写的索引
        let insensitive_keyword = Keyword::new(&FieldOption::Keyword {
            name: "test".to_string(),
            is_array: false,
            index: true,
            persist_option: None,
            case_sensitive: false,
        });

        // 准备测试数据
        let schema = Arc::new(Schema::new(vec![
            Field::new("internal_id", DataType::UInt32, false),
            Field::new("test", DataType::Utf8, false),
        ]));

        let test_data = vec![
            ("Hello", 1u32),
            ("WORLD", 2u32),
            ("Test", 3u32),
            ("hello", 4u32),
            ("world", 5u32),
        ];

        // 写入数据到两个索引
        for batch_start in (0..test_data.len()).step_by(2) {
            let batch_end = (batch_start + 2).min(test_data.len());
            let batch_data: Vec<_> = test_data[batch_start..batch_end].to_vec();

            let ids: Vec<u32> = batch_data.iter().map(|(_, id)| *id).collect();
            let keywords: Vec<String> = batch_data.iter().map(|(s, _)| s.to_string()).collect();

            let id_array = Arc::new(UInt32Array::from(ids));
            let keyword_array = Arc::new(StringArray::from(keywords));

            let batch =
                RecordBatch::try_new(schema.clone(), vec![id_array, keyword_array]).unwrap();

            sensitive_keyword.write(&batch).unwrap();
            insensitive_keyword.write(&batch).unwrap();
        }

        println!("【Case Sensitive Index】");

        // 区分大小写：查询 "hello" 只能找到 id=4
        let result = sensitive_keyword.query("hello");
        assert!(result.is_some());
        let bitmap = result.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(4));
        println!(
            "  Query 'hello': found {} documents (id: 4) ✓",
            bitmap.len()
        );

        // 区分大小写：查询 "Hello" 只能找到 id=1
        let result = sensitive_keyword.query("Hello");
        assert!(result.is_some());
        let bitmap = result.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(1));
        println!(
            "  Query 'Hello': found {} documents (id: 1) ✓",
            bitmap.len()
        );

        // 区分大小写：查询 "HELLO" 找不到
        let result = sensitive_keyword.query("HELLO");
        assert!(result.is_none());
        println!("  Query 'HELLO': found 0 documents ✓");

        println!("\n【Case Insensitive Index】");

        // 不区分大小写：查询 "hello" 能找到 id=1 和 id=4
        let result = insensitive_keyword.query("hello");
        assert!(result.is_some());
        let bitmap = result.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(4));
        println!(
            "  Query 'hello': found {} documents (id: 1, 4) ✓",
            bitmap.len()
        );

        // 不区分大小写：查询 "Hello" 能找到 id=1 和 id=4
        let result = insensitive_keyword.query("Hello");
        assert!(result.is_some());
        let bitmap = result.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(4));
        println!(
            "  Query 'Hello': found {} documents (id: 1, 4) ✓",
            bitmap.len()
        );

        // 不区分大小写：查询 "HELLO" 也能找到 id=1 和 id=4
        let result = insensitive_keyword.query("HELLO");
        assert!(result.is_some());
        let bitmap = result.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(4));
        println!(
            "  Query 'HELLO': found {} documents (id: 1, 4) ✓",
            bitmap.len()
        );

        // 不区分大小写：查询 "WORLD" 能找到 id=2 和 id=5
        let result = insensitive_keyword.query("WoRlD");
        assert!(result.is_some());
        let bitmap = result.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(5));
        println!(
            "  Query 'WoRlD': found {} documents (id: 2, 5) ✓",
            bitmap.len()
        );

        println!("\n=== Test Passed! ===\n");
    }

    /// 使用指定 chunk_size 运行完整基准测试
    fn run_benchmark_with_chunk_size(chunk_size: usize) {
        // 配置参数
        const TOTAL_RECORDS: usize = 10_000_000;
        const BATCH_SIZE: usize = 10_000;
        const UNIQUE_KEYWORDS: usize = 100_000; // 关键词数量 (UUID)
        const QUERY_SAMPLES: usize = 1000; // 查询测试样本数

        // 创建临时目录
        let test_dir = "/tmp/calmcore_keyword_benchmark";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        println!("=== Keyword Field Benchmark (UUID) ===");
        println!("Total Records: {}", TOTAL_RECORDS);
        println!("Batch Size: {}", BATCH_SIZE);
        println!("Unique UUIDs: {}", UNIQUE_KEYWORDS);
        println!("Query Samples: {}", QUERY_SAMPLES);
        println!("Chunk Size: {}", chunk_size);
        println!();

        // ===== 阶段 1: 写入数据 =====
        println!("【Phase 1: Writing Data】");

        use crate::schema::field::PersistOption;

        let keyword = Keyword::new(&FieldOption::Keyword {
            name: "test".to_string(),
            is_array: false,
            index: true,
            persist_option: Some(PersistOption::new(3, chunk_size)),
            case_sensitive: true,
        });

        // 生成 UUID 池
        let uuid_pool = generate_uuid_pool(UNIQUE_KEYWORDS);

        let write_start = Instant::now();
        let num_batches = TOTAL_RECORDS / BATCH_SIZE;

        for batch_idx in 0..num_batches {
            let batch_start = batch_idx * BATCH_SIZE;
            let batch = generate_batch_with_uuid(batch_start, BATCH_SIZE, &uuid_pool);
            keyword.write(&batch).unwrap();

            if (batch_idx + 1) % 100 == 0 {
                print!(
                    "\rWriting: {}/{} batches ({:.1}%)",
                    batch_idx + 1,
                    num_batches,
                    (batch_idx + 1) as f64 / num_batches as f64 * 100.0
                );
                use std::io::{self, Write};
                io::stdout().flush().unwrap();
            }
        }
        println!();

        let write_time = write_start.elapsed();
        println!("Writing completed in {:.2}s", write_time.as_secs_f64());
        println!(
            "Write throughput: {:.2} records/sec",
            TOTAL_RECORDS as f64 / write_time.as_secs_f64()
        );
        println!();

        // ===== 阶段 2: 测试内存索引查询性能 =====
        println!("【Phase 2: Memory Index Query Performance】");
        let query_keys = generate_query_keys_from_pool(QUERY_SAMPLES, &uuid_pool);
        let mem_query_time = benchmark_queries(&keyword, &query_keys);
        println!(
            "Memory query avg time: {:.2}μs",
            mem_query_time * 1_000_000.0
        );
        println!("Memory query QPS: {:.2}", 1.0 / mem_query_time);
        println!();

        // ===== 阶段 3: 持久化 (使用指定的 chunk_size) =====
        println!("【Phase 3: Persisting to Disk】");
        let persist_path = format!("{}/keyword_index", test_dir);
        let persist_start = Instant::now();
        let _disk_keyword = persist_with_chunk_size(&keyword, &persist_path, chunk_size).unwrap();
        let persist_time = persist_start.elapsed();

        let disk_size = calculate_dir_size(&persist_path);

        // 计算原始数据大小
        let raw_data_size = (UNIQUE_KEYWORDS * 36 + TOTAL_RECORDS * 4) as u64;

        println!("Persist time: {:.2}s", persist_time.as_secs_f64());
        println!("Disk size: {:.2}MB", disk_size as f64 / 1024.0 / 1024.0);
        println!(
            "Raw data size (estimated): {:.2}MB",
            raw_data_size as f64 / 1024.0 / 1024.0
        );
        println!(
            "Compression ratio: {:.2}x",
            raw_data_size as f64 / disk_size as f64
        );
        println!(
            "Bytes per record: {:.2}",
            disk_size as f64 / TOTAL_RECORDS as f64
        );

        // 计算索引文件结构信息
        let index_stats = get_index_file_stats(&persist_path);
        println!(
            "Node file size: {:.2}MB ({:.1}% of total)",
            index_stats.node_size as f64 / 1024.0 / 1024.0,
            index_stats.node_size as f64 / disk_size as f64 * 100.0
        );
        println!(
            "Data file size: {:.2}MB ({:.1}% of total)",
            index_stats.data_size as f64 / 1024.0 / 1024.0,
            index_stats.data_size as f64 / disk_size as f64 * 100.0
        );
        println!("\n💡 Why chunk_size doesn't affect file size:");
        println!("   • Data file (93%): stores all RoaringBitmaps (unchanged)");
        println!("   • Node file (7%): stores all keys+offsets (total unchanged)");
        println!("   • chunk_size only affects B-tree organization, not data amount");
        println!("   • BUT it significantly affects query performance!");
        println!();

        // ===== 阶段 4: 从磁盘加载 =====
        println!("【Phase 4: Loading from Disk】");
        let load_start = Instant::now();
        let loaded_keyword = Keyword::from_disk(
            &FieldOption::Keyword {
                name: "test".to_string(),
                is_array: false,
                index: true,
                persist_option: Some(PersistOption::new(3, chunk_size)),
                case_sensitive: true,
            },
            &persist_path,
        )
        .unwrap();
        let load_time = load_start.elapsed();
        println!("Load time: {:.2}ms", load_time.as_secs_f64() * 1000.0);
        println!();

        // ===== 阶段 5: 测试磁盘索引查询性能 =====
        println!("【Phase 5: Disk Index Query Performance】");
        let disk_query_time = benchmark_queries(&loaded_keyword, &query_keys);
        println!(
            "Disk query avg time: {:.2}μs",
            disk_query_time * 1_000_000.0
        );
        println!("Disk query QPS: {:.2}", 1.0 / disk_query_time);
        println!();

        // ===== 生成报告 =====
        print_final_report(
            TOTAL_RECORDS,
            BATCH_SIZE,
            UNIQUE_KEYWORDS,
            write_time,
            mem_query_time,
            persist_time,
            disk_size,
            raw_data_size,
            load_time,
            disk_query_time,
        );

        // 清理
        let _ = fs::remove_dir_all(test_dir);
    }
    /// 使用指定 chunk_size 持久化索引
    fn persist_with_chunk_size(
        keyword: &Keyword,
        path: &str,
        chunk_size: usize,
    ) -> CoreResult<Keyword> {
        use super::StringRoaringSerializer;
        use mem_btree::persist::TreeWriter;

        // 1. Extract the memory index
        let memory_index = keyword.indexs.read().unwrap();
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

        // 3. Persist to disk with specified chunk_size
        let serializer = StringRoaringSerializer::default();
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

        Ok(Keyword {
            field: keyword.field.clone(),
            indexs: RwLock::new(disk_index),
        })
    }

    /// 获取索引文件统计信息
    struct IndexFileStats {
        node_size: u64,
        data_size: u64,
    }

    fn get_index_file_stats(path: &str) -> IndexFileStats {
        use std::path::Path;

        let node_path = Path::new(path).join("node");
        let data_path = Path::new(path).join("data");

        let node_size = if let Ok(metadata) = fs::metadata(&node_path) {
            metadata.len()
        } else {
            0
        };

        let data_size = if let Ok(metadata) = fs::metadata(&data_path) {
            metadata.len()
        } else {
            0
        };

        IndexFileStats {
            node_size,
            data_size,
        }
    }

    /// 生成 UUID 池
    fn generate_uuid_pool(count: usize) -> Vec<String> {
        use uuid::Uuid;
        (0..count).map(|_| Uuid::new_v4().to_string()).collect()
    }

    /// 生成测试数据批次 (使用 UUID)
    fn generate_batch_with_uuid(start_id: usize, size: usize, uuid_pool: &[String]) -> RecordBatch {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        // 生成 internal_id
        let internal_ids: Vec<u32> = (start_id as u32..(start_id + size) as u32).collect();
        let id_array = Arc::new(UInt32Array::from(internal_ids));

        // 从 UUID 池中随机选择
        let keywords: Vec<String> = (0..size)
            .map(|_| uuid_pool[rng.gen_range(0..uuid_pool.len())].clone())
            .collect();
        let keyword_array = Arc::new(StringArray::from(keywords));

        // 创建 Schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("internal_id", DataType::UInt32, false),
            Field::new("test", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(schema, vec![id_array, keyword_array]).unwrap()
    }

    /// 生成测试数据批次 (旧版本，使用简单关键词)
    fn generate_batch(start_id: usize, size: usize, unique_keywords: usize) -> RecordBatch {
        use rand::Rng;

        let mut rng = rand::thread_rng();

        // 生成 internal_id
        let internal_ids: Vec<u32> = (start_id as u32..(start_id + size) as u32).collect();
        let id_array = Arc::new(UInt32Array::from(internal_ids));

        // 生成关键词
        let keywords: Vec<String> = (0..size)
            .map(|_| format!("keyword_{}", rng.gen_range(0..unique_keywords)))
            .collect();
        let keyword_array = Arc::new(StringArray::from(keywords));

        // 创建 Schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("internal_id", DataType::UInt32, false),
            Field::new("test", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(schema, vec![id_array, keyword_array]).unwrap()
    }

    /// 计算目录大小
    fn calculate_dir_size(path: &str) -> u64 {
        let mut total_size = 0u64;
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_file() {
                        total_size += metadata.len();
                    } else if metadata.is_dir() {
                        total_size += calculate_dir_size(&entry.path().to_string_lossy());
                    }
                }
            }
        }
        total_size
    }

    /// 从 UUID 池中生成查询测试用的关键词
    fn generate_query_keys_from_pool(count: usize, uuid_pool: &[String]) -> Vec<String> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        (0..count)
            .map(|_| uuid_pool[rng.gen_range(0..uuid_pool.len())].clone())
            .collect()
    }

    /// 生成查询测试用的关键词 (旧版本)
    fn generate_query_keys(count: usize, unique_keywords: usize) -> Vec<String> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        (0..count)
            .map(|_| format!("keyword_{}", rng.gen_range(0..unique_keywords)))
            .collect()
    }

    /// 基准测试查询性能，返回平均查询时间(秒)
    fn benchmark_queries(keyword: &Keyword, query_keys: &[String]) -> f64 {
        let start = Instant::now();
        let indexs = keyword.indexs.read().unwrap();

        let mut total_hits = 0usize;
        for key in query_keys {
            if let Some(bitmap) = indexs.get_bitmap(key) {
                total_hits += bitmap.len() as usize;
            }
        }

        let elapsed = start.elapsed();

        // 防止编译器优化掉查询结果
        if total_hits == usize::MAX {
            println!("Impossible");
        }

        elapsed.as_secs_f64() / query_keys.len() as f64
    }

    /// 打印最终报告
    fn print_final_report(
        total_records: usize,
        batch_size: usize,
        unique_keywords: usize,
        write_time: std::time::Duration,
        mem_query_time: f64,
        persist_time: std::time::Duration,
        disk_size: u64,
        raw_data_size: u64,
        load_time: std::time::Duration,
        disk_query_time: f64,
    ) {
        println!();
        println!("==================== BENCHMARK REPORT ====================");
        println!();

        println!("【Test Configuration】");
        println!("  Total Records: {}", total_records);
        println!("  Batch Size: {}", batch_size);
        println!("  Unique UUIDs: {}", unique_keywords);
        println!();

        println!("【Write Performance】");
        println!("  Write Time: {:.2}s", write_time.as_secs_f64());
        println!(
            "  Write Throughput: {:.2} records/sec",
            total_records as f64 / write_time.as_secs_f64()
        );
        println!(
            "  Avg Batch Time: {:.2}ms",
            write_time.as_secs_f64() * 1000.0 / (total_records / batch_size) as f64
        );
        println!();

        println!("【Memory Index Query Performance】");
        println!("  Avg Query Time: {:.2}μs", mem_query_time * 1_000_000.0);
        println!("  Query QPS: {:.2}", 1.0 / mem_query_time);
        println!();

        println!("【Persistence】");
        println!("  Persist Time: {:.2}s", persist_time.as_secs_f64());
        println!(
            "  Raw Data Size: {:.2}MB",
            raw_data_size as f64 / 1024.0 / 1024.0
        );
        println!("  Disk Size: {:.2}MB", disk_size as f64 / 1024.0 / 1024.0);
        println!(
            "  Compression Ratio: {:.2}x",
            raw_data_size as f64 / disk_size as f64
        );
        println!(
            "  Space Saving: {:.2}%",
            (1.0 - disk_size as f64 / raw_data_size as f64) * 100.0
        );
        println!(
            "  Bytes per Record: {:.2}",
            disk_size as f64 / total_records as f64
        );
        println!();

        println!("【Disk Index】");
        println!("  Load Time: {:.2}ms", load_time.as_secs_f64() * 1000.0);
        println!("  Avg Query Time: {:.2}μs", disk_query_time * 1_000_000.0);
        println!("  Query QPS: {:.2}", 1.0 / disk_query_time);
        println!();

        println!("【Performance Comparison】");
        let slowdown = disk_query_time / mem_query_time;
        println!("  Disk vs Memory Query: {:.2}x slower", slowdown);
        println!("  Memory vs Disk Query: {:.2}x faster", slowdown);
        println!();

        println!("==========================================================");
    }
}
