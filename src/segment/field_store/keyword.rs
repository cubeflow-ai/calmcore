use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use arrow::array::{ArrayRef, ListArray, RecordBatch, StringArray, UInt32Array};
use byteorder::WriteBytesExt;
use mem_btree::persist::zigzag;
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
        // Create disk-based Keyword
        let inverted_index =
            InvertedIndex::new_disk(&field_path, super::StringRoaringSerializer::default())?;

        Ok(Self {
            field: field.clone(),
            indexs: RwLock::new(inverted_index),
        })
    }

    /// Persist the in-memory index to disk and return a new Keyword with disk-based index
    pub fn persist(&self, path: &str) -> CoreResult<Self> {
        use super::StringRoaringSerializer;
        use mem_btree::persist::TreeWriter;

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

        // 3. Persist to disk
        let serializer = StringRoaringSerializer::default();
        let writer = TreeWriter::new(
            std::path::PathBuf::from(path),
            128, // chunk_size
            0,   // key_len (0 means variable-length keys)
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
        let internal_id_array = arrow_downcast!(data.column(0), UInt32Array);

        for (a, id) in arrow_downcast!(pk, StringArray)
            .iter()
            .zip(internal_id_array.iter())
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
        mem_btree::persist::value_codec::decode_roaring_from_bytes(data)
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
        let bytes = mem_btree::persist::value_codec::encode_roaring_from_bitmap(value);
        std::borrow::Cow::Owned(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_basic() {
        use arrow::array::{StringBuilder, UInt32Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let field = FieldOption::Keyword {
            name: "tags".to_string(),
            is_array: false,
            index: true,
        };

        let keyword = Keyword::new(&field);

        // 写入一批数据
        let mut id_vec = vec![1u32, 2u32, 3u32];
        let mut builder = StringBuilder::new();
        builder.append_value("tag1");
        builder.append_value("tag2");
        builder.append_value("tag1"); // 重复的tag

        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt32, false),
                Field::new("tags", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(UInt32Array::from(id_vec)),
                Arc::new(builder.finish()),
            ],
        )
        .unwrap();

        keyword.write(&data).unwrap();

        // 验证索引
        let indexs = keyword.indexs.read().unwrap();
        println!("索引大小: {} 个不同的 tag", indexs.len());

        // 应该有2个不同的tag: tag1 和 tag2
        assert_eq!(indexs.len(), 2);
    }

    #[test]
    fn bench_keyword_write_1m() {
        use arrow::array::StringBuilder;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let field = FieldOption::Keyword {
            name: "tags".to_string(),
            is_array: false,
            index: true,
        };

        let keyword = Keyword::new(&field);

        let batch_size = 1000;
        let batch_count = 1_000; // 100万条记录

        println!("开始写入 100万条记录...");
        let start = std::time::Instant::now();

        use rand::Rng;
        let tag_count = 10_000; // 1万个不同的tag
        let mut rng = rand::thread_rng();

        for batch in 0..batch_count {
            let mut id_vec = Vec::with_capacity(batch_size);
            let mut builder = StringBuilder::new();

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

            if (batch + 1) % 100 == 0 {
                println!(
                    "  已写入 {} 万条，耗时: {:?}",
                    (batch + 1) / 10,
                    start.elapsed()
                );
            }
        }

        let elapsed = start.elapsed();
        println!("\n100万条记录写入完成!");
        println!("总耗时: {:?}", elapsed);
        println!("平均速度: {:.0} 条/秒", 1_000_000.0 / elapsed.as_secs_f64());

        // 检查索引状态
        let indexs = keyword.indexs.read().unwrap();
        println!("索引大小: {} 个不同的 tag", indexs.len());
        println!("预期大小: {} 个不同的 tag", tag_count);
    }

    #[test]
    fn bench_keyword_write_10m() {
        use arrow::array::StringBuilder;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let field = FieldOption::Keyword {
            name: "tags".to_string(),
            is_array: false,
            index: true,
        };

        let keyword = Keyword::new(&field);

        let batch_size = 1000;
        let batch_count = 3_000; // 300万条记录

        println!("开始写入 300万条记录...");
        let start = std::time::Instant::now();

        use rand::Rng;
        let tag_count = 100_000; // 10万个不同的tag
        let mut rng = rand::thread_rng();

        for batch in 0..batch_count {
            let mut id_vec = Vec::with_capacity(batch_size);
            let mut builder = StringBuilder::new();

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

            if (batch + 1) % 1000 == 0 {
                println!(
                    "  已写入 {} 百万条，耗时: {:?}",
                    (batch + 1) / 1000,
                    start.elapsed()
                );
            }
        }

        let elapsed = start.elapsed();
        println!("\n300万条记录写入完成!");
        println!("总耗时: {:?}", elapsed);
        println!("平均速度: {:.0} 条/秒", 3_000_000.0 / elapsed.as_secs_f64());

        // 检查索引状态
        let indexs = keyword.indexs.read().unwrap();
        println!("索引大小: {} 个不同的 tag", indexs.len());
        println!("预期大小: {} 个不同的 tag", tag_count);
    }

    #[test]
    fn test_keyword_array() {
        use arrow::array::{ListBuilder, StringBuilder, UInt32Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let field = FieldOption::Keyword {
            name: "tags".to_string(),
            is_array: true,
            index: true,
        };

        let keyword = Keyword::new(&field);

        // 创建包含数组的数据
        let mut list_builder = ListBuilder::new(StringBuilder::new());

        // ID 1: ["tag1", "tag2"]
        let values_builder = list_builder.values();
        values_builder.append_value("tag1");
        values_builder.append_value("tag2");
        list_builder.append(true);

        // ID 2: ["tag2", "tag3"]
        let values_builder = list_builder.values();
        values_builder.append_value("tag2");
        values_builder.append_value("tag3");
        list_builder.append(true);

        // ID 3: ["tag1"]
        let values_builder = list_builder.values();
        values_builder.append_value("tag1");
        list_builder.append(true);

        let list_array = list_builder.finish();

        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt32, false),
                Field::new(
                    "tags",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ),
            ])),
            vec![
                Arc::new(UInt32Array::from(vec![1u32, 2u32, 3u32])),
                Arc::new(list_array),
            ],
        )
        .unwrap();

        keyword.write(&data).unwrap();

        // 验证索引
        let indexs = keyword.indexs.read().unwrap();
        println!("索引大小: {} 个不同的 tag", indexs.len());

        // 应该有3个不同的tag: tag1, tag2, tag3
        assert_eq!(indexs.len(), 3);
    }

    #[test]
    fn test_keyword_persist() {
        use arrow::array::StringBuilder;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        // 1. 创建内存索引并写入数据
        let field = FieldOption::Keyword {
            name: "tags".to_string(),
            is_array: false,
            index: true,
        };

        let keyword = Keyword::new(&field);

        println!("写入测试数据...");
        let start = std::time::Instant::now();

        // 写入 10万条记录，1000个不同的tag
        use rand::Rng;
        let record_count = 100_000;
        let tag_count = 1_000;
        let batch_size = 1000;
        let batch_count = record_count / batch_size;
        let mut rng = rand::rng();

        for batch in 0..batch_count {
            let mut id_vec = Vec::with_capacity(batch_size);
            let mut builder = StringBuilder::new();

            for i in 0..batch_size {
                id_vec.push((batch * batch_size + i) as u32);
                let tag = format!("tag{}", rng.random_range(0..tag_count));
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

        println!("写入完成，耗时: {:?}", start.elapsed());

        // 2. 验证内存索引
        let memory_len = keyword.indexs.read().unwrap().len();
        println!("内存索引大小: {} 个不同的 tag", memory_len);

        // 3. Persist 到磁盘
        let persist_path = "/tmp/test_keyword_persist";
        let _ = std::fs::remove_dir_all(persist_path);

        println!("\n开始 persist 到磁盘: {}", persist_path);
        let persist_start = std::time::Instant::now();

        let disk_keyword = keyword.persist(persist_path).unwrap();

        println!("Persist 完成，耗时: {:?}", persist_start.elapsed());

        // 4. 验证磁盘索引
        let disk_len = disk_keyword.indexs.read().unwrap().len();
        println!("磁盘索引大小: {} 个不同的 tag", disk_len);
        assert_eq!(memory_len, disk_len);

        // 5. 测试读取功能
        println!("\n测试磁盘读取...");
        let test_keys = vec!["tag0", "tag100", "tag500", "tag999", "tag_not_exist"];
        for key in test_keys {
            let indexs = disk_keyword.indexs.read().unwrap();
            let bitmap = indexs.get_bitmap(&key.to_string());
            if let Some(bm) = bitmap {
                println!("  {}: {} 条记录", key, bm.len());
                assert!(bm.len() > 0);
            } else {
                println!("  {}: 不存在", key);
            }
        }

        // 6. 验证写操作应该失败（磁盘索引是只读的）
        println!("\n验证磁盘索引是只读的...");
        let write_result = std::panic::catch_unwind(|| {
            let id_vec = vec![999999u32];
            let mut builder = StringBuilder::new();
            builder.append_value("new_tag");
            let string_array = builder.finish();

            let data = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::UInt32, false),
                    Field::new("tags", DataType::Utf8, true),
                ])),
                vec![Arc::new(UInt32Array::from(id_vec)), Arc::new(string_array)],
            )
            .unwrap();

            disk_keyword.write(&data).unwrap();
        });

        assert!(write_result.is_err(), "磁盘索引应该不支持写操作");
        println!("确认：磁盘索引拒绝写操作 ✓");

        // 清理测试目录
        let _ = std::fs::remove_dir_all(persist_path);
        println!("\n测试完成！");
    }

    #[test]
    fn bench_keyword_persist_1m() {
        use arrow::array::StringBuilder;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        // 1. 创建内存索引并写入 100万条数据
        let field = FieldOption::Keyword {
            name: "tags".to_string(),
            is_array: false,
            index: true,
        };

        let keyword = Keyword::new(&field);

        println!("写入 100万条记录...");
        let start = std::time::Instant::now();

        // 写入 100万条记录，10000个不同的tag
        use rand::Rng;
        let record_count = 1_000_000;
        let tag_count = 10_000;
        let batch_size = 1000;
        let batch_count = record_count / batch_size;
        let mut rng = rand::rng();

        for batch in 0..batch_count {
            let mut id_vec = Vec::with_capacity(batch_size);
            let mut builder = StringBuilder::new();

            for i in 0..batch_size {
                id_vec.push((batch * batch_size + i) as u32);
                let tag = format!("tag{}", rng.random_range(0..tag_count));
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

            if (batch + 1) % 100 == 0 {
                println!(
                    "  已写入 {} 万条，耗时: {:?}",
                    (batch + 1) / 10,
                    start.elapsed()
                );
            }
        }

        let write_elapsed = start.elapsed();
        println!("\n100万条记录写入完成!");
        println!("写入耗时: {:?}", write_elapsed);
        println!(
            "写入速度: {:.0} 条/秒",
            record_count as f64 / write_elapsed.as_secs_f64()
        );

        // 2. 验证内存索引
        let memory_len = keyword.indexs.read().unwrap().len();
        println!("\n内存索引大小: {} 个不同的 tag", memory_len);

        // 3. Persist 到磁盘
        let persist_path = "/tmp/bench_keyword_persist_1m";
        let _ = std::fs::remove_dir_all(persist_path);

        println!("\n开始 persist 到磁盘: {}", persist_path);
        let persist_start = std::time::Instant::now();

        let disk_keyword = keyword.persist(persist_path).unwrap();

        let persist_elapsed = persist_start.elapsed();
        println!("Persist 完成，耗时: {:?}", persist_elapsed);
        println!(
            "Persist 速度: {:.0} keys/秒",
            memory_len as f64 / persist_elapsed.as_secs_f64()
        );

        // 4. 验证磁盘索引
        let disk_len = disk_keyword.indexs.read().unwrap().len();
        println!("\n磁盘索引大小: {} 个不同的 tag", disk_len);
        assert_eq!(memory_len, disk_len);

        // 5. 测试读取性能
        println!("\n测试磁盘读取性能...");
        let read_start = std::time::Instant::now();
        let test_count = 1000;

        for i in 0..test_count {
            let key = format!("tag{}", i);
            let indexs = disk_keyword.indexs.read().unwrap();
            let _bitmap = indexs.get_bitmap(&key);
        }

        let read_elapsed = read_start.elapsed();
        println!("随机读取 {} 次耗时: {:?}", test_count, read_elapsed);
        println!("平均单次读取: {:?}", read_elapsed / test_count);

        // 6. 检查文件大小
        let node_path = format!("{}/node", persist_path);
        let data_path = format!("{}/data", persist_path);
        if let Ok(node_meta) = std::fs::metadata(&node_path) {
            println!(
                "\nNODE 文件大小: {:.2} MB",
                node_meta.len() as f64 / 1024.0 / 1024.0
            );
        }
        if let Ok(data_meta) = std::fs::metadata(&data_path) {
            println!(
                "DATA 文件大小: {:.2} MB",
                data_meta.len() as f64 / 1024.0 / 1024.0
            );
        }

        // 清理测试目录
        let _ = std::fs::remove_dir_all(persist_path);
        println!("\n测试完成！");
    }
}
