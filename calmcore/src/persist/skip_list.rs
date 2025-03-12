// use std::{
//     borrow::Cow,
//     collections::BTreeMap,
//     fs::{File, OpenOptions},
//     io::{self, BufWriter, Read, Seek, SeekFrom, Write},
//     path::{Path, PathBuf},
// };

// use flate2::{
//     write::{DeflateEncoder, GzEncoder},
//     Compression,
// };
// use mem_btree::BTree;

// pub trait KVSerializer<K, V>: Send + Sync {
//     fn serialize_key<'a>(&self, k: &'a K) -> Cow<'a, [u8]>;
//     fn serialize_value<'a>(&self, v: &'a V) -> Cow<'a, [u8]>;
// }

// // 索引项，存储键值对在文件中的位置信息
// #[derive(Debug, Clone)]
// struct IndexEntry {
//     offset: u64, // 数据块在文件中的偏移量
//     length: u32, // 数据块长度
// }

// pub struct SkipListWriter<K, V> {
//     path: PathBuf,
//     skip_len: usize,
//     serializer: Box<dyn KVSerializer<K, V>>,
//     compression_level: Compression,
// }

// impl<K, V> SkipListWriter<K, V> {
//     pub fn new<P: AsRef<Path>>(
//         path: P,
//         skip_len: usize,
//         serializer: impl KVSerializer<K, V> + 'static,
//     ) -> Self {
//         Self {
//             path: path.as_ref().to_path_buf(),
//             skip_len,
//             serializer: Box::new(serializer),
//             compression_level: Compression::default(),
//         }
//     }

//     pub fn with_compression_level(mut self, level: u32) -> Self {
//         self.compression_level = Compression::new(level);
//         self
//     }
// }

// impl<K, V> SkipListWriter<K, V>
// where
//     K: Ord,
// {
//     pub fn persist(self, tree: BTree<K, V>) -> io::Result<()> {
//         // 1. 创建数据文件和索引
//         let file = OpenOptions::new()
//             .write(true)
//             .create(true)
//             .truncate(true)
//             .open(&self.path)?;

//         let mut writer = BufWriter::new(file);

//         // 2. 写入文件头部信息
//         let header = [b'S', b'K', b'I', b'P']; // 文件标识
//         writer.write_all(&header)?;

//         // 写入版本号
//         writer.write_all(&[1, 0])?; // 版本 1.0

//         // 写入跳表间隔
//         writer.write_all(&(self.skip_len as u32).to_le_bytes())?;

//         // 3. 准备构建索引并写入数据
//         let mut index = BTreeMap::new();
//         let mut current_offset = 8 + 4; // 头部长度 8 字节 + skip_len 4 字节
//         let mut counter = 0;
//         let mut last_indexed_key: Option<Vec<u8>> = None;

//         // 4. 遍历树中的每个键值对并写入到文件中
//         let mut entries = Vec::new();
//         let iter = tree.iter();
//         for entry in iter {
//             let key = self.serializer.serialize_key(&entry.0).to_vec();
//             let value = self.serializer.serialize_value(&entry.1).to_vec();

//             // 每隔 skip_len 个键值对创建一个索引项
//             if counter % self.skip_len == 0 {
//                 last_indexed_key = Some(key.clone());
//                 index.insert(
//                     key.clone(),
//                     IndexEntry {
//                         offset: current_offset,
//                         length: 0, // 暂时设为 0，后续更新
//                     },
//                 );
//             }

//             // 保存键值对和其偏移量，稍后再一次性写入
//             entries.push((key, value, current_offset));

//             // 计算下一个键值对的偏移位置
//             // key长度(4字节) + key + value长度(4字节) + value
//             current_offset += 4 + key.len() as u64 + 4 + value.len() as u64;
//             counter += 1;
//         }

//         // 5. 创建索引区域的临时占位符
//         // 记录数据起始位置
//         let data_start_offset = current_offset + 8; // 索引大小占用 8 字节

//         // 写入索引大小占位符
//         writer.write_all(&[0u8; 8])?;

//         // 6. 压缩并写入实际数据
//         let mut data_encoder = GzEncoder::new(Vec::new(), self.compression_level);

//         // 写入每个键值对
//         for (key, value, _) in &entries {
//             // 写入键长度和键
//             data_encoder.write_all(&(key.len() as u32).to_le_bytes())?;
//             data_encoder.write_all(key)?;

//             // 写入值长度和值
//             data_encoder.write_all(&(value.len() as u32).to_le_bytes())?;
//             data_encoder.write_all(value)?;
//         }

//         // 获取压缩后的数据
//         let compressed_data = data_encoder.finish()?;
//         let compressed_size = compressed_data.len();

//         // 写入压缩后的数据
//         writer.write_all(&compressed_data)?;

//         // 7. 更新索引中的长度信息
//         for i in 0..entries.len() {
//             if i + 1 < entries.len() {
//                 if let Some((key, _, offset)) = entries.get(i) {
//                     if let Some((_, _, next_offset)) = entries.get(i + 1) {
//                         if let Some(entry) = index.get_mut(key) {
//                             entry.length = (next_offset - offset) as u32;
//                         }
//                     }
//                 }
//             }
//         }

//         // 为最后一个索引条目设置长度
//         if let Some(last_key) = last_indexed_key {
//             if let Some(entry) = index.get_mut(&last_key) {
//                 entry.length = (current_offset - entry.offset) as u32;
//             }
//         }

//         // 8. 序列化并写入索引
//         let mut index_data = Vec::new();

//         // 写入索引项数量
//         index_data.write_all(&(index.len() as u32).to_le_bytes())?;

//         // 写入每个索引项
//         for (key, entry) in &index {
//             // 写入键长度和键
//             index_data.write_all(&(key.len() as u32).to_le_bytes())?;
//             index_data.write_all(key)?;

//             // 写入偏移量和长度
//             index_data.write_all(&entry.offset.to_le_bytes())?;
//             index_data.write_all(&entry.length.to_le_bytes())?;
//         }

//         // 9. 压缩索引数据
//         let mut index_encoder = GzEncoder::new(Vec::new(), self.compression_level);
//         index_encoder.write_all(&index_data)?;
//         let compressed_index = index_encoder.finish()?;

//         // 10. 将文件指针定位到索引大小占位符位置
//         writer.flush()?;
//         writer
//             .get_mut()
//             .seek(SeekFrom::Start(data_start_offset - 8))?;

//         // 写入压缩后的索引大小
//         writer.write_all(&(compressed_index.len() as u64).to_le_bytes())?;

//         // 11. 将文件指针定位到数据区域末尾
//         writer
//             .get_mut()
//             .seek(SeekFrom::Start(data_start_offset + compressed_size as u64))?;

//         // 写入压缩后的索引数据
//         writer.write_all(&compressed_index)?;

//         // 12. 确保所有数据都写入磁盘
//         writer.flush()?;

//         Ok(())
//     }
// }

// pub struct SkipListReader<K, V> {
//     path: PathBuf,
//     skip_len: usize,
//     index: BTreeMap<Vec<u8>, IndexEntry>,
//     serializer: Box<dyn KVSerializer<K, V>>,
//     file: File,
//     data_offset: u64,
// }

// impl<K, V> SkipListReader<K, V>
// where
//     K: Ord,
// {
//     pub fn open<P: AsRef<Path>>(
//         path: P,
//         serializer: impl KVSerializer<K, V> + 'static,
//     ) -> io::Result<Self> {
//         let path = path.as_ref().to_path_buf();
//         let mut file = File::open(&path)?;

//         // 读取文件头
//         let mut header = [0u8; 4];
//         file.read_exact(&mut header)?;

//         if &header != b"SKIP" {
//             return Err(io::Error::new(
//                 io::ErrorKind::InvalidData,
//                 "Invalid skip list file format",
//             ));
//         }

//         // 读取版本
//         let mut version = [0u8; 2];
//         file.read_exact(&mut version)?;

//         // 读取跳表间隔
//         let mut skip_len_bytes = [0u8; 4];
//         file.read_exact(&mut skip_len_bytes)?;
//         let skip_len = u32::from_le_bytes(skip_len_bytes) as usize;

//         // 读取索引区大小
//         let data_offset = 8 + 4; // 头部 + skip_len
//         file.seek(SeekFrom::Start(data_offset))?;

//         let mut index_size_bytes = [0u8; 8];
//         file.read_exact(&mut index_size_bytes)?;
//         let index_size = u64::from_le_bytes(index_size_bytes);

//         // 计算索引的位置
//         let mut reader = Self {
//             path,
//             skip_len,
//             index: BTreeMap::new(),
//             serializer: Box::new(serializer),
//             file,
//             data_offset: data_offset + 8, // 数据开始位置 = 头部 + skip_len + 索引大小占位符
//         };

//         // 加载索引
//         reader.load_index(index_size)?;

//         Ok(reader)
//     }

//     fn load_index(&mut self, index_size: u64) -> io::Result<()> {
//         // 待实现: 从文件加载索引部分
//         // 此处为了简洁省略具体实现，实际应该读取索引区并解析为BTreeMap
//         Ok(())
//     }

//     // 根据键获取值，使用索引进行快速定位
//     pub fn get(&mut self, key: &K) -> io::Result<Option<Vec<u8>>> {
//         // 待实现: 使用索引快速定位并读取数据
//         // 此处为了简洁省略具体实现
//         Ok(None)
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use flate2::read::GzDecoder;
//     use mem_btree::BTree;
//     use std::fs;
//     use std::io::{BufReader, Read};
//     use tempfile::tempdir;

//     // 测试用序列化器，用于将字符串键和值序列化为字节
//     struct TestSerializer;

//     impl KVSerializer<String, String> for TestSerializer {
//         fn serialize_key<'a>(&self, k: &'a String) -> Cow<'a, [u8]> {
//             Cow::Borrowed(k.as_bytes())
//         }

//         fn serialize_value<'a>(&self, v: &'a String) -> Cow<'a, [u8]> {
//             Cow::Borrowed(v.as_bytes())
//         }
//     }

//     // 辅助函数：填充BTree测试数据
//     fn create_test_tree(size: usize) -> BTree<String, String> {
//         let mut tree = BTree::new(32);
//         for i in 0..size {
//             let key = format!("key-{:08}", i);
//             let value = format!("value-for-key-{}", i);
//             tree.put(key, value);
//         }
//         tree
//     }

//     #[test]
//     fn test_basic_write_read() {
//         // 创建临时目录以避免测试垃圾文件
//         let temp_dir = tempdir().expect("无法创建临时目录");
//         let file_path = temp_dir.path().join("test_basic.skip");

//         // 创建测试树并写入文件
//         let tree = create_test_tree(100);
//         let writer = SkipListWriter::new(&file_path, 10, TestSerializer);
//         writer.persist(tree).expect("写入失败");

//         // 检查文件是否存在且大小合理
//         assert!(file_path.exists());
//         let metadata = fs::metadata(&file_path).expect("无法获取文件元数据");
//         assert!(metadata.len() > 0, "生成的文件为空");

//         // 打开文件并检查文件头
//         let mut file = File::open(&file_path).expect("无法打开文件");
//         let mut header = [0u8; 4];
//         file.read_exact(&mut header).expect("无法读取文件头");
//         assert_eq!(&header, b"SKIP", "文件头不正确");
//     }

//     #[test]
//     fn test_compression_levels() {
//         // 创建临时目录
//         let temp_dir = tempdir().expect("无法创建临时目录");

//         // 生成一个有大量重复内容的大树（可压缩性高）
//         let mut tree = BTree::new(32);
//         for i in 0..1000 {
//             let key = format!("key-{:08}", i);
//             // 使用重复内容让压缩效率更明显
//             let value = "a".repeat(100);
//             tree.put(key, value);
//         }

//         // 使用不同压缩等级写入文件
//         let file_path_low = temp_dir.path().join("test_low_compression.skip");
//         let file_path_high = temp_dir.path().join("test_high_compression.skip");

//         // 低压缩级别
//         let writer_low =
//             SkipListWriter::new(&file_path_low, 10, TestSerializer).with_compression_level(1);
//         writer_low
//             .persist(tree.clone())
//             .expect("低压缩级别写入失败");

//         // 高压缩级别
//         let writer_high =
//             SkipListWriter::new(&file_path_high, 10, TestSerializer).with_compression_level(9);
//         writer_high.persist(tree).expect("高压缩级别写入失败");

//         // 检查文件大小，高压缩级别应该产生更小的文件
//         let size_low = fs::metadata(&file_path_low)
//             .expect("无法获取低压缩文件元数据")
//             .len();
//         let size_high = fs::metadata(&file_path_high)
//             .expect("无法获取高压缩文件元数据")
//             .len();

//         println!("低压缩级别文件大小: {} 字节", size_low);
//         println!("高压缩级别文件大小: {} 字节", size_high);

//         // 高压缩级别应该产生更小的文件，或至少不会大很多
//         assert!(
//             size_high <= size_low * 105 / 100,
//             "高压缩级别未能有效减小文件大小"
//         );
//     }

//     #[test]
//     fn test_skip_intervals() {
//         // 创建临时目录
//         let temp_dir = tempdir().expect("无法创建临时目录");

//         // 创建测试树
//         let tree = create_test_tree(1000);

//         // 使用不同跳表间隔写入文件
//         let file_path_small = temp_dir.path().join("test_small_interval.skip");
//         let file_path_large = temp_dir.path().join("test_large_interval.skip");

//         // 小间隔（更多索引项）
//         let writer_small = SkipListWriter::new(&file_path_small, 5, TestSerializer);
//         writer_small.persist(tree.clone()).expect("小间隔写入失败");

//         // 大间隔（更少索引项）
//         let writer_large = SkipListWriter::new(&file_path_large, 50, TestSerializer);
//         writer_large.persist(tree).expect("大间隔写入失败");

//         // 文件大小比较 - 小间隔会产生更大的索引，通常导致更大的文件
//         let size_small = fs::metadata(&file_path_small)
//             .expect("无法获取小间隔文件元数据")
//             .len();
//         let size_large = fs::metadata(&file_path_large)
//             .expect("无法获取大间隔文件元数据")
//             .len();

//         println!("小间隔(5)文件大小: {} 字节", size_small);
//         println!("大间隔(50)文件大小: {} 字节", size_large);

//         // 小间隔通常产生更大的索引，导致文件更大
//         assert!(size_small > size_large, "小间隔应该产生更大的索引和文件");
//     }

//     #[test]
//     fn test_large_dataset() {
//         if std::env::var("RUN_LARGE_TESTS").is_err() {
//             println!("跳过大数据集测试。设置 RUN_LARGE_TESTS=1 环境变量以启用。");
//             return;
//         }

//         // 创建临时目录
//         let temp_dir = tempdir().expect("无法创建临时目录");
//         let file_path = temp_dir.path().join("test_large.skip");

//         // 创建大规模测试树 (100,000 个项目)
//         println!("创建大规模测试数据集...");
//         let tree = create_test_tree(100_000);
//         println!("数据集创建完成，开始持久化...");

//         // 写入文件并记录时间
//         let start = std::time::Instant::now();
//         let writer = SkipListWriter::new(&file_path, 100, TestSerializer);
//         writer.persist(tree).expect("大数据集写入失败");
//         let duration = start.elapsed();

//         println!("写入 100,000 项目耗时: {:?}", duration);
//         println!(
//             "文件大小: {} 字节",
//             fs::metadata(&file_path).expect("无法获取文件元数据").len()
//         );

//         // 检查文件头以验证至少基本的文件结构是正确的
//         let mut file = File::open(&file_path).expect("无法打开文件");
//         let mut header = [0u8; 4];
//         file.read_exact(&mut header).expect("无法读取文件头");
//         assert_eq!(&header, b"SKIP", "文件头不正确");
//     }

//     #[test]
//     fn test_file_format() {
//         // 创建临时目录
//         let temp_dir = tempdir().expect("无法创建临时目录");
//         let file_path = temp_dir.path().join("test_format.skip");

//         // 创建小型测试树
//         let tree = create_test_tree(10);

//         // 写入文件
//         let writer = SkipListWriter::new(&file_path, 2, TestSerializer);
//         writer.persist(tree).expect("写入失败");

//         // 直接读取文件检查格式
//         let mut file = File::open(&file_path).expect("无法打开文件");

//         // 检查文件头
//         let mut buffer = [0u8; 4];
//         file.read_exact(&mut buffer).expect("无法读取文件头");
//         assert_eq!(&buffer, b"SKIP", "文件头不正确");

//         // 检查版本号
//         let mut version = [0u8; 2];
//         file.read_exact(&mut version).expect("无法读取版本号");
//         assert_eq!(version, [1, 0], "版本号不正确");

//         // 检查跳表间隔
//         let mut skip_len = [0u8; 4];
//         file.read_exact(&mut skip_len).expect("无法读取跳表间隔");
//         let skip_len_value = u32::from_le_bytes(skip_len);
//         assert_eq!(skip_len_value, 2, "跳表间隔不正确");

//         // 检查索引大小存在
//         let mut index_size = [0u8; 8];
//         file.read_exact(&mut index_size).expect("无法读取索引大小");
//         let index_size_value = u64::from_le_bytes(index_size);
//         assert!(index_size_value > 0, "索引大小应大于零");

//         println!("文件格式检查通过");
//     }

//     // 当SkipListReader完成实现后可添加读取测试
//     // #[test]
//     // fn test_reader() {
//     //     // 此测试将在 SkipListReader 完成后实现
//     // }
// }
