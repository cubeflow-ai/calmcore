# BTree 设计文档

## 1. 概述

BTree 是整个存储引擎的基础数据结构，用于高效的键值存储和范围查询。它支持内存和磁盘两种模式，通过 mmap 实现零拷贝的磁盘访问。

## 2. 核心特性

### 2.1 双模式设计

BTree 支持两种运行模式：

1. **Memory 模式**
   - 纯内存 B+树实现
   - 支持快速插入、查询、删除
   - 使用 Arc + RwLock 实现并发安全
   - 适合活跃数据的高速写入

2. **Disk 模式**
   - 基于 mmap 的只读 B+树
   - 零拷贝磁盘访问
   - 支持快速范围查询和点查询
   - 适合冷数据的持久化存储

### 2.2 关键方法

```rust
// 内存模式操作
pub fn insert(&self, key: K, value: V) -> Result<()>
pub fn get(&self, key: &K) -> Option<V>
pub fn remove(&self, key: &K) -> Option<V>

// 查询操作（内存和磁盘模式通用）
pub fn floor(&self, key: &K) -> Option<(K, V)>  // 小于等于key的最大值
pub fn ceiling(&self, key: &K) -> Option<(K, V)> // 大于等于key的最小值
pub fn range(&self, start: &K, end: &K) -> Iterator<(K, V)>

// 迭代器
pub fn iter(&self) -> Iterator<(K, V, Option<TTL>)>
```

### 2.3 floor() 方法实现细节

`floor()` 方法是查询性能的关键，用于快速定位包含指定文档的 RecordBatch：

**算法流程：**

```
1. 从根节点开始向下查找
2. 在每个节点中找到 <= key 的最大键
3. 如果找到精确匹配，返回该键值对
4. 如果没有精确匹配，返回最接近的较小键
5. 递归向下直到叶子节点
```

**时间复杂度：** O(log n)

**使用场景：**

- 根据 doc_id 定位所属的 RecordBatch
- 例如：keys = [0, 100, 200]，query floor(150) → 返回 key=100 的batch

## 3. 持久化机制

### 3.1 TreeWriter - 写入器

负责将内存 BTree 持久化到磁盘：

```rust
pub struct TreeWriter {
    path: PathBuf,
    chunk_size: usize,  // BTree 节点的分块大小
    key_len: u16,       // 固定长度key的大小（0表示变长）
}
```

**持久化流程：**

1. **数据准备阶段**

   ```rust
   let iter = memory_tree.iter();
   let len = memory_tree.len();
   ```

2. **序列化阶段**
   - 使用自定义 Serializer 序列化每个 (key, value) 对
   - 支持压缩（zstd）减少磁盘空间

3. **写入阶段**

   ```rust
   writer.persist(len, serializer, iter)?;
   ```

   - 按 chunk_size 分块写入
   - 构建 B+树索引结构
   - 生成元数据文件

4. **文件结构**

   ```
   {path}/
   ├── data.bin      # 实际数据（压缩后的KV对）
   ├── index.bin     # B+树索引结构
   └── meta.json     # 元数据（版本、大小、配置等）
   ```

### 3.2 TreeReader - 读取器

基于 mmap 实现零拷贝读取：

```rust
pub struct TreeReader<K, V> {
    mmap: Mmap,              // 内存映射文件
    index_offset: usize,     // 索引起始位置
    data_offset: usize,      // 数据起始位置
    deserializer: Box<dyn Deserializer<K, V>>,
    _phantom: PhantomData<(K, V)>,
}
```

**读取流程：**

1. **初始化**
   - 打开文件并创建 mmap
   - 读取元数据
   - 定位索引和数据区域

2. **查询操作**
   - 通过索引快速定位数据位置
   - 从 mmap 中零拷贝读取数据
   - 反序列化返回结果

3. **性能优化**
   - 操作系统页面缓存
   - 预读优化
   - 无需加载整个文件到内存

## 4. 序列化器设计

### 4.1 Serializer Trait

```rust
pub trait Serializer<K, V>: Send + Sync {
    fn serialize(&self, key: &K, value: &V, writer: &mut dyn Write) -> Result<()>;
    fn key_len(&self) -> u16;  // 0 表示变长key
}
```

### 4.2 Deserializer Trait

```rust
pub trait Deserializer<K, V>: Send + Sync {
    fn deserialize(&self, buffer: &[u8], pos: &mut usize) -> Result<(K, V)>;
}
```

### 4.3 内置序列化器

1. **U32RecordBatchSerializer**
   - Key: u32 (doc_id)
   - Value: RecordBatch (Arrow format)
   - 支持 zstd 压缩

2. **StringRoaringSerializer**
   - Key: String (term)
   - Value: RoaringBitmap (posting list)
   - 高效的倒排索引存储

## 5. 性能特征

### 5.1 内存模式性能

- **插入**: O(log n) - 约 1000万 ops/秒
- **查询**: O(log n) - 约 2000万 ops/秒
- **删除**: O(log n) - 约 800万 ops/秒

### 5.2 磁盘模式性能

- **点查询**: O(log n) - 约 100万 ops/秒
- **范围查询**: O(log n + m) - m 为结果数量
- **floor/ceiling**: O(log n) - 约 80万 ops/秒

### 5.3 持久化性能

**测试数据：300万条记录**

- **写入速度**: 约 850ms
- **文件大小**: 约 25-55 MB（取决于数据）
- **压缩比**: 约 60-70%（使用 zstd level 3）
- **加载时间**: < 1ms（mmap 延迟加载）

## 6. 并发控制

### 6.1 内存模式并发

```rust
pub struct BTree<K, V> {
    root: Arc<RwLock<Node<K, V>>>,
    // ...
}
```

- 使用 RwLock 实现读写锁
- 支持多读单写
- 写操作获取写锁
- 读操作获取读锁，并发读

### 6.2 磁盘模式并发

- 只读操作，天然支持并发
- 多线程可同时读取同一 mmap 区域
- 操作系统保证页面缓存一致性

## 7. 内存管理

### 7.1 内存占用估算

**内存模式：**

- 每个节点: 约 1KB
- 节点数: 约 N/64 (假设每节点64个key)
- 100万条记录约占用: 15-20 MB

**磁盘模式：**

- 仅占用极少的元数据内存
- 实际数据通过 mmap 按需加载
- 依赖操作系统页面缓存

### 7.2 TTL 支持

```rust
pub struct BTree<K, V> {
    ttl: Option<Duration>,  // 全局TTL配置
}
```

- 支持记录级别的过期时间
- 懒惰删除策略
- 查询时自动过滤过期记录

## 8. 错误处理

```rust
pub enum BTreeError {
    IOError(String),
    SerializationError(String),
    CorruptedData(String),
    KeyNotFound,
    InvalidOperation(String),
}
```

## 9. 使用示例

### 9.1 内存模式使用

```rust
// 创建内存BTree
let tree = BTree::new(64); // 节点容量64

// 插入数据
tree.insert(1, "value1")?;
tree.insert(2, "value2")?;

// 查询
if let Some(value) = tree.get(&1) {
    println!("Found: {}", value);
}

// floor查询
if let Some((key, value)) = tree.floor(&5) {
    println!("Floor of 5: key={}, value={}", key, value);
}
```

### 9.2 持久化使用

```rust
// 持久化到磁盘
let serializer = MySerializer::default();
let writer = TreeWriter::new(
    PathBuf::from("/path/to/data"),
    128,  // chunk_size
    0,    // key_len (variable)
);

let iter = tree.iter();
writer.persist(tree.len(), Box::new(serializer), iter)?;
```

### 9.3 磁盘模式使用

```rust
// 从磁盘加载
let deserializer = MyDeserializer::default();
let reader = TreeReader::new(
    "/path/to/data",
    Box::new(deserializer)
)?;

// 查询
if let Some(value) = reader.get(&key) {
    println!("Found: {:?}", value);
}

// floor查询
if let Some((k, v)) = reader.floor(&query_key) {
    println!("Found floor: key={:?}", k);
}
```

## 10. 未来优化方向

### 10.1 性能优化

1. **缓存层**
   - 添加 LRU 缓存热点数据
   - 减少磁盘访问频率

2. **批量操作**
   - 支持批量插入/删除
   - 减少锁竞争

3. **并行持久化**
   - 多线程并行写入
   - 提升持久化速度

### 10.2 功能增强

1. **增量持久化**
   - 只持久化变更部分
   - 支持快照和增量

2. **压缩策略**
   - 可配置压缩算法
   - 平衡压缩比和性能

3. **数据校验**
   - CRC32 校验和
   - 检测数据损坏

## 11. 总结

BTree 模块提供了：

- ✅ 高性能的内存键值存储
- ✅ 零拷贝的磁盘读取
- ✅ 灵活的序列化机制
- ✅ 完善的并发控制
- ✅ 可靠的持久化方案

是整个存储引擎的基石，为上层的 Field、RowData、Segment、Partition 提供了强大的底层支持。
