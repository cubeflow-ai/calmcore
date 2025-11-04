# RowData BTree 持久化格式详解

## 概述

RowData 使用自定义的 **BTree 磁盘索引格式** 进行持久化，而不是 Parquet。这种格式针对 **随机访问** 和 **主键查询** 进行了优化。

## 核心设计理念

### 为什么用 BTree 而不是 Parquet？

| 特性 | BTree 格式 | Parquet 格式 |
|------|-----------|-------------|
| **随机访问** | ✅ O(log N) 查找 | ❌ 需要扫描 RowGroup |
| **读取少量行** | ✅ 只读取需要的行 | ❌ 需要读取整个 RowGroup |
| **主键查询** | ✅ 直接通过 doc_id 定位 | ❌ 需要遍历或外部索引 |
| **存储压缩** | ⚠️ 中等 | ✅ 列式压缩，很好 |
| **生态工具** | ❌ 自定义格式 | ✅ 行业标准 |

**结论**: 对于需要频繁主键查询的场景（如你的 `get_by_pk`），BTree 格式性能远超 Parquet。

---

## 文件结构

每个 segment 持久化后会创建一个目录，包含以下文件：

```
segment-0-99999/
├── data          # 存储实际的 RecordBatch 数据（序列化后）
├── node          # BTree 索引结构
├── field-id      # 主键字段的倒排索引
├── field-name    # name 字段的倒排索引
├── ...           # 其他字段的倒排索引
├── pk_bloomfilter    # 主键 hash 的 bloomfilter
├── deleted       # 删除标记的 bitmap
└── meta.json     # 元数据（start, doc_id_gen, max_doc_id）
```

---

## BTree 文件格式详解

### 1. Node 文件结构

```
┌─────────────────────────────────────────────────────────┐
│ MAGIC_VERSION (2 bytes) = [95, 67]                     │
├─────────────────────────────────────────────────────────┤
│ root_offset (8 bytes, BigEndian)                       │  ← 根节点在文件中的位置
├─────────────────────────────────────────────────────────┤
│ key_len (2 bytes, BigEndian)                           │  ← 0 表示变长key，>0表示固定长度
├─────────────────────────────────────────────────────────┤
│ tree_len (4 bytes, BigEndian)                          │  ← 总共有多少个key-value对
├─────────────────────────────────────────────────────────┤
│                                                         │
│                   BTree 节点数据                         │
│                 (多层索引结构)                           │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### 2. 单个 BTree 节点格式

```
┌─────────────────────────────────────────────────────────┐
│ is_leaf (1 byte)                                       │  ← 1=叶子节点, 0=索引节点
├─────────────────────────────────────────────────────────┤
│ keys_len (4 bytes, BigEndian)                          │  ← keys 数据的字节长度
├─────────────────────────────────────────────────────────┤
│ keys_data (keys_len bytes)                             │  ← 序列化的多个 key
│   - 对于 u32 类型: [key1, key2, key3, ...]             │
│   - 使用自定义序列化器                                   │
├─────────────────────────────────────────────────────────┤
│ offsets_len (4 bytes, BigEndian)                       │  ← offsets 数组长度
├─────────────────────────────────────────────────────────┤
│ offsets (variable length, zigzag encoded)              │  ← 每个 key 对应的偏移量
│   - 如果 is_leaf=1: 指向 data 文件中的位置             │
│   - 如果 is_leaf=0: 指向 node 文件中子节点的位置        │
└─────────────────────────────────────────────────────────┘
```

### 3. Data 文件结构

```
┌─────────────────────────────────────────────────────────┐
│ MAGIC_VERSION (2 bytes) = [95, 67]                     │
├─────────────────────────────────────────────────────────┤
│                                                         │
│              连续存储的 RecordBatch 数据                  │
│                                                         │
│  ┌──────────────────────────────────────────┐          │
│  │ RecordBatch 1 (IPC 格式序列化)            │          │
│  ├──────────────────────────────────────────┤          │
│  │ RecordBatch 2 (IPC 格式序列化)            │          │
│  ├──────────────────────────────────────────┤          │
│  │ ...                                       │          │
│  └──────────────────────────────────────────┘          │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## 查询过程详解

### 示例：查询 doc_id = 50000

假设我们有一个持久化的 segment，包含 100,000 条记录，chunk_size = 128。

#### 1. 文件加载（使用 mmap）

```rust
// 使用内存映射文件，不需要全部加载到内存
let node_mmap = memmap2::Mmap::map(&node_file)?;  // 映射 node 文件
let data_mmap = memmap2::Mmap::map(&data_file)?;  // 映射 data 文件
```

**优点**:

- 只有访问时才真正从磁盘读取（lazy loading）
- 操作系统自动管理页面缓存
- 多次查询可以复用缓存

#### 2. BTree 查找过程

```
查询 key = 50000

Root Node (Level 2):
  keys:    [0, 16384, 32768, 49152, 65536, 81920, 98304]
  offsets: [off_0, off_1, off_2, off_3, off_4, off_5, off_6]
  
  → 50000 >= 49152 且 < 65536
  → 跳转到 offset_3 指向的节点

Index Node (Level 1) at offset_3:
  keys:    [49152, 49280, 49408, 49536, 49664, ..., 50176]
  offsets: [off_a, off_b, off_c, off_d, off_e, ..., off_x]
  
  → 50000 >= 49664 且 < 50176
  → 跳转到 offset_e 指向的节点

Leaf Node (Level 0) at offset_e:
  keys:    [49664, 49665, 49666, ..., 50000, ..., 50176]
  offsets: [123456, 123789, 124012, ..., 187623, ..., 199234]
  
  → 找到 key=50000
  → start_offset = 187623
  → end_offset = 187956 (下一个key的offset)
  → value_size = 187956 - 187623 = 333 bytes
```

#### 3. 读取数据

```rust
// 从 data 文件读取 RecordBatch
let value_bytes = &data_mmap[187623..187956];  // 只读取 333 字节
let record_batch = deserialize_from_ipc(value_bytes)?;  // 反序列化
```

**性能**:

- 磁盘 IO: 通常 2-3 次（如果没有缓存）
  - 1 次读取 root node
  - 1 次读取 index node  
  - 1 次读取 leaf node
  - 1 次读取 data
- 时间复杂度: O(log N)
- 对于 100 万条记录: 约 log₁₂₈(1000000) ≈ 3 层

---

## 持久化过程

### 代码执行流程

```rust
// 1. 收集内存中的所有 RecordBatch
let all_batches = memory_tree.iter().collect();

// 2. 合并成一个大的 RecordBatch（优化后的实现）
let merged_batch = concat_batches(&schema, &all_batches)?;

// 3. 重新分批（每 1000 行一个 batch）
for chunk_start in (0..total_rows).step_by(1000) {
    let batch_key = internal_ids.value(chunk_start);  // 第一行的 doc_id
    let chunk_batch = merged_batch.slice(chunk_start, 1000);
    reorganized_batches.push((batch_key, chunk_batch));
}

// 4. 写入 BTree
let writer = TreeWriter::new(path, chunk_size=128, key_len=0);
writer.persist(
    batch_count,
    U32RecordBatchSerializer,  // 序列化器
    reorganized_batches.into_iter()
)?;
```

### 写入顺序

```
data 文件写入顺序:
  [batch_0] [batch_1] [batch_2] ... [batch_N]
  
node 文件构建过程:
  1. 创建 leaf nodes (每 128 个 batch 一个节点)
  2. 创建 index nodes (指向 leaf nodes)
  3. 如果还有多个 index nodes，继续创建上层
  4. 最后写入 root node offset 到文件头
```

---

## 性能优化技术

### 1. Memory Mapping (mmap)

- 不需要一次性加载整个文件
- 操作系统自动管理页面缓存
- 多进程可以共享同一份物理内存

### 2. Zigzag Encoding (offsets 压缩)

- 将 i64 编码为变长字节
- 小数字占用更少字节
- 例如: 100 → 1 byte, 1000000 → 3 bytes

### 3. Binary Search (二分查找)

- 在每个节点内使用二分查找
- 时间复杂度: O(log chunk_size)

### 4. Batch Size 优化

- chunk_size=128: 平衡节点大小和树高度
- RecordBatch batch_size=1000: 减少小对象数量

### 5. 预分配与 Slice

- 使用 `RecordBatch::slice()` 避免复制
- 直接切片引用原始数据

---

## 与 Parquet 的性能对比

### 场景: 从 100 万行中读取 10 个随机 doc_id

#### BTree 方式 (当前实现)

```
总 IO:
  - 10 个查询
  - 每个查询约 3 次节点访问 + 1 次数据读取
  - 约 40 次小的随机 IO
  - 总读取: ~10KB (节点) + ~3KB (数据) = 13KB
  
时间: 每个查询 0.01-0.1ms (缓存后更快)
```

#### Parquet 方式 (假设)

```
总 IO:
  - 读取文件 footer (1 次)
  - 对于每个目标行:
    - 找到所在 RowGroup（假设 10000 行/组）
    - 读取整个 RowGroup 的元数据
    - 读取相关列的数据页
  - 如果 10 个 ID 分散在不同 RowGroup: 需要读取 10 个 RowGroup
  - 总读取: ~1MB+ (包含大量不需要的行)
  
时间: 每个查询 1-10ms (取决于 RowGroup 大小)
```

**结论**: BTree 格式在随机访问场景下比 Parquet 快 **10-100 倍**。

---

## 实际文件大小示例

以 10 万条记录为例（每条约 200 字节）：

```
segment-0-99999/
├── data          ~20MB   (RecordBatch 数据)
├── node          ~800KB  (BTree 索引，约 4% 开销)
├── field-id      ~5MB    (倒排索引)
├── field-name    ~3MB    
├── ...
├── pk_bloomfilter ~100KB
├── deleted       ~1KB
└── meta.json     <1KB
──────────────────────────
总计: ~30MB (每条记录 ~300 字节)
```

**存储开销**: BTree 索引约占原始数据的 4%，这是为随机访问性能付出的合理代价。

---

## 总结

### BTree 格式的优势

1. ✅ **极快的随机访问**: O(log N) 查找
2. ✅ **只读取需要的数据**: 不浪费 IO
3. ✅ **支持 floor 查询**: 可以找到最接近的 key
4. ✅ **mmap 优化**: 操作系统级别的缓存管理

### 适用场景

- ✅ 主键查询为主的工作负载
- ✅ 需要读取少量特定行
- ✅ 高 QPS 随机访问
- ❌ 全表扫描（此时 Parquet 更优）
- ❌ 大批量分析查询（此时 Parquet 更优）

### 未来优化方向

1. 在 BTree 层面增加 zstd 压缩
2. 支持列裁剪（只读取需要的列）
3. 增加缓存层减少磁盘 IO
4. 支持并行查询多个 key

这就是 RowData 的 BTree 持久化格式！它专为你的主键查询场景优化，性能远超通用的 Parquet 格式。🚀
