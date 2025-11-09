# Add Segment 功能说明

## 概述

`add_segment_from_parquet` 是 Partition 的一个新方法,允许你直接添加外部 Parquet 文件作为一个 segment,而无需复制数据。这个功能的核心思想是:

- **RowData 使用引用**: 通过 `RowDataStore::Parquet` 引用外部 parquet 文件
- **索引在内存**: 为 parquet 数据构建索引,加速查询
- **零拷贝**: parquet 文件保持原样,不会被复制或移动

## 使用场景

### 1. 导入大量历史数据

```rust
// 你有一个包含大量历史数据的 parquet 文件
let historical_data = "/data/archive/2023_sales.parquet";

// 直接添加为 segment,无需数据拷贝
let seg_id = partition.add_segment_from_parquet(historical_data)?;

// 索引会在内存中构建,查询时可以直接使用
```

### 2. 混合在线/离线数据

```rust
// 先添加离线数据 (parquet 文件)
partition.add_segment_from_parquet("/offline/batch1.parquet")?;
partition.add_segment_from_parquet("/offline/batch2.parquet")?;

// 再写入在线数据 (upsert)
partition.upsert(online_batch)?;

// 查询时自动合并所有 segment
```

### 3. 数据湖集成

```rust
// 从数据湖导入预处理好的 parquet 文件
for parquet_file in datalake.list_parquet_files() {
    partition.add_segment_from_parquet(&parquet_file)?;
}
```

## API 说明

### Partition::add_segment_from_parquet

```rust
pub fn add_segment_from_parquet(&self, parquet_path: &str) -> CoreResult<u64>
```

**参数**:
- `parquet_path`: Parquet 文件的绝对路径

**返回**:
- `Ok(seg_id)`: 新添加的 segment ID
- `Err(...)`: 错误信息

**行为**:
1. 读取 parquet 文件元数据,获取行数
2. 根据当前 partition 状态确定 segment 的 ID 范围 (start_id, end_id)
3. 读取 parquet 数据,为每个字段构建索引
4. 创建引用该 parquet 文件的 Segment
5. 将 segment 添加到 frozen_segments
6. 更新 current_segment 的起始位置

### Segment::from_parquet

```rust
pub fn from_parquet(
    start: u64,
    end: u64,
    parquet_path: &str,
    data: &RecordBatch,
    schema: Arc<Schema>,
) -> CoreResult<Self>
```

**参数**:
- `start`: segment 起始文档 ID
- `end`: segment 结束文档 ID (inclusive)
- `parquet_path`: Parquet 文件路径
- `data`: RecordBatch (用于构建索引)
- `schema`: Schema 定义

**返回**:
- `Ok(segment)`: 新创建的 segment
- `Err(...)`: 错误信息

**特点**:
- `row_data` 使用 `RowDataStore::Parquet`,引用外部文件
- `persisted` 设置为 `true` (因为数据已在磁盘)
- `base_path` 设置为 parquet 文件路径
- 所有索引在内存中构建

## 架构设计

### RowDataStore 枚举

```rust
pub enum RowDataStore {
    Memory(mem_btree::BTree<u32, RecordBatch>),  // 内存模式
    Disk(Arc<TreeReader<u32, RecordBatch>>),     // BTree 磁盘模式
    Parquet(Arc<ParquetRowDataReader>),          // Parquet 引用模式 ⭐ NEW
}
```

新增的 `Parquet` 变体:
- 不持有数据,只持有文件路径和元数据
- 读取时按需从 parquet 文件加载
- 支持列投影,减少 I/O

### 数据流图

```
外部 Parquet 文件
       │
       │ (引用,不复制)
       ▼
   Segment
       ├─ row_data: RowDataStore::Parquet ──────┐
       │                                         │
       ├─ fields: Vec<IndexWriter> (内存索引)   │
       │                                         │
       └─ pk_bloomfilter: Bloom (内存)          │
                                                 │
查询时                                           │
       ├─ 通过索引定位文档                      │
       └─ 从 Parquet 读取 RowData ◄─────────────┘
```

## 示例代码

### 基础用法

```rust
use calm::partition::Partition;
use std::path::PathBuf;

// 创建 partition
let partition = Partition::new(0, PathBuf::from("/data"), schema, tx);

// 添加外部 parquet 文件
let seg_id = partition.add_segment_from_parquet("/external/data.parquet")?;

println!("Added segment: {}", seg_id);
```

### 完整示例

参考 `examples/add_segment_demo.rs`:

```bash
cargo run --example add_segment_demo
```

输出示例:
```
🎯 演示 add_segment_from_parquet 功能

📝 步骤 1: 创建外部 Parquet 文件
  ✓ 创建外部 Parquet 文件: "/tmp/external_parquet_demo/external_data.parquet"
  ✓ 包含 500 行数据

📝 步骤 2: 创建 Partition
  ✓ Partition 创建完成

📝 步骤 3: 插入正常数据 (通过 upsert)
  ✓ 插入 5 条正常数据

📝 步骤 4: 添加外部 Parquet 文件作为 Segment
📦 Adding segment from Parquet: /tmp/external_parquet_demo/external_data.parquet
  📊 Parquet file contains 500 rows
  📍 Segment range: 5 - 504
  🔨 Building indexes for segment 0...
  ✓ Read 500 rows from Parquet
  🔧 Creating segment from Parquet with 500 rows
    ✓ Built index for field: id
    ✓ Built index for field: name
    ✓ Built index for field: score
  ✓ Segment created
✅ Segment 0 added successfully (500 rows, range: 5-504)

📝 步骤 5: 再插入更多数据
  ✓ 插入 3 条后续数据

📝 步骤 6: 验证 Partition 状态
  ✓ Partition 统计:
    - Segment 数量: 1
    - 总文档数: 503

  ✓ Frozen Segments:
    - Segment 0: 500 文档
      路径: /tmp/external_parquet_demo/external_data.parquet

  ✓ Current Segment:
    - 文档数: 3
    - Start ID: 505

✅ 演示完成!
```

## 性能特点

### 优点

1. **零拷贝**: Parquet 文件不需要复制,直接引用
2. **快速导入**: 只需要构建索引,不需要数据转换
3. **节省空间**: 不重复存储数据
4. **灵活性**: 可以混合使用 upsert 和 add_segment
5. **列式读取**: Parquet 天然支持列式读取,查询高效

### 权衡

1. **索引构建**: 需要读取整个 parquet 文件来构建索引 (一次性开销)
2. **内存占用**: 索引需要保存在内存中
3. **查询延迟**: 读取 RowData 时需要从磁盘加载 parquet
4. **依赖外部文件**: Parquet 文件必须保持可访问

### 性能建议

**适合的场景**:
- ✅ 大量历史数据导入
- ✅ 只读或很少更新的数据
- ✅ 数据已经是 parquet 格式
- ✅ 需要节省存储空间

**不适合的场景**:
- ❌ 频繁更新的数据
- ❌ 需要极低延迟的实时查询
- ❌ Parquet 文件可能移动或删除

## 注意事项

### 1. Parquet 文件要求

- **Schema 匹配**: Parquet 文件的 schema 必须与 Partition 的 schema 兼容
- **文件持久性**: Parquet 文件必须在 segment 生命周期内保持可访问
- **路径稳定**: 文件路径不应该改变

### 2. 文档 ID 分配

文档 ID 会自动分配:
```
Before add_segment:
  current_segment.start = 100
  
After add_segment (500 rows):
  new segment: start=100, end=599
  current_segment.start = 600
```

### 3. 索引一致性

- 索引会在添加时构建
- 如果 parquet 文件在添加后被修改,索引将不一致
- 建议使用只读的 parquet 文件

### 4. 并发安全

`add_segment_from_parquet` 方法是线程安全的:
- 使用 RwLock 保护 frozen_segments
- 自动分配 segment ID
- 原子更新 current_segment

## 与现有功能对比

### vs. upsert

| 特性 | add_segment_from_parquet | upsert |
|------|-------------------------|--------|
| 数据源 | 外部 parquet 文件 | RecordBatch |
| 数据复制 | 否 (引用) | 是 (复制到内存) |
| 索引构建 | 立即 | 立即 |
| 更新支持 | 否 | 是 |
| 适用场景 | 历史数据导入 | 实时写入 |

### vs. load frozen segment

| 特性 | add_segment_from_parquet | load frozen |
|------|-------------------------|-------------|
| 数据来源 | 任意 parquet | 持久化的 segment |
| 索引来源 | 新建 | 从磁盘加载 |
| base_path | parquet 路径 | segment 目录 |
| 用途 | 导入外部数据 | 恢复持久化数据 |

## 未来增强

### 计划中的功能

1. **批量添加**: 一次添加多个 parquet 文件
   ```rust
   partition.add_segments_from_parquet(&[
       "/data/file1.parquet",
       "/data/file2.parquet",
   ])?;
   ```

2. **延迟索引构建**: 按需构建索引
   ```rust
   partition.add_segment_from_parquet_lazy("/data/big.parquet")?;
   ```

3. **Parquet 目录**: 支持添加整个目录
   ```rust
   partition.add_segments_from_dir("/data/parquet_dir")?;
   ```

4. **Schema 映射**: 支持 schema 转换
   ```rust
   partition.add_segment_with_mapping(
       "/data/old_format.parquet",
       field_mapping,
   )?;
   ```

## 总结

`add_segment_from_parquet` 是一个强大的功能,允许你:

✅ **零拷贝导入**: 直接引用外部 parquet 文件
✅ **混合使用**: 与 upsert 数据无缝集成
✅ **高效查询**: 索引加速,parquet 列式读取
✅ **灵活架构**: 适合多种数据集成场景

适用于需要导入大量历史数据、集成数据湖、或构建分层存储系统的场景。

---

**相关文档**:
- [RowDataStore 设计](../src/segment/field_store/row_data.rs)
- [Partition API](../src/partition.rs)
- [Segment API](../src/segment/mod.rs)
- [示例代码](../examples/add_segment_demo.rs)
