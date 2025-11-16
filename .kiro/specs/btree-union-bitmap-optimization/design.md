# B-Tree Union Bitmap 优化设计文档

## 概述

本文档描述如何在 B-Tree 的非叶子节点中存储子树的 bitmap 并集，以优化 range query 性能。

## 架构设计

### 架构层次

```
┌─────────────────────────────────────────────┐
│ field_store 层（上层）                       │
│                                             │
│  InvertedIndex<K>                           │
│    ├─ Disk(TreeReader)   ← 本次优化        │
│    │   └─ Union Bitmap 读取逻辑             │
│    └─ Memory(BTree)      ← 不修改           │
│                                             │
│  持久化逻辑                                  │
│    └─ Union Bitmap 计算和写入  ← 本次优化   │
└─────────────────────────────────────────────┘
                    ↓ 使用接口
┌─────────────────────────────────────────────┐
│ mem_btree 层（底层，通用库）                 │
│                                             │
│  BTree（内存版本）        ← 不修改           │
│  TreeReader（磁盘读取）   ← 扩展接口         │
│  TreeWriter（磁盘写入）   ← 扩展接口         │
└─────────────────────────────────────────────┘
```

**设计原则**：
1. **保持 mem_btree 的通用性**：不在 mem_btree 中添加 bitmap 相关逻辑
2. **只优化磁盘版本**：内存版本不修改，保持简单
3. **在上层实现优化**：union bitmap 的计算和存储在 field_store 层
4. **最小化底层修改**：只在 TreeReader/TreeWriter 中添加必要的扩展点

### 当前架构

```
B-Tree 结构（当前）：
┌─────────────────┐
│  非叶子节点      │
│  keys: [10, 20] │
│  children: [→,→,→]│  ← 只有指向子节点的指针
└─────────────────┘
        ↓
┌─────────────────┐
│  叶子节点        │
│  keys: [1,2,3]  │
│  bitmaps: [b1,b2,b3] │
└─────────────────┘

Range Query 流程：
1. 遍历 B-Tree 找到范围内的所有叶子节点
2. 逐个读取每个 key 的 bitmap
3. 逐个合并：result = b1 | b2 | b3 | ...
4. 返回结果

性能瓶颈：步骤 3 需要 O(n) 次合并操作
```

### 优化后架构

```
B-Tree 结构（优化后）：
┌─────────────────────────┐
│  非叶子节点              │
│  keys: [10, 20]         │
│  children: [→,→,→]      │
│  union_bitmaps: [U0,U1,U2] │  ← 新增：每个子树的并集
└─────────────────────────┘
        ↓
┌─────────────────┐
│  叶子节点        │
│  keys: [1,2,3]  │
│  bitmaps: [b1,b2,b3] │
└─────────────────┘

其中：
- U0 = child0 子树中所有 bitmaps 的并集
- U1 = child1 子树中所有 bitmaps 的并集
- U2 = child2 子树中所有 bitmaps 的并集

Range Query 流程（优化后）：
1. 遍历 B-Tree
2. 如果整个子树都在范围内 → 直接使用 union_bitmap
3. 如果部分在范围内 → 递归处理子树
4. 合并结果
5. 返回结果

性能提升：从 O(n) 降低到 O(log n)
```

## 关键设计决策

### Union Bitmap 存储在哪里？

**方案**：存储在**非叶子节点**中，作为节点的扩展数据

**原因**：
1. **避免修改 mem_btree 核心逻辑**：union bitmap 是上层（field_store）的概念
2. **利用现有的持久化机制**：可以扩展节点格式，添加额外字段
3. **查询时可以快速访问**：读取节点时一并读取 union bitmap

### 如何计算 Union Bitmap？

**时机**：在持久化时（TreeWriter::persist）

**流程**：
```
1. mem_btree 构建完成，准备持久化
2. field_store 层遍历 B-Tree 的所有节点
3. 对每个非叶子节点：
   a. 获取所有子节点的 key 范围
   b. 递归计算子树的 union bitmap
   c. 将 union bitmap 写入 DATA 文件
   d. 在节点中记录 union bitmap 的 offset
4. 持久化节点数据（包含 union_offsets）
```

### 如何查询 Union Bitmap？

**接口设计决策**：在 TreeReader 添加新的优化接口 `range_query_with_union()`

**原因**：
1. **清晰分层**：新接口专门用于优化，不需要考虑向后兼容
2. **代码清晰**：新旧逻辑完全分离，易于理解和维护
3. **上层选择**：field_store 层根据索引版本选择调用哪个方法
4. **简单直接**：不需要内部判断版本，直接使用 union bitmap

**流程**：
```rust
// TreeReader 添加新接口
impl TreeReader<K, RoaringBitmap> {
    // 新增：使用 union bitmap 的优化接口
    pub fn range_query_with_union(
        &self,
        start: Option<&K>,
        start_inclusive: bool,
        end: Option<&K>,
        end_inclusive: bool,
    ) -> RoaringBitmap {
        self.range_query_node_with_union(
            self.root_offset,
            start,
            start_inclusive,
            end,
            end_inclusive
        )
    }
    
    // 内部递归实现
    fn range_query_node_with_union(
        &self,
        node_offset: i64,
        start: Option<&K>,
        start_inclusive: bool,
        end: Option<&K>,
        end_inclusive: bool,
    ) -> RoaringBitmap {
        // 读取节点和 union bitmaps
        // 如果整个子树在 range 内 → 使用 union bitmap
        // 如果部分在 range 内 → 递归查询子树
    }
}

// field_store 层根据版本选择接口
impl<K> InvertedIndex<K> {
    pub(crate) fn range_query(
        &self,
        start: Option<&K>,
        start_inclusive: bool,
        end: Option<&K>,
        end_inclusive: bool,
    ) -> RoaringBitmap {
        match self {
            InvertedIndex::Disk(reader) => {
                // 根据版本选择接口
                if reader.version() >= 1 {
                    reader.range_query_with_union(start, start_inclusive, end, end_inclusive)
                } else {
                    // 使用现有的迭代器方式（向后兼容）
                    // ... 现有代码 ...
                }
            }
            InvertedIndex::Memory(btree) => {
                // 使用现有逻辑（不变）
                // ...
            }
        }
    }
}
```

## 数据结构设计

### 磁盘格式

#### 当前格式（NODE 文件）

```
文件头：
[MAGIC: 2 bytes] [root_offset: 8 bytes] [key_len: 2 bytes] [tree_len: 4 bytes]

节点格式：
[is_leaf: 1 byte] [num_keys: 2 bytes] [keys_data: variable] [offsets: variable]

叶子节点：
- keys_data: 序列化的 keys
- offsets: 指向 DATA 文件中 bitmap 的偏移量

非叶子节点：
- keys_data: 序列化的 keys
- offsets: 指向子节点的偏移量
```

#### 优化后格式（NODE 文件）

```
文件头：
[MAGIC: 2 bytes] [VERSION: 1 byte] [root_offset: 8 bytes] [key_len: 2 bytes] [tree_len: 4 bytes]
                  ↑ 新增版本号

节点格式：
[is_leaf: 1 byte] [num_keys: 2 bytes] [has_union: 1 byte] [keys_data: variable] [offsets: variable] [union_offsets: variable]
                                        ↑ 新增标志位                                                    ↑ 新增 union bitmap 偏移

叶子节点（不变）：
- keys_data: 序列化的 keys
- offsets: 指向 DATA 文件中 bitmap 的偏移量
- has_union: 0（叶子节点不需要 union bitmap）

非叶子节点（新增 union_offsets）：
- keys_data: 序列化的 keys
- offsets: 指向子节点的偏移量
- has_union: 1
- union_offsets: 指向 DATA 文件中 union bitmap 的偏移量（每个子节点一个）
```

#### DATA 文件（不变）

```
[MAGIC: 2 bytes] [bitmaps: variable]

每个 bitmap 的格式：
[size: 4 bytes] [roaring_bitmap_data: variable]
```

### 内存结构

#### TreeReader 结构（需要修改）

```rust
pub struct TreeReader<K, R> {
    key_len: u16,
    tree_len: u32,
    root_offset: i64,
    node: memmap2::Mmap,  // NODE 文件
    data: memmap2::Mmap,  // DATA 文件
    reader_ser: Box<dyn ReadSerializer<K, R>>,
    version: u8,  // 新增：文件版本号
}
```

#### 节点结构（概念上）

```rust
// 非叶子节点（概念）
struct InternalNode {
    keys: Vec<K>,
    child_offsets: Vec<i64>,      // 指向子节点
    union_offsets: Vec<i64>,      // 新增：指向 union bitmaps
}

// 叶子节点（不变）
struct LeafNode {
    keys: Vec<K>,
    value_offsets: Vec<i64>,      // 指向 bitmaps
}
```

## 核心算法

### Range Query 优化算法

**接口层（field_store）**：
```rust
// field_store/mod.rs
impl<K> InvertedIndex<K> {
    pub(crate) fn range_query(
        &self,
        start: Option<&K>,
        start_inclusive: bool,
        end: Option<&K>,
        end_inclusive: bool,
    ) -> RoaringBitmap {
        match self {
            InvertedIndex::Disk(reader) => {
                // 根据版本选择接口
                if reader.version() >= 1 {
                    // 使用优化接口
                    reader.range_query_with_union(start, start_inclusive, end, end_inclusive)
                } else {
                    // 使用现有的迭代器方式（向后兼容）
                    let mut result = RoaringBitmap::new();
                    let mut iter = reader.iter();
                    if let Some(s) = start {
                        iter.seek(s);
                    }
                    for item in iter {
                        let (key, bitmap, _ttl) = &*item;
                        // 检查范围并合并
                        // ... 现有逻辑 ...
                    }
                    result
                }
            }
            InvertedIndex::Memory(btree) => {
                // 使用现有逻辑（不变）
                // ...
            }
        }
    }
}
```

**底层实现（TreeReader 新增接口）**：
```rust
// mem_btree TreeReader 新增接口
impl TreeReader<K, RoaringBitmap> {
    // 新增：获取版本号
    pub fn version(&self) -> u8 {
        self.version
    }
    
    // 新增：使用 union bitmap 的优化接口
    pub fn range_query_with_union(
        &self,
        start: Option<&K>,
        start_inclusive: bool,
        end: Option<&K>,
        end_inclusive: bool,
    ) -> RoaringBitmap {
        self.range_query_node_with_union(
            self.root_offset,
            start,
            start_inclusive,
            end,
            end_inclusive
        )
    }
    
    // 内部递归实现
    fn range_query_node_with_union(
        &self,
        node_offset: i64,
        start: Option<&K>,
        start_inclusive: bool,
        end: Option<&K>,
        end_inclusive: bool,
    ) -> RoaringBitmap {
        let node = self.read_node(node_offset);
        
        if node.is_leaf {
            // 叶子节点：逐个合并
            let mut result = RoaringBitmap::new();
            for (key, bitmap) in node.iter() {
                if self.key_in_range(key, start, start_inclusive, end, end_inclusive) {
                    result |= bitmap;
                }
            }
            return result;
        }
        
        // 非叶子节点：利用 union bitmap 优化
        let mut result = RoaringBitmap::new();
        
        for i in 0..node.children.len() {
            let child_range = self.get_child_key_range(&node, i);
            
            if child_range.completely_inside(start, start_inclusive, end, end_inclusive) {
                // 整个子树都在范围内 → 直接使用 union bitmap
                let union_bitmap = self.read_union_bitmap(node.union_offsets[i]);
                result |= union_bitmap;
            } else if child_range.overlaps(start, start_inclusive, end, end_inclusive) {
                // 部分在范围内 → 递归处理
                let child_result = self.range_query_node_with_union(
                    node.child_offsets[i],
                    start,
                    start_inclusive,
                    end,
                    end_inclusive
                );
                result |= child_result;
            }
            // else: 完全不在范围内 → 跳过
        }
        
        return result;
    }
    
    // 辅助方法：判断 key 是否在范围内
    fn key_in_range(
        &self,
        key: &K,
        start: Option<&K>,
        start_inclusive: bool,
        end: Option<&K>,
        end_inclusive: bool,
    ) -> bool {
        // 检查 start bound
        let start_ok = match start {
            Some(s) => if start_inclusive { key >= s } else { key > s },
            None => true,
        };
        
        // 检查 end bound
        let end_ok = match end {
            Some(e) => if end_inclusive { key <= e } else { key < e },
            None => true,
        };
        
        start_ok && end_ok
    }
    
    // 辅助方法：获取子节点的 key 范围
    fn get_child_key_range(&self, node: &Node, child_index: usize) -> KeyRange<K> {
        // 根据 B-Tree 结构确定子节点的 key 范围
        // ...
    }
    
    // 辅助方法：读取 union bitmap
    fn read_union_bitmap(&self, offset: i64) -> RoaringBitmap {
        // 从 DATA 文件读取 union bitmap
        // ...
    }
}
```

### Union Bitmap 计算算法

```rust
fn compute_union_bitmap(child_node_offset: i64) -> RoaringBitmap {
    let child = read_node(child_node_offset);
    
    if child.is_leaf {
        // 叶子节点：合并所有 bitmaps
        let mut union = RoaringBitmap::new();
        for bitmap in child.bitmaps {
            union |= bitmap;
        }
        return union;
    } else {
        // 非叶子节点：合并所有子节点的 union bitmaps
        let mut union = RoaringBitmap::new();
        for union_offset in child.union_offsets {
            let child_union = read_union_bitmap(union_offset);
            union |= child_union;
        }
        return union;
    }
}
```

### 写入时维护 Union Bitmap

```rust
fn insert_and_update_union(
    node_offset: i64,
    key: K,
    bitmap: RoaringBitmap,
) -> Result<()> {
    // 1. 正常插入 key-bitmap
    let (new_node_offset, affected_path) = insert_internal(node_offset, key, bitmap)?;
    
    // 2. 向上传播，更新所有祖先节点的 union bitmap
    for ancestor_offset in affected_path.iter().rev() {
        update_union_bitmap_for_node(ancestor_offset)?;
    }
    
    Ok(())
}

fn update_union_bitmap_for_node(node_offset: i64) -> Result<()> {
    let node = read_node(node_offset);
    
    if node.is_leaf {
        return Ok(()); // 叶子节点不需要 union bitmap
    }
    
    // 重新计算每个子节点的 union bitmap
    for i in 0..node.children.len() {
        let union = compute_union_bitmap(node.child_offsets[i]);
        write_union_bitmap(node.union_offsets[i], union)?;
    }
    
    Ok(())
}
```

## 实现计划

### Phase 1：写入支持（构建时生成 Union Bitmap）

**目标**：在构建索引时生成 union bitmap

**说明**：Disk 版本的索引是只读的，只在构建时写入一次，没有增量更新

**步骤**：
1. 修改 TreeWriter，支持写入版本号
2. 修改节点写入逻辑，支持写入 union_offsets
3. 在构建完 B-Tree 后，遍历所有非叶子节点
4. 为每个非叶子节点计算并写入 union bitmap
5. 添加测试验证正确性

**风险**：中（需要修改写入逻辑，但是批量操作，一次性完成）

### Phase 2：读取支持（优化 Range Query）

**目标**：支持读取包含 union bitmap 的索引文件，优化 range query

**步骤**：
1. 修改 TreeReader，支持读取版本号
2. 修改节点读取逻辑，支持读取 union_offsets
3. 实现优化的 range_query 算法
4. 添加测试验证正确性

**风险**：低（只读操作，不会破坏数据）

### Phase 3：向后兼容

**目标**：支持读取旧版本索引，提供升级路径

**步骤**：
1. 检测索引版本号
2. 对于旧版本索引，降级到原始算法
3. 提供工具重建索引以获得优化
4. 添加测试验证兼容性

**风险**：低（降级逻辑简单）

### 注意：没有 Phase 4（增量更新）

**原因**：Disk 版本的索引是只读的，构建后不再修改
- 插入新数据 → 创建新的 Segment，构建新的索引
- 删除数据 → 标记删除，不修改索引
- 更新数据 → 标记删除 + 插入新数据

因此不需要增量更新 union bitmap 的逻辑。

## 性能分析

### 时间复杂度

| 操作 | 优化前 | 优化后 | 说明 |
|------|--------|--------|------|
| Range Query (小范围) | O(n) | O(log n) | n = keys 数量 |
| Range Query (大范围) | O(n) | O(log n) | 大范围时优势更明显 |
| 插入 | O(log n) | O(log n × h) | h = 树高度，需要更新祖先 |
| 删除 | O(log n) | O(log n × h) | 同上 |

### 空间复杂度

| 项目 | 大小 | 说明 |
|------|------|------|
| 原始 bitmaps | N × B | N = keys 数量，B = 平均 bitmap 大小 |
| Union bitmaps | M × U | M = 非叶子节点数，U = 平均 union bitmap 大小 |
| 总增加 | ~15-20% | 因为 union bitmap 是压缩的 |

### 实际场景分析

**场景 1：小范围查询**
```
查询：age BETWEEN 25 AND 30
Keys 数量：100
B-Tree 高度：3

优化前：
- 遍历 100 个叶子节点
- 合并 100 个 bitmaps
- 耗时：~10ms

优化后：
- 访问 ~5 个非叶子节点
- 直接获取 union bitmaps
- 耗时：~1ms
- 提升：10x
```

**场景 2：大范围查询**
```
查询：age BETWEEN 18 AND 65
Keys 数量：1000
B-Tree 高度：3

优化前：
- 遍历 1000 个叶子节点
- 合并 1000 个 bitmaps
- 耗时：~100ms

优化后：
- 访问 ~10 个非叶子节点
- 大部分子树直接使用 union bitmap
- 耗时：~10ms
- 提升：10x
```

## 测试策略

### 单元测试

1. **Union Bitmap 计算**
   - 测试叶子节点的 union 计算
   - 测试非叶子节点的 union 计算
   - 测试空节点的处理

2. **Range Query 优化**
   - 测试完全覆盖子树的情况
   - 测试部分覆盖子树的情况
   - 测试不覆盖子树的情况

3. **增量更新**
   - 测试插入后 union bitmap 的更新
   - 测试删除后 union bitmap 的更新
   - 测试节点分裂时的处理

### 集成测试

1. **正确性测试**
   - 对比优化前后的查询结果
   - 测试各种范围查询场景
   - 测试边界条件

2. **性能测试**
   - 测试不同数据量下的性能
   - 测试不同范围大小的性能
   - 对比优化前后的性能差异

3. **兼容性测试**
   - 测试读取旧版本索引
   - 测试新旧版本混合使用
   - 测试索引升级流程

## 风险缓解

### 数据正确性风险

**缓解措施**：
1. 添加校验和验证 union bitmap 的正确性
2. 提供调试工具对比优化前后的结果
3. 在测试环境充分验证后再上线

### 性能退化风险

**缓解措施**：
1. 提供开关可以禁用优化
2. 监控查询性能，发现问题及时回滚
3. 对于小范围查询，可能不使用优化

### 存储开销风险

**缓解措施**：
1. 使用 RoaringBitmap 压缩
2. 监控索引文件大小
3. 如果开销过大，可以选择性地只为某些层级存储 union bitmap

## 总结

本设计通过在 B-Tree 非叶子节点存储 union bitmap，将 range query 的时间复杂度从 O(n) 降低到 O(log n)，预期性能提升 5-10x。实现分为 4 个阶段，逐步增加功能，降低风险。
