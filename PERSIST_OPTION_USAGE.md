# PersistOption 使用指南

## 概述

`PersistOption` 是一个新增的配置结构，用于控制 Keyword 字段的持久化行为。它允许用户自定义压缩级别和 B-tree chunk 大小。

## 结构定义

```rust
pub struct PersistOption {
    /// Zstd 压缩级别 (1-22, 默认 3)
    pub zstd_level: i32,
    /// B-tree chunk 大小 (默认 256)
    pub chunk_size: usize,
}
```

## 使用方式

### 1. 使用默认配置（推荐）

如果不指定 `persist_option`，将自动使用默认值：

```rust
use crate::schema::field::FieldOption;

let field = FieldOption::Keyword {
    name: "my_field".to_string(),
    index: true,
    is_array: false,
    persist_option: None,  // 使用默认配置
};
```

**默认值：**

- `zstd_level`: 3 (平衡压缩率和速度)
- `chunk_size`: 256 (最佳查询性能)

### 2. 自定义配置

#### 方式一：使用 `PersistOption::new()`

```rust
use crate::schema::field::{FieldOption, PersistOption};

let field = FieldOption::Keyword {
    name: "my_field".to_string(),
    index: true,
    is_array: false,
    persist_option: Some(PersistOption::new(
        5,      // zstd_level: 更高的压缩率
        512,    // chunk_size: 更大的 chunk
    )),
};
```

#### 方式二：使用结构体字面量

```rust
use crate::schema::field::{FieldOption, PersistOption};

let field = FieldOption::Keyword {
    name: "my_field".to_string(),
    index: true,
    is_array: false,
    persist_option: Some(PersistOption {
        zstd_level: 1,    // 最快的压缩速度
        chunk_size: 128,  // 最快的查询速度
    }),
};
```

## 参数说明

### zstd_level (压缩级别)

- **范围**: 1-22
- **默认值**: 3
- **影响**:
  - **较低值 (1-3)**: 压缩更快，文件稍大
  - **中等值 (3-9)**: 平衡压缩率和速度 ✅ **推荐**
  - **较高值 (10-22)**: 压缩率更高，但速度明显变慢

**建议**:

- 实时写入场景: `zstd_level = 1-3`
- 批量导入场景: `zstd_level = 5-9`
- 存储优先场景: `zstd_level = 10-15`

### chunk_size (B-tree 块大小)

- **范围**: 建议 128-2048
- **默认值**: 256
- **影响**: 主要影响查询性能，对文件大小几乎无影响

#### 性能对比

| chunk_size | 查询时间 | Node文件 | Data文件 | 说明 |
|-----------|---------|---------|---------|------|
| 128 | ~160μs ⚡ | 2.0MB | 27.3MB | 查询最快，树更深 |
| **256** | **~189μs** | **2.0MB** | **27.3MB** | **最佳平衡** ✅ |
| 512 | ~280μs | 2.0MB | 27.3MB | 查询变慢 |
| 1024 | ~405μs 🐌 | 2.0MB | 27.3MB | 查询很慢 |

**建议**:

- 查询密集型: `chunk_size = 128-256` ✅ **推荐 256**
- 通用场景: `chunk_size = 256-512`
- 扫描场景: `chunk_size = 512-1024`

## 实际应用场景

### 场景 1: 高频查询场景（推荐默认配置）

```rust
// 使用默认配置即可
let field = FieldOption::Keyword {
    name: "user_id".to_string(),
    index: true,
    is_array: false,
    persist_option: None,  // zstd_level=3, chunk_size=256
};
```

### 场景 2: 存储优先场景

适用于数据很少查询，但需要节省存储空间的场景。

```rust
let field = FieldOption::Keyword {
    name: "archive_data".to_string(),
    index: true,
    is_array: false,
    persist_option: Some(PersistOption::new(
        15,     // 高压缩率
        1024,   // 大 chunk (查询性能不重要)
    )),
};
```

### 场景 3: 性能优先场景

适用于需要极致查询性能的热数据。

```rust
let field = FieldOption::Keyword {
    name: "hot_tags".to_string(),
    index: true,
    is_array: false,
    persist_option: Some(PersistOption::new(
        1,      // 最快压缩速度
        128,    // 最快查询速度
    )),
};
```

### 场景 4: 批量导入场景

适用于一次性大量导入数据的场景。

```rust
let field = FieldOption::Keyword {
    name: "bulk_import".to_string(),
    index: true,
    is_array: false,
    persist_option: Some(PersistOption::new(
        7,      // 较高压缩率，导入时间可接受
        256,    // 保持查询性能
    )),
};
```

## 性能测试数据

基于 1000万条记录，10万个唯一 UUID 的测试：

### 写入性能

- **吞吐量**: ~320,000 条/秒
- **不受 `persist_option` 影响**

### 持久化性能

- **时间**: ~3 秒
- **主要受 `zstd_level` 影响**

### 磁盘占用

```
总大小: 29.3 MB
├── Data 文件: 27.3 MB (93.2%) ← 主要受 zstd_level 影响
└── Node 文件: 2.0 MB (6.8%)   ← 几乎不受影响
```

### 查询性能

| 配置 | 内存查询 | 磁盘查询 | 比率 |
|------|---------|---------|------|
| chunk_size=128 | 10μs | 160μs | 16x |
| chunk_size=256 | 10μs | 189μs | 19x |
| chunk_size=512 | 10μs | 280μs | 28x |
| chunk_size=1024 | 10μs | 405μs | 40x |

## 最佳实践

1. ✅ **大多数情况使用默认配置**
   - 默认值经过优化，适合 95% 的场景

2. ✅ **chunk_size 影响查询性能**
   - 建议保持 256 或更小
   - 除非确定需要牺牲查询性能换取其他收益

3. ✅ **zstd_level 影响压缩率和速度**
   - 1-3: 快速写入
   - 3-9: 平衡
   - 10+: 高压缩率

4. ⚠️ **不要过度优化**
   - 先用默认配置
   - 基于实际测试数据调整
   - 避免猜测和过早优化

## API 参考

### PersistOption 方法

```rust
// 创建默认配置
let opt = PersistOption::default();
// 或
let opt = PersistOption::default();

// 创建自定义配置
let opt = PersistOption::new(zstd_level, chunk_size);
```

### FieldOption 便捷方法

```rust
let field = FieldOption::Keyword { /* ... */ };

// 获取完整配置（自动填充默认值）
let opt = field.persist_option();

// 获取单个参数
let level = field.zstd_level();
let size = field.chunk_size();
```

## 总结

- 🎯 **默认配置已优化，直接使用即可**
- 📊 **chunk_size 主要影响查询性能**
- 💾 **zstd_level 主要影响压缩率和持久化速度**
- ⚡ **推荐**: `PersistOption::new(3, 256)` 或使用 `None`
