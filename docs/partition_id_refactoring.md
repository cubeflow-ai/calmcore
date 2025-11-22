# Partition ID 重构文档

## 概述

将 partition ID 从 `usize` 类型改为 `String` 类型，并统一所有 partition 目录命名逻辑。

## 核心改动

### 1. 统一的 Partition ID 生成

在 `src/catalog/table_meta.rs` 中的 `PartitionStrategy` 提供了统一的方法：

```rust
// 生成 partition ID（字符串格式）
pub fn generate_partition_id(
    &self,
    table_name: &str,
    partition_index: usize,
    partition_value: Option<&str>,
) -> String

// 生成 partition 目录名（统一格式：partition-{id}）
pub fn generate_partition_dir_name(partition_id: &str) -> String

// 从目录名提取 partition ID
pub fn extract_partition_id_from_dir_name(dir_name: &str) -> Option<String>
```

### 2. Hash 分区的 ID 格式

Hash 分区使用 **19 位补零格式**：

```rust
PartitionStrategy::Hash { .. } => {
    format!("{:019}", partition_index)  // 例如：0000000000000000001
}
```

**为什么是 19 位？**
- 支持最多 10^19 个分区
- 保证字典序和数值序一致
- 便于文件系统排序和查找

### 3. 目录命名统一

所有 partition 目录统一使用格式：`partition-{id}`

**示例：**
- Hash 分区：`partition-0000000000000000001`
- Range 分区：`partition-100_200`
- List 分区：`partition-us-west`
- Custom 分区：`partition-custom_value`

## 已更新的文件

### 核心文件
- ✅ `src/catalog/table_meta.rs` - 添加统一方法
- ✅ `src/engine.rs` - 使用统一方法构建目录
- ✅ `src/segment_loader.rs` - 使用统一方法

### 待迁移的文件（使用了 deprecated API）
- ⚠️ `src/catalog/mod.rs` - 4 处使用 `partition_dir(usize)`
- ⚠️ `src/catalog/table_meta.rs` - 1 处使用 `partition_dir(usize)`

## 迁移指南

### 旧代码
```rust
// 旧方式：直接拼接
let dir = format!("partition-{}", partition_id);

// 旧方式：使用 usize
let partition_dir = table_meta.partition_dir(&work_dir, partition_id);
```

### 新代码
```rust
// 新方式：使用统一方法
let dir = PartitionStrategy::generate_partition_dir_name(&partition_id);

// 新方式：使用字符串 ID
let partition_dir = table_meta.partition_dir_by_id(&work_dir, &partition_id);

// 提取 ID
if let Some(id) = PartitionStrategy::extract_partition_id_from_dir_name("partition-0000000000000000001") {
    println!("Partition ID: {}", id);  // "0000000000000000001"
}
```

## 优势

1. **类型安全**：String 类型更灵活，支持各种分区策略
2. **统一管理**：所有目录命名逻辑集中在一处
3. **易于维护**：修改格式只需改一个地方
4. **向后兼容**：旧 API 标记为 deprecated，逐步迁移

## 注意事项

1. **文件系统兼容性**：19 位数字确保在所有文件系统上正常工作
2. **性能影响**：String 比 usize 略慢，但可忽略不计
3. **序列化**：JSON 中 partition_id 现在是字符串类型

## 测试建议

```rust
#[test]
fn test_partition_id_format() {
    let strategy = PartitionStrategy::Hash { 
        field: "id".to_string(), 
        num_partitions: 4 
    };
    
    let id = strategy.generate_partition_id("test_table", 1, None);
    assert_eq!(id, "0000000000000000001");
    assert_eq!(id.len(), 19);
    
    let dir_name = PartitionStrategy::generate_partition_dir_name(&id);
    assert_eq!(dir_name, "partition-0000000000000000001");
    
    let extracted = PartitionStrategy::extract_partition_id_from_dir_name(&dir_name);
    assert_eq!(extracted, Some("0000000000000000001".to_string()));
}
```

## 相关 Issue

- 时间戳显示问题（已解决）
- Partition ID 统一管理（本次重构）
