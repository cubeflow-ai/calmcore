# Partition 目录迁移指南

## 问题描述

在 partition ID 重构后，Hash 分区的目录命名格式从 `partition-3` 改为 `partition-0000000000000000003`（19位补零）。

旧数据的目录名不匹配，导致重启后找不到 partition。

## 错误信息

```
⚠️  [DistributedExecutor] Failed to execute aggregation on partition 0000000000000000003: 
Partition 0000000000000000003 not found for table 'taxi_trips' not existed.
```

## 解决方案

### 方案 1：运行迁移脚本（推荐）

使用提供的迁移脚本自动重命名所有旧格式的 partition 目录：

```bash
# 迁移默认数据目录
./scripts/migrate_partition_dirs.sh

# 或指定数据目录
./scripts/migrate_partition_dirs.sh /path/to/data
```

**脚本功能：**
- 自动扫描所有表
- 识别旧格式的 partition 目录（如 `partition-3`）
- 重命名为新格式（如 `partition-0000000000000000003`）
- 跳过已经是新格式的目录

**示例输出：**
```
🔄 Migrating partition directories in: ./data

📁 Processing table: taxi_trips
  🔄 Renaming: partition-0 -> partition-0000000000000000000
  ✅ Success
  🔄 Renaming: partition-1 -> partition-0000000000000000001
  ✅ Success
  🔄 Renaming: partition-2 -> partition-0000000000000000002
  ✅ Success
  🔄 Renaming: partition-3 -> partition-0000000000000000003
  ✅ Success

✅ Migration complete!
```

### 方案 2：临时兼容（已实现）

代码已经添加了向后兼容逻辑，可以自动识别旧格式目录：

```rust
// 如果新格式目录不存在，尝试旧格式
if !partition_dir.exists() {
    if let Ok(old_id) = partition_id.parse::<usize>() {
        let old_dir = table_dir.join(format!("partition-{}", old_id));
        if old_dir.exists() {
            log::warn!("⚠️  Found old format partition directory");
            partition_dir = old_dir;
        }
    }
}
```

**注意：** 这只是临时方案，建议尽快运行迁移脚本。

### 方案 3：手动重命名

如果只有少量 partition，可以手动重命名：

```bash
cd data/tables/taxi_trips

# 重命名 partition 目录
mv partition-0 partition-0000000000000000000
mv partition-1 partition-0000000000000000001
mv partition-2 partition-0000000000000000002
mv partition-3 partition-0000000000000000003
```

## 验证迁移

迁移后，检查目录结构：

```bash
ls -la data/tables/taxi_trips/

# 应该看到：
# partition-0000000000000000000/
# partition-0000000000000000001/
# partition-0000000000000000002/
# partition-0000000000000000003/
```

重启服务，查询应该正常工作：

```sql
SELECT COUNT(*) FROM taxi_trips;
```

## 新数据

新创建的表会自动使用新格式，无需迁移。

## 回滚

如果需要回滚到旧版本代码，可以反向重命名：

```bash
cd data/tables/taxi_trips

mv partition-0000000000000000000 partition-0
mv partition-0000000000000000001 partition-1
mv partition-0000000000000000002 partition-2
mv partition-0000000000000000003 partition-3
```

## 常见问题

### Q: 迁移会影响数据吗？
A: 不会。只是重命名目录，数据文件完全不变。

### Q: 需要停止服务吗？
A: 建议停止服务后再迁移，避免并发问题。

### Q: 迁移失败怎么办？
A: 目录重命名是原子操作，失败不会损坏数据。可以手动检查并重试。

### Q: 为什么要用 19 位？
A: 
- 支持最多 10^19 个 partition
- 保证字典序和数值序一致
- 便于文件系统排序和查找

## 相关文档

- [Partition ID 重构文档](./partition_id_refactoring.md)
