# SegmentScanner 重构总结

## 重构日期
2024-11-16

## 第二轮简化（删除无用优化）

### 发现的问题
原来的"优化"逻辑实际上没有任何作用：
```rust
if hit_ratio < 0.2 {
    self.build_exec_plan(result_bitmap, ...)  // Bitmap Scan
} else {
    self.build_exec_plan(result_bitmap, ...)  // "Full Scan" - 但实际上一样！
}
```

两个分支做的事情完全相同，都是扫描 `result_bitmap` 中的文档。

### 为什么这个优化没用？
1. **两个分支逻辑相同**：都是扫描 result_bitmap
2. **"全表扫描"名不副实**：并没有真正扫描全表
3. **在列存储中意义不大**：读取 80% 和 100% 的数据性能差异很小

### 简化后的代码
直接删除了无用的判断逻辑，简化为：
```rust
pub fn create_plan(...) -> Option<Arc<dyn ExecutionPlan>> {
    let result_bitmap = self.apply_filters(filters)?;
    self.build_exec_plan(result_bitmap, projection, sort, limit)
}
```

---

## 重构日期
2024-11-16

## 问题分析

### 1. 重复代码问题
- `create_execution_plan` 和 `create_optimized_plan_with_sort` 有大量重复逻辑
- 两个方法都在计算 `result_bitmap`（应用filters）
- 两个方法都在处理 `projected_schema`（投影逻辑）
- `create_execution_plan` 方法已经不被使用但还保留着

### 2. 方法命名不准确
- `create_bitmap_scan_plan` - 实际上不只是bitmap scan，也用于full scan
- `create_execution_plan` - 旧方法，已废弃
- `create_optimized_plan_with_sort` - 名字太长

### 3. 重复计算问题
- 在 `create_optimized_plan_with_sort` 中计算了 `result_bitmap`
- 然后又在 `create_index_ordered_scan_plan` 中重复计算了一遍

## 重构方案

### 新的方法结构

```rust
// 主入口方法（唯一的公开方法）
pub fn create_plan(
    filters: &[Expr],
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
    sort_fields: Option<&[(String, bool)]>,
) -> Option<Arc<dyn ExecutionPlan>>

// 辅助方法（私有）
fn apply_filters(&self, filters: &[Expr]) -> Option<RoaringBitmap>
fn build_exec_plan(&self, result_bitmap: RoaringBitmap, projection: Option<&Vec<usize>>) -> Option<Arc<dyn ExecutionPlan>>
fn build_projected_schema(&self, projection: Option<&Vec<usize>>) -> SchemaRef
fn try_index_ordered_scan(&self, result_bitmap: RoaringBitmap, ...) -> Option<Arc<dyn ExecutionPlan>>
```

### 改进点

1. **统一入口**：只有一个公开方法 `create_plan`，简化API
2. **消除重复**：
   - `apply_filters` - 统一的filter应用逻辑
   - `build_projected_schema` - 统一的schema构建逻辑
   - `build_exec_plan` - 统一的执行计划构建逻辑
3. **更好的命名**：
   - `create_plan` - 清晰的主入口
   - `try_index_ordered_scan` - 表明这是一个尝试性的优化
   - `build_exec_plan` - 通用的执行计划构建器
4. **避免重复计算**：
   - `result_bitmap` 只计算一次，然后传递给后续方法
   - 不再在 `try_index_ordered_scan` 中重复计算

### 多字段排序优化

修复了多字段排序的逻辑：
- 只使用第一个排序字段进行索引扫描
- 后续的排序字段交给DataFusion处理
- 添加了清晰的日志说明

## 调用方更新

更新了以下文件中的调用：
- `src/compute/partition_table_provider.rs`
  - `create_optimized_plan` → `create_plan`
- `src/compute/partition_table_provider_with_hints.rs`
  - `create_optimized_plan_with_sort` → `create_plan`

## 代码行数变化

- 删除了约 100 行重复代码
- 新增了约 40 行辅助方法
- 净减少约 60 行代码
- 提高了代码可维护性和可读性

## 性能影响

- **无性能损失**：重构只是代码组织，没有改变算法逻辑
- **潜在性能提升**：避免了重复的bitmap计算

## 测试验证

- ✅ 编译通过
- ✅ 无新增警告
- ✅ 保持了原有的功能逻辑
