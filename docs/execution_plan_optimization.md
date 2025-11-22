# 执行计划优化 - WHERE 条件分析与执行提示

## 概述

本次优化借鉴了 Elasticsearch 的执行计划优化思路，在查询分析阶段提前收集 WHERE 条件、SELECT 字段等信息，为执行器提供执行提示，实现更智能的执行策略选择。

## 核心改进

### 1. ExecutionHints 结构

```rust
pub struct ExecutionHints {
    pub has_where: bool,                        // 是否有 WHERE 条件
    pub where_conditions: Vec<WhereCondition>,  // WHERE 条件列表
    pub projection_fields: Vec<String>,         // SELECT 字段列表
    pub is_select_star: bool,                   // 是否 SELECT *
}
```

### 2. WHERE 条件分析

支持的条件类型：
- **Equality**: `field = value`
- **Range**: `field > value`, `field BETWEEN x AND y`
- **In**: `field IN (...)`
- **Like**: `field LIKE pattern`

```rust
pub struct WhereCondition {
    pub field_name: String,
    pub condition_type: ConditionType,
}
```

### 3. 查询类型增强

所有查询类型都增加了 `has_where_filter` 标记：

```rust
// ORDER BY + LIMIT
pub struct SortLimitInfo {
    // ... 原有字段
    pub has_where_filter: bool,  // 🆕 新增
}

// 纯 LIMIT
pub struct PureLimitInfo {
    // ... 原有字段
    pub has_where_filter: bool,  // 🆕 新增
}

// ORDER BY 流式
pub struct SortStreamingInfo {
    // ... 原有字段
    pub has_where_filter: bool,  // 🆕 新增
}
```

## 示例输出

```
📝 SQL: SELECT * FROM users WHERE age BETWEEN 20 AND 30 ORDER BY age LIMIT 10
   描述: WHERE + ORDER BY + LIMIT
   ✅ 表: users
   🔍 查询类型: ParallelSortLimit
   🎯 WHERE 条件: 1 个过滤条件
      - 字段: age, 类型: Range
   📋 SELECT: * (所有字段)
   🔀 执行策略: 并行查询 + TopK 合并
```

## 借鉴 ES 的优化策略

### 已实现 ✅

1. **Early Termination（早停优化）**
   - `SerialLimit`: 串行扫描，收集够数据就停止
   - `execute_serial_limit`: 在分区级别实现早停

2. **Filter 下推**
   - WHERE 条件在 `segment_scanner` 级别转换为 bitmap
   - 多个 filter 做位运算合并

3. **Query Rewrite（部分）**
   - `COUNT(*)` 无 GROUP BY → 直接统计行数
   - `COUNT(*) + 单字段 GROUP BY` → 准备使用倒排索引

### 待实现 🚧

1. **Query Cache（查询缓存）**
   ```rust
   // 缓存 filter 的 bitmap 结果
   struct QueryCache {
       filter_cache: LruCache<String, RoaringBitmap>,
   }
   ```

2. **Multi-Phase Search（多阶段搜索）**
   - Query Phase: 只获取 doc ID 和排序字段
   - Fetch Phase: 只对最终结果获取完整文档
   
   **适用场景**：`SELECT * ORDER BY timestamp LIMIT 100`
   - Phase 1: 只读索引，获取 top 100 的 doc IDs
   - Phase 2: 只读取这 100 条完整文档

3. **Adaptive Execution（自适应执行）**
   ```rust
   // 根据前几个分区的结果动态调整策略
   if first_few_partitions.result_count > target * 2 {
       // 可以提前结束后续分区的查询
   }
   ```

4. **索引覆盖查询优化**
   ```rust
   // 如果 SELECT 的字段都在索引中，不需要读原始数据
   if execution_hints.projection_fields.all_in_index() {
       // 只从索引读取，不读 RowDataStore
   }
   ```

5. **选择性评估（Selectivity Estimation）**
   ```rust
   pub struct ExecutionHints {
       // 🆕 待添加
       pub estimated_selectivity: f64,  // WHERE 过滤后的估算比例 (0-1)
   }
   
   // 根据选择性选择执行策略
   if estimated_selectivity < 0.01 {  // < 1%
       // 高度选择性，使用索引扫描
   } else if estimated_selectivity > 0.5 {  // > 50%
       // 低选择性，全表扫描可能更快
   }
   ```

## 执行策略决策树

```
┌─────────────────────────────────────────────────────────┐
│              analyze_query (计划分析)                     │
│  - 提取 WHERE 条件                                        │
│  - 提取 SELECT 字段                                       │
│  - 分析查询模式                                           │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│           QueryPlan + ExecutionHints                      │
│  - query_type: ParallelSortLimit                          │
│  - has_where: true                                        │
│  - where_conditions: [age: Range]                         │
│  - projection_fields: [*]                                 │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│          execute_sql (扁平化路由)                         │
│  match query_type:                                        │
│    ParallelSortLimit => execute_parallel_sort_limit()    │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│      execute_parallel_sort_limit (执行)                   │
│  - 并行查询所有分区                                        │
│  - 如果 has_where_filter:                                 │
│    → 在 segment_scanner 应用 filter bitmap               │
│    → 过滤后再排序                                          │
│  - TopK 合并所有结果                                       │
└─────────────────────────────────────────────────────────┘
```

## 下一步优化方向

### 优先级 1：Bitmap 传递到执行器
当前 WHERE 的 bitmap 只在 segment_scanner 内部计算，应该：
1. 在 `analyze_query` 阶段预估选择性
2. 将 bitmap 信息传递给执行器
3. 根据选择性调整执行策略

### 优先级 2：索引覆盖查询
```sql
-- 如果只查询索引字段，不需要读原始数据
SELECT timestamp FROM logs ORDER BY timestamp DESC LIMIT 100
```

### 优先级 3：COUNT 优化
```sql
-- COUNT(*) + WHERE 可以只计算 bitmap cardinality
SELECT COUNT(*) FROM users WHERE age > 20
```

### 优先级 4：多阶段查询
```sql
-- Phase 1: 获取 top 100 的 doc IDs (只读索引)
-- Phase 2: 读取这 100 条完整文档 (只读必要数据)
SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100
```

## 总结

通过引入 `ExecutionHints` 和 WHERE 条件分析，我们实现了：

1. ✅ **判断前置**：所有分析在 `analyze_query` 完成
2. ✅ **信息传递**：WHERE 条件、SELECT 字段等信息传递到执行器
3. ✅ **扁平化路由**：执行器根据 QueryType 直接分发
4. ✅ **可扩展架构**：为后续优化（bitmap 传递、选择性评估）打下基础

这为后续实现更智能的执行策略（如自适应执行、索引覆盖查询）提供了坚实的基础。
