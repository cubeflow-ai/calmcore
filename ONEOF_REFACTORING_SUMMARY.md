# OneofObject 分区策略重构总结

## 改造成果

### ✅ 问题解决

**改造前的问题：**
1. `PartitionStrategyInput` 有 9+ 个可选字段
2. 用户不清楚哪些字段是必填的
3. 运行时才能发现配置错误
4. 文档复杂，学习曲线陡峭

**改造后的优势：**
1. ✨ **类型安全**：GraphQL 编译时就能验证
2. 🎯 **清晰明确**：每种策略的必填字段一目了然
3. 🚀 **用户友好**：只能选一种策略，不会混淆
4. 📝 **自文档化**：IDE 自动补全，准确提示

---

## API 对比

### 旧版 API（复杂，容易出错）

```graphql
mutation {
  createTable(input: {
    name: "events"
    partitionStrategy: {
      strategyType: DATETIME_RANGE    # ❓ 还需要填什么？
      field: "timestamp"               # ❓ 这个是必填的吗？
      granularity: DAY                 # ❓ 需要 numPartitions 吗？
      timezone: "UTC"                  # ❓ 需要 start/step 吗？
      parallelism: 4
      # ❓ numPartitions? range_start? range_step? 太多可选字段！
    }
    fields: [...]
  })
}
```

**问题：**
- 9 个可选字段，用户不知道该填哪些
- 错误在运行时才能发现
- 文档需要详细说明每个字段的使用场景

---

### 新版 API（清晰，类型安全）

#### 1. PKHash 分区（最简单）
```graphql
mutation {
  createTable(input: {
    name: "users"
    primaryKey: "user_id"
    partitionStrategy: {
      pkHash: {                # ✅ 只有这个配置块！
        numPartitions: 4       # ✅ 唯一必填字段
      }
    }
    fields: [...]
  })
}
```

#### 2. Hash 分区（无主键表）
```graphql
mutation {
  createTable(input: {
    name: "events"
    partitionStrategy: {
      hash: {                  # ✅ 明确选择 hash 策略
        field: "user_id"       # ✅ 必填：分区字段
        numPartitions: 8       # ✅ 必填：分区数
      }
    }
    fields: [...]
  })
}
```

#### 3. DatetimeRange 分区（时序数据）
```graphql
mutation {
  createTable(input: {
    name: "logs"
    partitionStrategy: {
      datetimeRange: {         # ✅ 明确选择时序分区
        field: "timestamp"     # ✅ 必填：时间字段
        granularity: DAY       # ✅ 必填：时间粒度
        timezone: "UTC"        # ⚙️ 可选，有默认值
        parallelism: 2         # ⚙️ 可选，有默认值
      }
    }
    fields: [...]
  })
}
```

#### 4. Range 分区（手动范围）
```graphql
mutation {
  createTable(input: {
    name: "orders"
    partitionStrategy: {
      range: {                 # ✅ 明确选择范围分区
        field: "created_at"    # ✅ 必填：分区字段
        start: 1704067200000   # ✅ 必填：起始值
        step: 86400000         # ✅ 必填：步长
        numPartitions: 365     # ✅ 必填：分区数
        parallelism: 1         # ⚙️ 可选，有默认值
      }
    }
    fields: [...]
  })
}
```

#### 5. None 分区（小表）
```graphql
mutation {
  createTable(input: {
    name: "config"
    partitionStrategy: {
      none: {                  # ✅ 明确选择无分区
        enabled: true          # ⚙️ 可选，有默认值
      }
    }
    fields: [...]
  })
}
```

---

## 技术实现

### OneofObject 结构

```rust
// 1. 为每种策略创建独立的配置结构
#[derive(async_graphql::InputObject)]
pub struct PKHashPartitionConfig {
    pub num_partitions: u64,  // 必填
}

#[derive(async_graphql::InputObject)]
pub struct DatetimeRangePartitionConfig {
    pub field: String,                    // 必填
    pub granularity: TimeGranularityType, // 必填
    #[graphql(default = "UTC")]
    pub timezone: String,                 // 可选，有默认值
    #[graphql(default = 1)]
    pub parallelism: u64,                 // 可选，有默认值
}

// 2. 使用 OneofObject 枚举
#[derive(async_graphql::OneofObject)]
pub enum PartitionStrategyInput {
    PkHash(PKHashPartitionConfig),
    Hash(HashPartitionConfig),
    Range(RangePartitionConfig),
    DatetimeRange(DatetimeRangePartitionConfig),
    Custom(CustomPartitionConfig),
    None(NonePartitionConfig),
}
```

### 处理逻辑简化

**改造前**（需要大量 Option 处理和错误检查）：
```rust
match strategy_input.strategy_type {
    PartitionStrategyType::Hash => {
        let field = strategy_input.field.ok_or_else(|| 
            Error::new("Hash requires 'field'"))?;
        let num_partitions = strategy_input.num_partitions.ok_or_else(||
            Error::new("Hash requires 'num_partitions'"))?;
        // ... 更多错误检查
    }
}
```

**改造后**（直接解构，编译器保证字段存在）：
```rust
match strategy_input {
    PartitionStrategyInput::Hash(config) => {
        // ✅ config.field 和 config.num_partitions 保证存在
        PartitionStrategy::Hash {
            field: config.field,
            num_partitions: config.num_partitions as usize,
        }
    }
}
```

---

## 测试结果

✅ **所有 5 种分区策略测试通过：**

```bash
=== 测试 1: PKHash 分区 ===
✓ 创建成功，4 个分区

=== 测试 2: Hash 分区 ===
✓ 创建成功，8 个分区

=== 测试 3: DatetimeRange 分区 ===
✓ 创建成功，按需创建分区

=== 测试 4: Range 分区 ===
✓ 创建成功，按需创建分区

=== 测试 5: None 分区 ===
✓ 创建成功，1 个分区
```

---

## 用户体验提升

### 1. IDE 自动补全
当用户输入 `partitionStrategy: { d` 时，IDE 会提示：
- `datetimeRange`
- （不会提示其他不相关的选项）

当用户选择 `datetimeRange` 后，IDE 会准确提示：
- `field` (必填)
- `granularity` (必填)
- `timezone` (可选，有默认值)
- `parallelism` (可选，有默认值)

### 2. 编译时错误检查
```graphql
# ❌ 错误：缺少必填字段
partitionStrategy: {
  hash: {
    field: "user_id"
    # 缺少 numPartitions，GraphQL 编译时就会报错
  }
}

# ❌ 错误：不能同时指定多个策略
partitionStrategy: {
  hash: { ... }
  range: { ... }  # GraphQL 会拒绝这个
}
```

### 3. 文档自文档化
每个配置结构都有清晰的文档注释，说明：
- 适用场景
- 必填/可选字段
- 默认值
- 完整示例

---

## 迁移指南

旧代码需要简单调整字段嵌套：

**旧版：**
```graphql
partitionStrategy: {
  strategyType: HASH
  field: "user_id"
  numPartitions: 8
}
```

**新版：**
```graphql
partitionStrategy: {
  hash: {
    field: "user_id"
    numPartitions: 8
  }
}
```

---

## 总结

✨ **核心改进：**
1. 从"一个大对象 + 9 个可选字段"变成"6 个独立的类型安全配置"
2. 从"运行时错误检查"变成"编译时类型保证"
3. 从"复杂文档说明"变成"自文档化 API"
4. 保留所有功能，没有任何功能削减

🎯 **用户价值：**
- 新手：更容易上手，不会配置错误
- 老手：更快速准确，IDE 自动补全
- 团队：代码更易维护，错误更早发现

这正是 GraphQL OneOf 模式的最佳实践应用！🚀
