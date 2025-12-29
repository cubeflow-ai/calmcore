# MCP 集成优化指南

## 什么是 MCP?

MCP(Model Context Protocol)是一种协议,允许 AI 模型(如 Claude、GPT)理解和使用应用程序的 API。在 Calm 项目中,我们通过 GraphQL schema 的文档注释来优化 MCP 集成。

## 为什么需要 MCP 友好的注释?

1. **AI 辅助开发**: AI 可以通过 GraphQL introspection 读取注释,自动生成正确的查询
2. **自动补全**: IDE 和 GraphQL Playground 可以显示文档提示
3. **降低学习成本**: 开发者无需查阅外部文档即可理解 API
4. **提高代码质量**: 清晰的注释帮助 AI 生成符合最佳实践的代码

## 注释规范

### 1. 类型注释模板

```rust
/// 类型名称
/// 
/// 简短描述(1-2 句话)
/// 
/// # MCP 提示
/// 
/// **关键要点:**
/// - 要点 1
/// - 要点 2
/// 
/// **示例:**
/// ```graphql
/// // 代码示例
/// ```
#[derive(async_graphql::InputObject)]
pub struct TypeName {
    /// 字段描述 - 详细说明
    pub field: Type,
}
```

### 2. 方法注释模板

```rust
/// 方法名称
/// 
/// 方法功能描述
/// 
/// # MCP 提示
/// 
/// **参数:** 参数说明
/// **返回:** 返回值说明
/// 
/// **用途:**
/// - 用途 1
/// - 用途 2
/// 
/// **示例:**
/// ```graphql
/// query {
///   method(arg: "value") {
///     field
///   }
/// }
/// ```
async fn method(&self, ctx: &Context<'_>, arg: Type) -> Result<ReturnType> {
```

## 已优化的 GraphQL 类型

### 输入类型(InputObject)

✅ **CreateTableInput** - 表创建输入
- 包含快速开始和高级配置模板
- 字段级别的详细说明
- 分区策略选择指南

✅ **FieldInput** - 字段定义输入
- 5 种常见字段模板(ID, 文本, 数组, 整数, 时间戳)
- 配置选项说明(indexed, nullable, default_value)

✅ **PartitionStrategyInput** - 分区策略输入
- 3 种策略对比(Hash, Range, None)
- 完整代码示例
- 分区数推荐

✅ **PersistPolicyInput** - 持久化策略输入
- 默认值说明
- 场景推荐(高吞吐、实时、小数据)

✅ **PartitionValueInput** - 分区值输入
- 三选一填写指南
- 时间戳和字符串示例

✅ **RangePartitionInput** - 范围分区输入
- 区间规则说明
- 按月分区示例

### 枚举类型(Enum)

✅ **FieldTypeEnum** - 字段类型枚举
- 14 种类型的详细说明
- 值范围和适用场景
- 使用示例

✅ **PartitionStrategyType** - 分区策略类型
- 4 种策略的选择指南
- 完整代码示例

### 输出类型(SimpleObject)

✅ **Table** - 表元数据
- 查询示例
- 字段说明

✅ **Field** - 字段元数据
- 类型说明

✅ **InsertResult** - 插入结果
- 字段含义
- 使用示例

✅ **QueryResult** - 查询结果
- 结果结构说明
- 查询示例

✅ **PartitionInfo** - 分区信息
- 诊断用途说明
- 使用场景

✅ **SegmentInfo** - 段信息
- 字段详细说明
- 计算公式(实际文档数)

✅ **TableDetail** - 表详细信息
- 完整查询示例
- 使用场景

✅ **LoadSegmentResult** - 加载结果
- 字段说明
- 使用示例

### Query 方法

✅ **tables()** - 列出所有表
✅ **table(name)** - 获取表信息
✅ **partitions(table)** - 获取分区信息
✅ **tableDetail(name)** - 获取表详情
✅ **query(sql)** - 执行 SQL 查询

### Mutation 方法

✅ **createTable(input)** - 创建表
✅ **dropTable(name)** - 删除表
✅ **flushTable(name)** - 持久化表
✅ **insertData(input)** - 插入数据
✅ **loadSegment(input)** - 加载段数据

## MCP 使用场景

### 场景 1: AI 生成 createTable 查询

**用户提示:**
```
创建一个用户表,包含 ID、用户名、邮箱、年龄字段
```

**AI 推理过程:**
1. 读取 `CreateTableInput` 注释,了解参数结构
2. 读取 `FieldInput` 注释,了解字段配置
3. 参考"快速开始模板"生成查询
4. 根据字段类型选择 `FieldTypeEnum`

**生成的查询:**
```graphql
mutation {
  createTable(input: {
    name: "users"
    description: "用户信息表"
    primary_key: "user_id"
    partition_count: 4
    
    fields: [
      { name: "user_id", field_type: U64, nullable: false }
      { name: "username", field_type: KEYWORD, case_sensitive: false, nullable: false }
      { name: "email", field_type: KEYWORD, nullable: true }
      { name: "age", field_type: I8, default_value: "18", nullable: true }
    ]
  }) {
    name
    partition_count
  }
}
```

### 场景 2: AI 推荐分区策略

**用户提示:**
```
这是一个日志表,每天几百万条记录,按时间查询
```

**AI 推理过程:**
1. 读取 `PartitionStrategyInput` 注释
2. 识别时序数据特征
3. 推荐 Range 分区
4. 参考 Range 分区示例生成配置

**推荐配置:**
```graphql
partition_strategy: {
  strategy_type: RANGE
  field: "timestamp"
  ranges: [
    { partition_id: 0, start: {int_value: 1704067200000}, end: {int_value: 1706745600000} }
    { partition_id: 1, start: {int_value: 1706745600000}, end: {int_value: 1709251200000} }
  ]
}
```

### 场景 3: AI 调整持久化策略

**用户提示:**
```
这是一个实时交易表,需要低延迟查询
```

**AI 推理过程:**
1. 读取 `PersistPolicyInput` 注释
2. 识别"实时查询"场景
3. 参考场景建议生成配置

**推荐配置:**
```graphql
persist_policy: {
  max_docs_per_segment: 50000    # 5 万文档持久化
  max_segment_age_secs: 60       # 1 分钟持久化
}
```

## 如何验证 MCP 集成效果

### 1. GraphQL Playground 测试

访问 `http://localhost:5002/graphql`:

1. 点击右侧 "Docs" 面板
2. 查看类型定义
3. 验证注释是否显示
4. 测试自动补全

### 2. GraphQL Introspection 查询

```graphql
query {
  __type(name: "CreateTableInput") {
    description
    inputFields {
      name
      description
      type {
        name
      }
    }
  }
}
```

### 3. AI 助手测试

在支持 GraphQL introspection 的 AI 助手(如 Claude、Cursor)中:

1. 提供 GraphQL endpoint
2. 让 AI 生成 createTable 查询
3. 检查生成的代码是否符合最佳实践
4. 验证字段类型和配置是否正确

## 持续改进

### 当前优化覆盖率

- ✅ 所有输入类型(6 个)
- ✅ 所有枚举类型(2 个)
- ✅ 所有输出类型(8 个)
- ✅ 所有 Query 方法(5 个)
- ✅ 所有 Mutation 方法(5 个)

**总计:** 26 个类型/方法,100% 覆盖

### 未来优化方向

1. **更多代码示例**: 为每个字段类型添加更多使用场景
2. **错误处理指南**: 常见错误和解决方案
3. **性能调优提示**: 基于数据量的配置建议
4. **集成测试**: 自动化验证注释质量

## 最佳实践总结

### DO ✅

1. **使用"# MCP 提示"标记**: 让 AI 快速识别关键信息
2. **提供完整代码示例**: 包含所有必要字段
3. **说明使用场景**: 帮助 AI 选择正确的配置
4. **包含默认值**: 明确可选参数的默认行为
5. **字段级注释**: 解释每个字段的用途和范围

### DON'T ❌

1. **避免模糊描述**: "字段配置"太笼统,应说明具体作用
2. **避免过长注释**: 保持简洁,关键信息优先
3. **避免技术术语**: 使用通俗易懂的语言
4. **避免重复内容**: 引用其他类型而非复制粘贴
5. **避免过时示例**: 及时更新代码示例

## 文档结构

```
docs/
├── graphql_createtable_guide.md    # 本文档
├── graphql_ddl_complete_example.md # 完整示例
├── DDL_DESIGN_ANALYSIS.md          # 设计分析
└── mcp_integration.md              # MCP 集成指南(本文件)

src/protocol/graphql/mod.rs         # GraphQL schema 定义(包含注释)
```

## 相关资源

- [GraphQL 官方文档](https://graphql.org/)
- [async-graphql 文档](https://async-graphql.github.io/async-graphql/)
- [Model Context Protocol](https://modelcontextprotocol.io/)

---

**最后更新:** 2024
**覆盖率:** 100% (26/26 类型/方法)
