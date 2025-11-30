# Calm 建表语句设计分析与完善方案

## 当前状态分析

### 1. 三种协议的建表支持情况

#### ✅ GraphQL (完善度: 90%)
**支持的特性:**
- ✅ 表名、主键定义
- ✅ 丰富的字段类型 (14种: I8/I16/I32/I64/U8/U16/U32/U64/F32/F64/Boolean/Keyword/Timestamp)
- ✅ 字段索引控制
- ✅ 分区策略 (Hash/Range/Custom/None)
- ✅ 持久化策略配置 (max_docs_per_segment, max_segment_age)
- ✅ 是否存储原始数据 (store_source)
- ✅ Keyword 字段的 case_sensitive 配置
- ✅ Timestamp 字段的 format 配置

**缺失的特性:**
- ❌ 字段注释/描述
- ❌ 表注释/描述
- ❌ 默认值设置
- ❌ 约束条件 (NOT NULL, UNIQUE)
- ❌ 数组字段的深度配置

**示例:**
```graphql
mutation {
  createTable(input: {
    name: "taxi_trips"
    primary_key: "id"
    partition_strategy: {
      strategy_type: HASH
      field: "id"
      num_partitions: 4
    }
    fields: [
      { name: "id", field_type: U64, indexed: true }
      { name: "vendor_id", field_type: I32, indexed: true }
      { name: "pickup_datetime", field_type: TIMESTAMP, indexed: true, format: "iso8601" }
      { name: "passenger_count", field_type: I8, indexed: false }
    ]
    store_source: true
    persist_policy: {
      max_docs_per_segment: 100000
      max_segment_age_secs: 300
    }
  }) {
    name
    field_count
  }
}
```

#### ⚠️ Elasticsearch (完善度: 60%)
**支持的特性:**
- ✅ 索引名称
- ✅ Mappings 定义
- ✅ 基本字段类型 (text/keyword/long/integer/float/double/boolean/date)
- ✅ 字段索引控制 (index: true/false)
- ✅ 自动使用 _id 作为主键

**缺失的特性:**
- ❌ 分区策略配置 (目前硬编码为 Hash, 1 个分区)
- ❌ 持久化策略配置
- ❌ 丰富的数值类型 (I8/I16/U8/U16/U32/U64/F32)
- ❌ Keyword 的 case_sensitive 配置
- ❌ Timestamp 的 format 配置
- ❌ 数组字段支持
- ❌ Settings 配置 (number_of_shards, number_of_replicas)
- ❌ Analyzer 配置

**示例:**
```bash
PUT /taxi_trips
{
  "mappings": {
    "properties": {
      "vendor_id": { "type": "long", "index": true },
      "pickup_datetime": { "type": "date" },
      "passenger_count": { "type": "integer" }
    }
  }
}
```

#### ⚠️ MySQL (完善度: 50%)
**支持的特性:**
- ✅ 表名定义
- ✅ 基本字段类型 (INT/BIGINT/FLOAT/DOUBLE/BOOLEAN/TIMESTAMP/VARCHAR)
- ✅ PRIMARY KEY 定义
- ✅ 自动索引所有字段

**缺失的特性:**
- ❌ 分区策略配置 (目前硬编码为 Hash, 1 个分区)
- ❌ 持久化策略配置
- ❌ 字段索引控制 (目前全部索引)
- ❌ 表注释 (COMMENT)
- ❌ 字符集/排序规则 (CHARSET/COLLATE)
- ❌ 表选项 (ENGINE, AUTO_INCREMENT)
- ❌ 外键约束
- ❌ 检查约束 (CHECK)
- ❌ 默认值 (DEFAULT)
- ❌ NOT NULL 约束

**示例:**
```sql
CREATE TABLE taxi_trips (
    id BIGINT UNSIGNED PRIMARY KEY,
    vendor_id INT,
    pickup_datetime TIMESTAMP,
    passenger_count TINYINT
);
```

---

## 问题分析

### 1. Calm 的独特特性

Calm 与标准 SQL/ES 的关键区别:

#### 🎯 分区系统 (Partition Strategy)
- **Hash 分区**: 基于字段哈希分配，适合均衡负载
- **Range 分区**: 基于范围划分，适合时序数据
- **Custom 分区**: 用户通过文件加载自定义分区
- **None 分区**: 单分区模式

这是 Calm 特有的，标准 SQL/ES 没有对应概念。

#### 📦 段管理 (Segment Management)
- **持久化策略**: max_docs_per_segment, max_segment_age
- **段冻结**: Active → Frozen → Persisted
- **文档 ID 生成**: 自动分配 doc_id

这是 Calm 的核心存储机制，不同于标准数据库。

#### 🔍 索引策略
- 所有字段都可独立控制是否索引
- 使用 mem_btree 作为索引结构
- 支持精确查询、范围查询、IN 查询

#### 📄 原始数据存储
- `store_source`: 是否保存原始 JSON
- 类似 ES 的 `_source` 但更灵活

### 2. 用户使用场景

根据目前的实现，用户的典型使用场景:

1. **数据科学家/分析师**:
   - 通过 GraphQL 创建表 (Web UI)
   - 使用 SQL 查询数据 (熟悉的语法)
   - 通过 Python 加载 Parquet/CSV

2. **应用开发者**:
   - 通过 ES API 创建索引 (兼容现有工具)
   - 使用 ES API 插入/查询数据
   - 集成到现有 ES 生态

3. **DBA/运维人员**:
   - 通过 MySQL 客户端管理
   - 使用 SQL 查看表结构
   - 执行批量操作

---

## 建议方案

### 方案 A: 保持现状 + 文档增强 (推荐)

**核心思想**: SQL/ES 不支持建表，只用于查询；GraphQL 是唯一的 DDL 入口。

#### 优点:
✅ 符合 Calm 的设计理念 (GraphQL 为管理接口)
✅ 避免 SQL/ES DDL 的复杂性和兼容性问题
✅ 用户可通过 GraphQL Playground 轻松建表
✅ 程序化建表使用 GraphQL API

#### 实施步骤:

1. **明确文档说明**:
```markdown
## Calm DDL 设计

Calm 使用 GraphQL 作为唯一的 DDL (数据定义语言) 接口。

### 为什么不支持 SQL CREATE TABLE?

1. **分区策略复杂**: Calm 的分区系统 (Hash/Range/Custom) 无法映射到标准 SQL
2. **段管理配置**: 持久化策略、段大小等是 Calm 特有的
3. **避免兼容性陷阱**: SQL DDL 语法差异大，难以完全兼容

### 为什么不支持 ES PUT /<index>?

1. **配置更丰富**: Calm 需要配置分区、持久化策略等
2. **类型系统不同**: Calm 支持更多数值类型 (I8/U8/U16/U32/U64)
3. **避免误导**: ES 的 Settings (shards/replicas) 在 Calm 中无意义

### 推荐工作流:

1. **建表**: 使用 GraphQL
2. **查询**: 使用 SQL/ES/GraphQL (任选)
3. **管理**: 使用 GraphQL (DROP TABLE, SHOW TABLES)
```

2. **增强 GraphQL DDL**:

添加缺失的特性:
```rust
pub struct CreateTableInput {
    pub name: String,
    pub primary_key: Option<String>,
    pub description: Option<String>,  // ✨ 新增: 表描述
    pub partition_strategy: Option<PartitionStrategyInput>,
    pub partition_count: Option<u64>,
    pub fields: Vec<FieldInput>,
    pub store_source: Option<bool>,
    pub persist_policy: Option<PersistPolicyInput>,
    pub default_index: Option<bool>,  // ✨ 新增: 默认是否索引字段
}

pub struct FieldInput {
    pub name: String,
    pub field_type: FieldTypeEnum,
    pub indexed: Option<bool>,
    pub description: Option<String>,     // ✨ 新增: 字段描述
    pub default_value: Option<String>,   // ✨ 新增: 默认值
    pub nullable: Option<bool>,          // ✨ 新增: 是否可空
    
    // Keyword 特定配置
    pub case_sensitive: Option<bool>,
    pub is_array: Option<bool>,          // ✨ 新增: 是否数组
    
    // Timestamp 特定配置
    pub format: Option<String>,
}
```

3. **SQL/ES 明确报错**:

```rust
// MySQL
if query_lower.starts_with("create table") {
    return results.error(
        ErrorKind::ER_NOT_SUPPORTED_YET,
        b"CREATE TABLE is not supported in SQL. Please use GraphQL API:
        
        mutation {
          createTable(input: {
            name: \"your_table\"
            fields: [...]
          }) { name }
        }
        
        Visit http://localhost:9567/playground for interactive interface."
    );
}

// ES
async fn create_index(...) -> Result<...> {
    return Err(poem::Error::from_string(
        "PUT /<index> is not supported. Please use GraphQL API:
        
        mutation { createTable(input: {...}) { name } }
        
        For compatibility, use POST /<index>/_doc to insert data directly.
        The index will be created automatically with default settings.",
        StatusCode::METHOD_NOT_ALLOWED
    ));
}
```

### 方案 B: 支持简化 SQL DDL (次选)

如果必须支持 SQL 建表，提供**极简版本**:

```sql
-- 简化语法，只支持基本功能
CREATE TABLE taxi_trips (
    id BIGINT PRIMARY KEY,
    vendor_id INT,
    pickup_datetime TIMESTAMP
) WITH (
    -- Calm 扩展选项
    PARTITIONS = 4,              -- 分区数
    PARTITION_FIELD = 'id',      -- 分区字段
    PARTITION_STRATEGY = 'HASH'  -- 分区策略
);
```

**实现要点**:
```rust
async fn handle_create_table(
    engine: &Arc<Engine>,
    query: &str,
) -> Result<...> {
    // 1. 解析 WITH 子句
    let with_options = parse_with_clause(query)?;
    
    // 2. 使用默认配置
    let partition_strategy = match with_options.get("PARTITION_STRATEGY") {
        Some("HASH") => PartitionStrategy::Hash {
            field: with_options.get("PARTITION_FIELD")
                .unwrap_or(&"id".to_string()).clone(),
            num_partitions: with_options.get("PARTITIONS")
                .and_then(|s| s.parse().ok())
                .unwrap_or(1),
        },
        _ => PartitionStrategy::Hash {
            field: "id".to_string(),
            num_partitions: 1,
        }
    };
    
    // 3. 使用默认持久化策略
    let persist_policy = PersistPolicy::default();
    
    // ...
}
```

**优点**:
- ✅ 兼顾 SQL 用户习惯
- ✅ 可通过 MySQL 客户端建表

**缺点**:
- ❌ 语法不标准，用户需要学习
- ❌ 维护复杂度增加
- ❌ 容易误导用户（看起来像 SQL 但行为不同）

### 方案 C: 自动建表 + Schema Evolution (创新)

**核心思想**: 首次插入数据时自动创建表，后续自动推断和演化 schema。

```python
# 用户直接插入数据，无需建表
POST http://localhost:9200/taxi_trips/_doc
{
    "id": 1,
    "vendor_id": 2,
    "pickup_datetime": "2024-01-01T10:00:00Z"
}

# Calm 自动:
# 1. 创建表 taxi_trips
# 2. 推断字段类型 (id: U64, vendor_id: I32, pickup_datetime: Timestamp)
# 3. 使用默认分区策略 (Hash on id, 1 partition)
# 4. 使用默认持久化策略
```

**实现要点**:
```rust
async fn handle_insert(...) {
    // 检查表是否存在
    if !engine.table_exists(table_name) {
        // 从第一条数据推断 schema
        let schema = infer_schema_from_json(&first_doc)?;
        
        // 自动创建表
        engine.create_table(
            table_name,
            schema,
            PartitionStrategy::default(),
            1
        ).await?;
        
        log::info!("✨ Auto-created table: {}", table_name);
    }
    
    // 继续插入
    ...
}

fn infer_schema_from_json(doc: &JsonValue) -> Schema {
    let mut fields = vec![];
    
    for (key, value) in doc.as_object().unwrap() {
        let field = match value {
            JsonValue::Number(n) if n.is_i64() => FieldOption::I64 { ... },
            JsonValue::Number(n) if n.is_u64() => FieldOption::U64 { ... },
            JsonValue::Number(n) if n.is_f64() => FieldOption::F64 { ... },
            JsonValue::String(s) if is_timestamp(s) => FieldOption::Timestamp { ... },
            JsonValue::String(_) => FieldOption::Keyword { ... },
            JsonValue::Bool(_) => FieldOption::Boolean { ... },
            _ => FieldOption::Keyword { ... },
        };
        fields.push(field);
    }
    
    Schema::new(...)
}
```

**优点**:
- ✅ 用户体验极佳（零配置）
- ✅ 类似 MongoDB、ES 的自动建表
- ✅ 降低学习成本

**缺点**:
- ❌ 类型推断可能不准确
- ❌ 无法配置分区策略
- ❌ 后续 schema 变更复杂

---

## 最终推荐

### 🎯 推荐方案: **方案 A (保持现状 + 文档增强)**

**理由**:

1. **设计一致性**: GraphQL 本来就是管理接口，DDL 归它管理符合逻辑
2. **避免复杂性**: SQL/ES DDL 的完整兼容成本太高，收益有限
3. **用户教育**: 通过清晰的文档和友好的错误提示引导用户
4. **灵活性**: GraphQL 可以轻松扩展新特性

### 🔧 实施清单

#### 1. 增强 GraphQL DDL (2-3 天)
- [ ] 添加 description 字段 (表和字段)
- [ ] 添加 default_value 字段
- [ ] 添加 nullable 字段
- [ ] 添加 is_array 配置
- [ ] 完善错误提示

#### 2. SQL/ES 友好报错 (1 天)
- [ ] CREATE TABLE 返回清晰的错误信息和 GraphQL 示例
- [ ] PUT /<index> 返回替代方案说明
- [ ] 添加文档链接

#### 3. 文档完善 (2 天)
- [ ] 编写 "DDL 设计理念" 文档
- [ ] 编写 "快速开始" 教程
- [ ] 编写 "从 SQL/ES 迁移" 指南
- [ ] 提供完整的 GraphQL DDL 示例
- [ ] 录制视频教程

#### 4. 工具支持 (可选, 1-2 天)
- [ ] 提供 CLI 工具 (calm create-table)
- [ ] 提供 Python SDK
- [ ] 提供 Web UI (基于 GraphQL Playground)

### 📊 对比表

| 特性 | SQL | ES | GraphQL |
|------|-----|----|----|
| **建表支持** | ❌ 不支持 | ❌ 不支持 | ✅ 完整支持 |
| **查询支持** | ✅ 完整 | ✅ 完整 | ✅ 完整 |
| **分区配置** | - | - | ✅ Hash/Range/Custom/None |
| **持久化策略** | - | - | ✅ 完整配置 |
| **字段类型** | - | - | ✅ 14 种类型 |
| **索引控制** | - | - | ✅ 每字段可控 |
| **适用场景** | 数据分析 | 日志搜索 | 系统管理 |

---

## 未来展望

### Phase 1: 现在 (MVP)
- ✅ GraphQL DDL (90% 完成)
- ✅ SQL 查询 (90% 完成)
- ✅ ES API 查询 (80% 完成)

### Phase 2: 短期 (3-6 个月)
- 🔄 Schema Evolution (自动推断字段类型变化)
- 🔄 ALTER TABLE 支持 (通过 GraphQL)
- 🔄 约束系统 (UNIQUE, NOT NULL)
- 🔄 外键关系 (跨表引用)

### Phase 3: 中期 (6-12 个月)
- 📅 视图支持 (Virtual Table)
- 📅 物化视图 (Materialized View)
- 📅 触发器 (Trigger)
- 📅 存储过程 (Stored Procedure via GraphQL)

### Phase 4: 长期 (12+ 个月)
- 🔮 多租户 (Database/Schema 层级)
- 🔮 权限系统 (RBAC)
- 🔮 审计日志
- 🔮 时间旅行 (Time Travel Query)

---

## 总结

**结论**: Calm 应该**明确定位 GraphQL 为唯一的 DDL 接口**，SQL 和 ES 专注于查询。

**原因**:
1. Calm 的分区和段管理系统是独特的，无法映射到标准 SQL/ES
2. GraphQL 更灵活、更适合配置复杂的存储策略
3. 避免维护多套不完整的 DDL 实现
4. 通过清晰的文档和友好的错误提示，用户很快就能适应

**用户工作流**:
```
1. 建表:    GraphQL mutation createTable
2. 插入:    SQL INSERT / ES POST / GraphQL mutation insertData
3. 查询:    SQL SELECT / ES GET / GraphQL query
4. 管理:    GraphQL mutation (dropTable, flushTable, etc.)
```

这样的设计**清晰、一致、易维护**，符合 Calm 的长期发展方向。
