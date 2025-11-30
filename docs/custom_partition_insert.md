# GraphQL 指定分区插入功能

## 功能概述

增强了 `insertData` mutation,支持指定分区名称插入数据,跳过路由策略。如果指定的分区不存在,系统会自动创建新分区。

## 使用场景

1. **数据迁移**: 从其他系统迁移数据时,保持原有的分区结构
2. **手动分区管理**: 需要精确控制数据分布时,手动指定数据插入的分区
3. **动态分区创建**: 根据业务需求动态创建新分区(如按客户、按地区分区)
4. **批量导入**: 大批量数据导入时,直接指定目标分区提高性能

## API 变更

### InsertDataInput 新增字段

```graphql
input InsertDataInput {
  table: String!           # 表名
  data: [JSON!]!           # JSON 数据数组
  partition: String        # 新增:可选的分区名称
}
```

### 行为说明

#### 1. 不指定 partition(默认行为)

使用表的路由策略自动计算分区:

```graphql
mutation {
  insertData(input: {
    table: "users"
    data: [
      {"user_id": 1, "username": "alice"}
      {"user_id": 2, "username": "bob"}
    ]
  }) {
    success
    rows_inserted
    message
  }
}
```

**流程:**
1. 提取主键值
2. 根据路由策略(Hash/Range)计算 partition_id
3. 插入到计算出的分区

#### 2. 指定 partition(新功能)

直接插入到指定分区,跳过路由策略:

```graphql
mutation {
  insertData(input: {
    table: "users"
    partition: "custom_partition_A"
    data: [
      {"user_id": 100, "username": "custom_user_1"}
      {"user_id": 101, "username": "custom_user_2"}
    ]
  }) {
    success
    rows_inserted
    message
  }
}
```

**流程:**
1. 检查分区是否存在
2. 如果不存在,自动创建新分区
3. 批量插入所有数据到指定分区

## 详细示例

### 示例 1: 自动路由插入

```graphql
# 创建表(Hash 分区,2 个分区)
mutation {
  createTable(input: {
    name: "orders"
    primary_key: "order_id"
    partition_count: 2
    fields: [
      { name: "order_id", field_type: U64, nullable: false }
      { name: "customer_id", field_type: U64, nullable: false }
      { name: "amount", field_type: F64, nullable: false }
    ]
  }) { name }
}

# 插入数据(使用路由策略,根据 order_id 自动分配到 p0 或 p1)
mutation {
  insertData(input: {
    table: "orders"
    data: [
      {"order_id": 1, "customer_id": 100, "amount": 99.99}
      {"order_id": 2, "customer_id": 101, "amount": 199.99}
    ]
  }) {
    success
    rows_inserted
    message
  }
}
# 返回: "Successfully inserted 2 rows"
```

### 示例 2: 指定现有分区插入

```graphql
# 直接插入到 p0 分区
mutation {
  insertData(input: {
    table: "orders"
    partition: "p0"
    data: [
      {"order_id": 100, "customer_id": 200, "amount": 299.99}
    ]
  }) {
    success
    rows_inserted
    message
  }
}
# 返回: "Successfully inserted 1 rows to partition 'p0'"
```

### 示例 3: 自动创建新分区

```graphql
# 指定不存在的分区,系统自动创建
mutation {
  insertData(input: {
    table: "orders"
    partition: "customer_vip"  # 新分区,为 VIP 客户单独分区
    data: [
      {"order_id": 1000, "customer_id": 999, "amount": 9999.99}
      {"order_id": 1001, "customer_id": 999, "amount": 8888.88}
    ]
  }) {
    success
    rows_inserted
    message
  }
}
# 返回: "Successfully inserted 2 rows to partition 'customer_vip'"
# 日志: "Partition 'customer_vip' not found for table 'orders', creating new partition"
```

### 示例 4: 按地区手动分区

```graphql
# 创建按地区划分的用户表
mutation {
  createTable(input: {
    name: "users_by_region"
    primary_key: "user_id"
    partition_count: 1  # 不使用自动分区
    fields: [
      { name: "user_id", field_type: U64, nullable: false }
      { name: "username", field_type: KEYWORD, nullable: false }
      { name: "region", field_type: KEYWORD, nullable: false }
    ]
  }) { name }
}

# 插入北美用户
mutation {
  insertData(input: {
    table: "users_by_region"
    partition: "region_north_america"
    data: [
      {"user_id": 1, "username": "alice", "region": "US"}
      {"user_id": 2, "username": "bob", "region": "CA"}
    ]
  }) { success rows_inserted }
}

# 插入欧洲用户
mutation {
  insertData(input: {
    table: "users_by_region"
    partition: "region_europe"
    data: [
      {"user_id": 100, "username": "charlie", "region": "UK"}
      {"user_id": 101, "username": "david", "region": "DE"}
    ]
  }) { success rows_inserted }
}

# 插入亚洲用户
mutation {
  insertData(input: {
    table: "users_by_region"
    partition: "region_asia"
    data: [
      {"user_id": 200, "username": "emma", "region": "JP"}
      {"user_id": 201, "username": "frank", "region": "CN"}
    ]
  }) { success rows_inserted }
}

# 查看所有分区
query {
  partitions(table: "users_by_region") {
    partition_id
    segment_count
  }
}
# 返回:
# [
#   { partition_id: "region_north_america", segment_count: 1 },
#   { partition_id: "region_europe", segment_count: 1 },
#   { partition_id: "region_asia", segment_count: 1 }
# ]
```

## 技术实现

### 代码逻辑

```rust
async fn insert_data(&self, ctx: &Context<'_>, input: InsertDataInput) -> Result<InsertResult> {
    let engine = ctx.data::<Arc<Engine>>()?;
    let meta = engine.get_table_meta(&input.table)?;

    // 情况 1: 指定了 partition
    if let Some(partition_name) = &input.partition {
        // 获取或创建分区
        let partition = match engine.get_partition(&input.table, partition_name).await {
            Some(p) => p,  // 分区已存在
            None => {
                // 分区不存在,自动创建
                log::info!("Creating new partition: {}", partition_name);
                engine.load_partition(&input.table, partition_name.clone(), meta.schema.clone()).await?
            }
        };
        
        // 批量插入到指定分区
        partition.upsert_json(&input.data)?;
        return Ok(...);
    }

    // 情况 2: 未指定 partition,使用路由策略
    for doc in &input.data {
        let pk_value = doc.get(pk_field)?;
        let partition_id = engine.route_partition(&input.table, pk_value)?;
        let partition = engine.get_partition(&input.table, &partition_id).await?;
        partition.upsert_json(&[doc.clone()])?;
    }
    
    Ok(...)
}
```

### 分区创建机制

使用 `engine.load_partition()` 方法创建新分区:

1. 生成分区目录: `data/tables/{table_name}/{partition_name}/`
2. 创建 Partition 实例(使用表的 schema)
3. 注册到 Engine 的 partitions map
4. 返回 Arc<Partition> 引用

## 优势

1. **灵活性**: 支持自动路由和手动指定两种模式
2. **简化迁移**: 数据迁移时无需担心路由策略
3. **性能优化**: 批量插入到同一分区,减少分区查找开销
4. **动态扩展**: 按需创建新分区,支持业务动态变化

## 注意事项

1. **分区命名**: 分区名称必须符合目录命名规范(字母、数字、下划线、短横线)
2. **Schema 一致性**: 自动创建的分区使用表的 schema,无法指定不同的 schema
3. **并发安全**: 多个请求同时创建同一分区时,只有一个会成功创建,其他会使用已创建的分区
4. **查询范围**: 使用 SQL 查询时,会扫描所有分区(包括自定义分区)

## 测试

运行测试脚本:

```bash
# 启动服务
cargo run

# 运行测试脚本(另一个终端)
./test_custom_partition_insert.sh
```

测试内容:
1. 创建测试表
2. 自动路由插入
3. 指定现有分区插入
4. 自动创建新分区并插入
5. 验证所有分区和数据

## 相关文档

- [GraphQL createTable 使用指南](docs/graphql_createtable_guide.md)
- [分区策略设计](docs/DDL_DESIGN_ANALYSIS.md)
- [GraphQL DDL 完整示例](docs/graphql_ddl_complete_example.md)

---

**版本:** v1.0
**更新时间:** 2024
