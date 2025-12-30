# DatetimeRange 分区策略测试文档

本文档提供完整的测试流程，用于验证 DatetimeRange 分区策略的按需创建功能。

## 测试环境

- GraphQL 端口: 9567
- MySQL 端口: 3307
- 测试表: `events` (事件日志表)

---

## 步骤 1: 通过 GraphQL 创建表

### 1.1 启动 Calm 服务

```bash
cargo run --release --bin calm -- --config calm.toml
```

### 1.2 执行 GraphQL Mutation 创建表

访问 GraphQL Playground: http://localhost:9567/graphql

执行以下 mutation:

```graphql
mutation {
  createTable(input: {
    name: "events"
    primaryKey: "event_id"
    partitionStrategy: {
      datetimeRange: {
        field: "event_time"
        granularity: DAY
        timezone: "UTC"
        parallelism: 2
      }
    }
    fields: [
      {
        name: "event_id"
        fieldType: U64
        nullable: false
        description: "事件ID - 主键"
      }
      {
        name: "event_time"
        fieldType: TIMESTAMP
        nullable: false
        format: "ms"
        description: "事件时间 - 分区键"
      }
      {
        name: "user_id"
        fieldType: U64
        nullable: false
        indexed: true
        description: "用户ID"
      }
      {
        name: "event_type"
        fieldType: KEYWORD
        nullable: false
        indexed: true
        description: "事件类型"
      }
      {
        name: "event_data"
        fieldType: TEXT
        nullable: true
        description: "事件详细数据"
      }
      {
        name: "ip_address"
        fieldType: KEYWORD
        nullable: true
        description: "IP地址"
      }
      {
        name: "created_at"
        fieldType: TIMESTAMP
        nullable: false
        format: "ms"
        description: "记录创建时间"
      }
    ]
  }) {
    name
    primaryKey
    partitionCount
  }
}
```

### 1.3 验证表创建

执行查询验证表结构:

```graphql
query {
  getTable(tableName: "events") {
    name
    primaryKey
    description
    partitionCount
    fields {
      name
      fieldType
      nullable
      indexed
    }
  }
}
```

---

## 步骤 2: 通过 GraphQL 插入测试数据

### 2.1 访问 GraphQL Playground

浏览器访问: http://localhost:9567/graphql

### 2.2 插入测试数据

#### 测试场景 1: 同一天的数据（会创建 2 个分区，parallelism=2）

```graphql
mutation {
  insertData(input: {
    table: "events"
    data: [
      {event_id: 1, event_time: 1705276800000, user_id: 1001, event_type: "login", event_data: "{\"device\": \"iPhone\"}", ip_address: "192.168.1.100", created_at: 1705276800000}
      {event_id: 2, event_time: 1705280400000, user_id: 1002, event_type: "page_view", event_data: "{\"page\": \"/home\"}", ip_address: "192.168.1.101", created_at: 1705280400000}
      {event_id: 3, event_time: 1705284000000, user_id: 1003, event_type: "click", event_data: "{\"button\": \"buy_now\"}", ip_address: "192.168.1.102", created_at: 1705284000000}
      {event_id: 4, event_time: 1705287600000, user_id: 1001, event_type: "logout", event_data: "{}", ip_address: "192.168.1.100", created_at: 1705287600000}
    ]
  }) {
    success
    rows_inserted
    message
  }
}
```

**预期结果**: 
- 自动创建分区 `20240115_0` 和 `20240115_1`
- 4条记录根据 hash(event_time) 分配到这两个分区

#### 测试场景 2: 不同天的数据（会创建多个日期的分区）

```graphql
# 2024-01-16 的数据
mutation {
  insertData(input: {
    table: "events"
    data: [
      {event_id: 5, event_time: 1705363200000, user_id: 1004, event_type: "login", event_data: "{\"device\": \"Android\"}", ip_address: "192.168.1.103", created_at: 1705363200000}
      {event_id: 6, event_time: 1705366800000, user_id: 1005, event_type: "search", event_data: "{\"query\": \"laptop\"}", ip_address: "192.168.1.104", created_at: 1705366800000}
    ]
  }) {
    success
    rows_inserted
  }
}

# 2024-01-17 的数据
mutation {
  insertData(input: {
    table: "events"
    data: [
      {event_id: 7, event_time: 1705449600000, user_id: 1006, event_type: "purchase", event_data: "{\"amount\": 999}", ip_address: "192.168.1.105", created_at: 1705449600000}
      {event_id: 8, event_time: 1705453200000, user_id: 1007, event_type: "login", event_data: "{\"device\": \"Web\"}", ip_address: "192.168.1.106", created_at: 1705453200000}
    ]
  }) {
    success
    rows_inserted
  }
}

# 2024-01-18 的数据
mutation {
  insertData(input: {
    table: "events"
    data: [
      {event_id: 9, event_time: 1705536000000, user_id: 1008, event_type: "logout", event_data: "{}", ip_address: "192.168.1.107", created_at: 1705536000000}
      {event_id: 10, event_time: 1705539600000, user_id: 1009, event_type: "page_view", event_data: "{\"page\": \"/products\"}", ip_address: "192.168.1.108", created_at: 1705539600000}
    ]
  }) {
    success
    rows_inserted
  }
}
```

**预期结果**:
- 自动创建分区 `20240116_0`, `20240116_1`, `20240117_0`, `20240117_1`, `20240118_0`, `20240118_1`
- 每天的数据分配到对应日期的 parallelism 个分区中

#### 测试场景 3: 大批量插入

```graphql
mutation {
  insertData(input: {
    table: "events"
    data: [
      {event_id: 11, event_time: 1705276800000, user_id: 1010, event_type: "login", event_data: "{}", ip_address: "192.168.1.109", created_at: 1705276800000}
      {event_id: 12, event_time: 1705363200000, user_id: 1011, event_type: "page_view", event_data: "{}", ip_address: "192.168.1.110", created_at: 1705363200000}
      {event_id: 13, event_time: 1705449600000, user_id: 1012, event_type: "click", event_data: "{}", ip_address: "192.168.1.111", created_at: 1705449600000}
      {event_id: 14, event_time: 1705536000000, user_id: 1013, event_type: "search", event_data: "{}", ip_address: "192.168.1.112", created_at: 1705536000000}
      {event_id: 15, event_time: 1705622400000, user_id: 1014, event_type: "purchase", event_data: "{}", ip_address: "192.168.1.113", created_at: 1705622400000}
      {event_id: 16, event_time: 1705708800000, user_id: 1015, event_type: "logout", event_data: "{}", ip_address: "192.168.1.114", created_at: 1705708800000}
      {event_id: 17, event_time: 1705795200000, user_id: 1016, event_type: "login", event_data: "{}", ip_address: "192.168.1.115", created_at: 1705795200000}
      {event_id: 18, event_time: 1705881600000, user_id: 1017, event_type: "page_view", event_data: "{}", ip_address: "192.168.1.116", created_at: 1705881600000}
      {event_id: 19, event_time: 1705968000000, user_id: 1018, event_type: "click", event_data: "{}", ip_address: "192.168.1.117", created_at: 1705968000000}
      {event_id: 20, event_time: 1706054400000, user_id: 1019, event_type: "logout", event_data: "{}", ip_address: "192.168.1.118", created_at: 1706054400000}
    ]
  }) {
    success
    rows_inserted
    message
  }
}
```

**预期结果**:
- 自动创建更多日期的分区（2024-01-19 到 2024-01-24）
- 每个日期 2 个分区，总共约 16 个分区

---

## 步骤 3: 验证结果

### 3.1 查询所有数据

```graphql
query {
  queryData(input: {
    table: "events"
    sql: "SELECT * FROM events ORDER BY event_time"
  }) {
    success
    data
    message
  }
}
```

### 3.2 按日期查询（测试分区裁剪）

```graphql
# 查询 2024-01-15 的数据
query {
  queryData(input: {
    table: "events"
    sql: "SELECT * FROM events WHERE event_time >= 1705276800000 AND event_time < 1705363200000"
  }) {
    success
    data
  }
}

# 查询 2024-01-16 的数据
query {
  queryData(input: {
    table: "events"
    sql: "SELECT * FROM events WHERE event_time >= 1705363200000 AND event_time < 1705449600000"
  }) {
    success
    data
  }
}
```

### 3.3 通过 GraphQL 查询分区信息

```graphql
query {
  getTable(tableName: "events") {
    name
    partitionCount
  }
}
```

### 3.4 查看日志确认分区创建

查看 Calm 服务日志，应该看到类似信息:

```
✅ Auto-creating partition for on-demand strategy: table=events, partition=20240115_0
✅ Auto-creating partition for on-demand strategy: table=events, partition=20240115_1
✅ Partition created successfully: events/20240115_0
✅ Partition created successfully: events/20240115_1
```

---

## 步骤 4: 高级测试（可选）

### 4.1 测试并发插入

打开多个 GraphQL Playground 窗口，同时执行相同日期的 insertData，验证并发创建分区的幂等性。

### 4.2 测试分布式环境

1. 启动多节点集群
2. 插入数据到不同节点
3. 验证分区在所有节点的一致性

### 4.3 测试时区

修改 `timezone` 为 `"Asia/Shanghai"` 重新创建表，验证时区影响。

---

## 时间戳参考

| 日期 | 时间戳 (ms) | 分区名 |
|------|-------------|--------|
| 2024-01-15 00:00:00 UTC | 1705276800000 | 20240115_0, 20240115_1 |
| 2024-01-16 00:00:00 UTC | 1705363200000 | 20240116_0, 20240116_1 |
| 2024-01-17 00:00:00 UTC | 1705449600000 | 20240117_0, 20240117_1 |
| 2024-01-18 00:00:00 UTC | 1705536000000 | 20240118_0, 20240118_1 |
| 2024-01-19 00:00:00 UTC | 1705622400000 | 20240119_0, 20240119_1 |
| 2024-01-20 00:00:00 UTC | 1705708800000 | 20240120_0, 20240120_1 |
| 2024-01-21 00:00:00 UTC | 1705795200000 | 20240121_0, 20240121_1 |
| 2024-01-22 00:00:00 UTC | 1705881600000 | 20240122_0, 20240122_1 |
| 2024-01-23 00:00:00 UTC | 1705968000000 | 20240123_0, 20240123_1 |
| 2024-01-24 00:00:00 UTC | 1706054400000 | 20240124_0, 20240124_1 |

---

## 预期行为总结

✅ **正确行为**:
1. 首次插入某天数据时，自动创建该天的所有分区（parallelism 个）
2. 分区创建是幂等的，并发插入不会导致错误
3. 后续插入相同日期数据，不再创建分区，直接插入
4. 分区信息通过 Gossip 协议同步到所有节点
5. 查询时能够正确进行分区裁剪（Partition Pruning）

❌ **错误行为**:
1. 分区未自动创建，返回 "Partition not found" 错误
2. 并发创建分区导致冲突或 panic
3. 分区创建后数据丢失
4. 分区信息在不同节点不一致

---

## 清理测试数据

```graphql
mutation {
  dropTable(tableName: "events") {
    success
    message
  }
}
```

---

## 故障排查

如果遇到问题，检查以下内容:

1. **日志**: 查看 `logs/calm.log` 确认分区创建流程
2. **分区文件**: 检查 `data/tables/events/` 目录下的分区文件
3. **Catalog**: 验证 Catalog 中是否记录了分区元数据
4. **时间戳格式**: 确保使用毫秒级时间戳 (ms)
5. **字段类型**: 确保 `event_time` 字段类型为 TIMESTAMP

---

## 进一步测试

- 测试 Hour 粒度: `granularity: HOUR`
- 测试 Month 粒度: `granularity: MONTH`
- 测试不同并行度: `parallelism: 4` 或 `parallelism: 1`
- 测试中国时区: `timezone: "Asia/Shanghai"`
