# Calm Database 系统架构文档

> **版本**: 0.1.0  
> **最后更新**: 2025年12月29日  
> **作者**: Calm Team

---

## 目录

- [1. 系统概述](#1-系统概述)
- [2. 核心架构](#2-核心架构)
- [3. 关键模块详解](#3-关键模块详解)
- [4. 数据流与处理路径](#4-数据流与处理路径)
- [5. 分布式协调](#5-分布式协调)
- [6. 存储架构](#6-存储架构)
- [7. 多协议支持](#7-多协议支持)
- [8. 查询执行](#8-查询执行)
- [9. 配置与部署](#9-配置与部署)
- [10. 技术栈](#10-技术栈)

---

## 1. 系统概述

### 1.1 项目定位

**Calm** 是一个用 Rust 编写的现代分布式 HTAP（混合事务/分析处理）数据库，具有以下核心特点：

- 🚀 **高性能**: 基于 Rust 的零成本抽象和内存安全保证
- 🌐 **分布式**: 采用共享存储 + 无状态计算节点架构
- 🔌 **多协议**: 原生支持 GraphQL、MySQL、Elasticsearch 协议
- 📊 **HTAP**: 同时支持事务和分析工作负载
- 🔄 **Gossip 协调**: 去中心化的节点发现和协调机制

### 1.2 设计原则

#### 核心架构原则

**Partition Owner Model (分区所有者模型)**

- 每个分区在任意时刻有且仅有一个写入者（owner）
- 所有节点可以从共享存储读取所有分区
- 保证数据一致性同时提供高可用性

#### 存储计算分离

```
┌─────────────────────────────────────┐
│     共享存储层 (CubeFS/S3)          │
│  ┌───────┐  ┌───────┐  ┌───────┐   │
│  │Part 1 │  │Part 2 │  │Part 3 │   │
│  └───────┘  └───────┘  └───────┘   │
└─────────────────────────────────────┘
         ↑           ↑           ↑
         │           │           │
    ┌────┴───┐  ┌───┴────┐  ┌───┴────┐
    │ Node 1 │  │ Node 2 │  │ Node 3 │  ← 无状态计算节点
    │(Owner) │  │(Owner) │  │(Reader)│
    └────────┘  └────────┘  └────────┘
```

### 1.3 应用场景

- **日志分析**: 高吞吐量写入 + 灵活的分析查询
- **实时分析**: 实时数据摄入 + 秒级查询响应
- **时序数据**: 基于时间范围的分区策略
- **多租户系统**: 基于租户ID的分区隔离

---

## 2. 核心架构

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────┐
│                     客户端层                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐          │
│  │ GraphQL  │  │  MySQL   │  │  Elasticsearch   │          │
│  │  Client  │  │  Client  │  │     Client       │          │
│  └────┬─────┘  └────┬─────┘  └────────┬─────────┘          │
└───────┼─────────────┼──────────────────┼────────────────────┘
        │             │                  │
┌───────┼─────────────┼──────────────────┼────────────────────┐
│       │    协议层 (Protocol Layer)     │                     │
│  ┌────▼─────┐  ┌───▼──────┐  ┌────────▼─────────┐          │
│  │ GraphQL  │  │  MySQL   │  │  Elasticsearch   │          │
│  │  Server  │  │  Server  │  │     Server       │          │
│  │  (9567)  │  │  (3307)  │  │     (9200)       │          │
│  └────┬─────┘  └────┬─────┘  └────────┬─────────┘          │
└───────┼─────────────┼──────────────────┼────────────────────┘
        │             │                  │
        └─────────────┴──────────────────┘
                      │
┌─────────────────────▼─────────────────────────────────────┐
│              CalmService (核心协调层)                      │
│  ┌──────────┐  ┌────────────────┐  ┌──────────────┐      │
│  │ Catalog  │  │ClusterManager  │  │   Engine     │      │
│  │(元数据)  │  │  (Gossip协调)  │  │ (本地操作)   │      │
│  └──────────┘  └────────────────┘  └──────────────┘      │
└───────────────────────────────────────────────────────────┘
        │                  │                     │
        │                  │                     │
┌───────▼──────────────────▼─────────────────────▼──────────┐
│            统一 gRPC/Arrow Flight 通信层                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │do_action │  │ do_get   │  │ do_put   │  │do_exchange│ │
│  │(DDL/DML) │  │(分布式查询)│ │(数据插入)│  │(双向流)  │  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
└───────────────────────────────────────────────────────────┘
        │                  │                     │
        └──────────────────┴─────────────────────┘
                           │
┌──────────────────────────▼────────────────────────────────┐
│                  存储层 (Storage Layer)                    │
│  ┌──────────────────────────────────────────────────────┐ │
│  │           Partition (分区)                            │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐           │ │
│  │  │Segment 1 │  │Segment 2 │  │Segment 3 │           │ │
│  │  │(内存/磁盘) │ │(内存/磁盘) │ │(内存/磁盘) │          │ │
│  │  └──────────┘  └──────────┘  └──────────┘           │ │
│  └──────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────┘
```

### 2.2 分层说明

#### Layer 1: 协议层 (Protocol Layer)
- **职责**: 多协议接入，协议解析和转换
- **组件**: GraphQL Server、MySQL Server、Elasticsearch Server
- **特点**: 
  - GraphQL 是唯一的 DDL 接口
  - MySQL/ES 提供数据操作但不支持建表

#### Layer 2: 服务层 (Service Layer)
- **职责**: 核心业务逻辑协调
- **组件**: CalmService (Catalog + ClusterManager + Engine)
- **特点**: 统一的数据操作接口

#### Layer 3: 通信层 (Communication Layer)
- **职责**: 节点间通信
- **协议**: 统一 gRPC/Arrow Flight
- **端口**: 自动分配 (52000+)

#### Layer 4: 存储层 (Storage Layer)
- **职责**: 数据持久化和读取
- **结构**: Partition → Segment → Columnar Storage
- **特点**: 
  - 支持内存和磁盘混合存储
  - 主键去重
  - 全文索引

---

## 3. 关键模块详解

### 3.1 CalmService (核心协调器)

**位置**: `src/calm/mod.rs`

**职责**: 顶层服务协调器，整合所有核心组件

**结构**:
```rust
pub struct CalmService {
    catalog: Arc<Catalog>,                    // 元数据管理
    cluster_manager: ClusterManagerRef,       // 集群协调
    engine: Arc<Engine>,                      // 本地引擎
}
```

**关键方法**:
- `new(config)`: 初始化服务
- `start()`: 启动所有组件
- `shutdown()`: 优雅关闭

**子模块**:
- `service.rs`: RPC 服务定义
- `flight_service.rs`: Arrow Flight 服务实现
- `flight_actions.rs`: Flight 动作处理器
- `job/`: 分布式作业管理

### 3.2 Catalog (元数据管理)

**位置**: `src/catalog/mod.rs`

**职责**: 管理表结构和分区元数据

**核心类型**:
- **TableMeta**: 表元数据，包含表名、主键、字段定义、分区策略、创建时间
- **PartitionMeta**: 分区元数据，包含分区ID、所属表名、版本号、所有者节点

**分区策略**:
- `PKHash`: 按主键哈希 (默认)
- `Hash`: 按指定字段哈希
- `Range`: 按范围分区 (支持 Int64/Timestamp)
- `Custom`: 用户自定义
- `None`: 单分区

### 3.3 ClusterManager (集群协调)

**位置**: `src/cluster/mod.rs`

**职责**: 基于 Gossip 的节点发现和分区所有权管理

**节点 ID 格式**:
```
timestamp_host_grpc_port
示例: 20251229120530123_127.0.0.1_52000
```

**关键功能**:
1. **节点发现**: 通过 Chitchat 发现集群中的其他节点
2. **分区所有权**: 通过 Gossip 协议同步分区所有权信息
3. **故障检测**: 自动检测节点故障并重新分配分区
4. **负载均衡**: 基于节点负载自动迁移分区

**Gossip 键空间**:
- `partition_owner`: 分区所有权映射
- `node_status`: 节点状态信息（alive, dead, leaving）
- `partition_version`: 分区版本号

### 3.4 Engine (本地数据引擎)

**位置**: `src/engine/mod.rs`

**重构后结构** (2024年12月重构):
```
src/engine/
├── mod.rs              (78行)  - Engine 结构体定义
├── config.rs           (63行)  - 配置和共享类型
├── metadata.rs        (377行)  - 表和分区元数据管理
├── lifecycle.rs       (299行)  - 启动、加载、停止
├── persist.rs         (328行)  - 后台持久化任务
└── data_operations.rs (349行)  - 插入、查询、加载
```

**核心接口**:
- `insert_batch()`: 统一插入接口，支持自动路由和批量插入
- `scan_partition()`: 分区扫描接口，返回指定分区的数据流
- `load_partition()`: 分区加载接口，从磁盘加载持久化分区
- `persist_partition()`: 分区持久化接口，将内存数据刷盘

**支持的字段类型**:

**数值类型**:
- `I8`, `I16`, `I32`, `I64`: 有符号整数 (8/16/32/64位)
- `U8`, `U16`, `U32`, `U64`: 无符号整数 (8/16/32/64位)
- `F32`, `F64`: 浮点数 (32/64位)

**时间类型**:
- `TIMESTAMP`: 时间戳，支持多种格式
  - 格式选项: `s` (秒), `ms` (毫秒), `us` (微秒), `ns` (纳秒)
  - 支持字符串解析: ISO8601、RFC3339 等标准格式

**字符串类型**:
- `TEXT`: 长文本，支持全文索引
- `KEYWORD`: 关键词，精确匹配，支持排序和聚合

**布尔类型**:
- `BOOLEAN`: 布尔值 (true/false)

**数组类型**:
- 所有基础类型支持数组形式
- 示例: `KEYWORD[]` (关键词数组), `I64[]` (整数数组)

**字段选项**:
- `nullable`: 是否允许 NULL 值
- `indexed`: 是否创建索引
- `format`: 时间戳格式化选项
- `isArray`: 是否为数组类型

**核心特性**:
- ✅ 自动路由数据到对应分区
- ✅ 按需创建分区
- ✅ 后台持久化任务
- ✅ 主键去重机制
- ✅ 内存/磁盘混合存储
- ✅ 基于 Arrow 的列式存储
- ✅ 支持 13 种基础数据类型

### 3.5 Router (路由模块)

**位置**: `src/storage/router/mod.rs`

**职责**: 根据分区策略将数据路由到正确的分区

**架构** (2024年12月重构):
```
src/storage/router/
├── mod.rs          - Router 工厂和统一接口
├── hash_router.rs  - PKHash + Hash 路由实现
├── range_router.rs - Range 路由实现
└── utils.rs        - 共享工具函数
```

**核心接口**:
- `route_batch()`: 根据分区策略将 RecordBatch 路由到目标分区
- 支持策略模式，可灵活扩展新的路由算法

**路由策略**:
- **PKHash**: 对主键进行哈希运算，均匀分布到 N 个分区
- **Hash**: 对指定字段进行哈希，支持自定义分区字段
- **Range**: 基于范围分区，支持整数和时间戳类型
  - 适用场景: 时序数据、日志分析
  - 优势: 支持分区裁剪，查询性能优异
- **Custom**: 用户自定义分区逻辑
- **None**: 单分区模式，适合小规模数据

### 3.6 Storage (存储抽象)

**位置**: `src/storage/`

**结构**:
```
src/storage/
├── partition.rs    - 分区管理
├── segment/        - 段存储
│   ├── mod.rs             - Segment 核心逻辑
│   ├── fulltext/          - 全文索引
│   │   ├── posting_list.rs
│   │   ├── inverted_index.rs
│   │   └── tokenizer.rs
│   ├── persist.rs         - 持久化
│   └── segment_loader.rs  - 段加载
└── router/         - 路由逻辑
```

**Partition (分区)**:
```rust
pub struct Partition {
    partition_id: String,
    table_meta: Arc<TableMeta>,
    segments: RwLock<Vec<Arc<Segment>>>,
    pk_index: RwLock<PrimaryKeyIndex>,
}
```

**Segment (段)**:
```rust
pub struct Segment {
    segment_id: u64,
    batch: RecordBatch,
    deleted_bitmap: RoaringBitmap,
    fulltext_index: Option<InvertedIndex>,
    is_persisted: bool,
    base_path: Option<PathBuf>,
}
```

### 3.7 Compute (查询执行)

**位置**: `src/compute/mod.rs`

**职责**: 基于 DataFusion 的查询执行引擎

**关键特性**:
- 基于 DataFusion 51.0
- 支持分布式查询
- 分区裁剪优化
- 全文搜索下推

---

## 4. 数据流与处理路径

### 4.1 写入路径 (Write Path)

```
┌──────────┐
│  Client  │
└────┬─────┘
     │ INSERT/PUT
     ▼
┌─────────────────┐
│ Protocol Layer  │  GraphQL/MySQL/ES
└────┬────────────┘
     │ RecordBatch
     ▼
┌─────────────────┐
│  CalmService    │
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│     Router      │  根据分区策略路由
└────┬────────────┘
     │ RoutedBatches
     ▼
┌─────────────────┐
│ ClusterManager  │  查找分区 Owner
└────┬────────────┘
     │
     ├─ Local Partition  ──────┐
     │                          │
     └─ Remote Partition ───┐   │
                            │   │
                            ▼   ▼
                    ┌──────────────┐
                    │    Engine    │
                    │insert_batch()│
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │  Partition   │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │   Segment    │
                    │  (In-Memory) │
                    └──────┬───────┘
                           │
                           ▼ (后台任务)
                    ┌──────────────┐
                    │    Persist   │
                    │  (To Disk)   │
                    └──────────────┘
```

**详细步骤**:

1. **协议层接收**: 客户端通过 GraphQL/MySQL/ES 发送数据
2. **转换为 RecordBatch**: 协议层将数据转换为 Arrow RecordBatch
3. **路由**: Router 根据分区策略计算目标分区
4. **定位 Owner**: ClusterManager 查找分区所有者节点
5. **远程转发** (如需要): 通过 gRPC 转发到 Owner 节点
6. **本地插入**: Engine 将数据插入内存 Segment
7. **主键去重**: Partition 维护主键索引去重
8. **后台持久化**: 定期将内存 Segment 持久化到磁盘

### 4.2 查询路径 (Read Path)

```
┌──────────┐
│  Client  │
└────┬─────┘
     │ SELECT/QUERY
     ▼
┌─────────────────┐
│ Protocol Layer  │  GraphQL/MySQL/ES
└────┬────────────┘
     │ Query AST
     ▼
┌─────────────────┐
│  CalmService    │
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│  Compute/       │  生成执行计划
│  DataFusion     │
└────┬────────────┘
     │ Physical Plan
     ▼
┌─────────────────┐
│ Partition       │  分区裁剪
│   Pruning       │
└────┬────────────┘
     │ Selected Partitions
     ▼
┌─────────────────────────────┐
│  Distributed Query Engine   │
│  (Arrow Flight do_get)      │
└─────┬───────────────────┬───┘
      │                   │
      ▼                   ▼
┌───────────┐       ┌───────────┐
│  Node 1   │       │  Node 2   │
│  Scan     │       │  Scan     │
└─────┬─────┘       └─────┬─────┘
      │                   │
      └─────────┬─────────┘
                │ RecordBatch Stream
                ▼
        ┌──────────────┐
        │   Aggregate  │
        │   & Filter   │
        └──────┬───────┘
               │
               ▼
        ┌──────────────┐
        │    Client    │
        └──────────────┘
```

**优化特性**:
- ✅ 分区裁剪 (Partition Pruning)
- ✅ 谓词下推 (Predicate Pushdown)
- ✅ 列裁剪 (Projection Pushdown)
- ✅ 全文搜索下推 (Fulltext Pushdown)
- ✅ 并行扫描 (Parallel Scan)

---

## 5. 分布式协调

### 5.1 Gossip 协议

**使用 Chitchat 0.9 实现去中心化协调**

```
┌─────────────────────────────────────────────────────────┐
│                   Gossip Network                        │
│                                                         │
│  ┌──────────┐         ┌──────────┐         ┌──────────┐│
│  │ Node 1   │◄───────►│ Node 2   │◄───────►│ Node 3   ││
│  │          │         │          │         │          ││
│  │ State:   │         │ State:   │         │ State:   ││
│  │ - P1:own │         │ - P2:own │         │ - P3:own ││
│  │ - P2:N2  │         │ - P1:N1  │         │ - P1:N1  ││
│  │ - P3:N3  │         │ - P3:N3  │         │ - P2:N2  ││
│  └──────────┘         └──────────┘         └──────────┘│
└─────────────────────────────────────────────────────────┘
```

**Gossip 键空间**:
```rust
// 分区所有权
partition_owner:table_name:partition_id = node_id

// 节点状态
node_status:node_id = {alive, dead, leaving}

// 分区版本
partition_version:table_name:partition_id = version_number
```

### 5.2 分区所有权模型

**核心原则**:
- 每个分区同一时刻仅有一个 Owner
- Owner 负责该分区的所有写入
- 所有节点可读取所有分区（从共享存储）

**所有权分配算法**:
```rust
// 基于一致性哈希
fn assign_partition_owner(partition_id: &str, nodes: &[Node]) -> Node {
    let hash = hash_partition_id(partition_id);
    let index = hash % nodes.len();
    nodes[index].clone()
}
```

**故障转移**:
1. 节点 N1 故障 (Gossip 检测到)
2. 集群重新计算 N1 拥有的分区的新 Owner
3. 新 Owner 从共享存储加载分区
4. 更新 Gossip 状态
5. 客户端请求自动路由到新 Owner

### 5.3 统一 gRPC 通信

**架构演进** (2024年11月):
- **之前**: 双协议 (tarpc RPC + Arrow Flight)
- **现在**: 统一 gRPC/Arrow Flight

**端口配置**:
```toml
[cluster.distributed]
# grpc_port 可选，None 时自动分配 (从 52000 开始)
grpc_port = 52000
query_timeout_ms = 30000
```

**Flight Actions**:
```rust
// DDL/DML 操作
do_action("create_table", params)
do_action("insert_batch", params)

// 分布式查询
do_get(ticket) -> Stream<RecordBatch>

// 数据插入
do_put(flight_data) -> Result

// 双向流
do_exchange(flight_data) -> Stream<FlightData>
```

---

## 6. 存储架构

### 6.1 存储层次

```
┌─────────────────────────────────────────────────────────┐
│                       Table                             │
├─────────────────────────────────────────────────────────┤
│  Partition 1  │  Partition 2  │  Partition 3  │  ...   │
├───────────────┼───────────────┼───────────────┼────────┤
│  Segment 1.1  │  Segment 2.1  │  Segment 3.1  │        │
│  Segment 1.2  │  Segment 2.2  │  Segment 3.2  │        │
│  Segment 1.3  │     ...       │     ...       │        │
└─────────────────────────────────────────────────────────┘
```

### 6.2 Segment 结构

**内存格式**:
- `segment_id`: 全局唯一的段标识符
- `batch`: Arrow RecordBatch 列式存储
- `deleted_bitmap`: RoaringBitmap 压缩的删除标记
- `fulltext_index`: 倒排索引支持全文搜索
- `bloom_filter`: 布隆过滤器加速主键查找

**磁盘格式**:
- `data.parquet`: Parquet 列式压缩数据文件
- `deleted.roaring`: RoaringBitmap 序列化的删除位图
- `fulltext.idx`: 持久化的倒排索引文件
- `metadata.json`: 段元数据（行数、列信息等）

### 6.3 主键索引

**主键索引结构**:
- 哈希表映射: 主键值 → (段ID, 行ID)
- 支持快速点查询（O(1) 时间复杂度）
- 自动去重机制

**去重流程**:
1. 计算主键哈希
2. 查找 PrimaryKeyIndex 判断是否已存在
3. 若存在旧版本，标记为删除（软删除）
4. 插入新版本数据并更新索引
5. 保证主键唯一性约束

### 6.4 全文索引

**统一架构** (2024年11月重构):
- `posting_list.rs`: 倒排列表实现
- `inverted_index.rs`: 倒排索引核心逻辑
- `tokenizer.rs`: 多语言分词器

**倒排索引结构**:
- Term → PostingList 映射
- PostingList 包含文档ID和词位置信息
- 支持位置感知的短语查询

**查询能力**:
- 单词匹配: 精确词匹配查询
- 短语匹配: 连续词序列查询
- 布尔查询: AND/OR/NOT 逻辑组合
- 前缀查询: 词前缀匹配
- 模糊查询: 基于编辑距离的模糊匹配

---

## 7. 多协议支持

### 7.1 协议层架构

```
src/protocol/
├── mod.rs
├── graphql/         - GraphQL 服务器
│   ├── schema.rs       - Schema 定义
│   ├── query.rs        - Query 解析
│   └── mutation.rs     - Mutation 处理
├── mysql/           - MySQL 协议
│   ├── server.rs       - 协议实现
│   ├── handler.rs      - 命令处理
│   └── resultset.rs    - 结果集转换
└── elasticsearch/   - Elasticsearch 协议
    ├── server.rs       - REST API
    ├── bulk.rs         - Bulk API
    └── search.rs       - Search API
```

### 7.2 GraphQL 协议

**端口**: 9567 (默认)

**DDL 操作**:
- `createTable`: 创建表，定义字段类型、分区策略
- `dropTable`: 删除表及其所有分区
- `alterTable`: 修改表结构（添加字段等）
- `showTables`: 列出所有表
- `describeTable`: 查看表结构详情

**查询操作**:
- 支持灵活的 filter 条件（eq, ne, gt, lt, in 等）
- 支持 limit/offset 分页
- 支持 orderBy 排序
- 支持字段投影（选择返回字段）
- 支持聚合查询（count, sum, avg 等）

**关键特性**:
- ✅ 唯一的 DDL 接口
- ✅ 灵活的类型系统
- ✅ 内省支持
- ✅ 批量操作

### 7.3 MySQL 协议

**端口**: 3307 (默认)

**协议实现**:
- 基于 `msql-srv` (Calm 定制版本)
- 支持 MySQL 5.7+ 协议
- 支持 `CLIENT_SECURE_CONNECTION` 认证

**支持的 SQL 功能**:
- ✅ SELECT 查询（单表）
- ✅ INSERT/REPLACE 语句
- ✅ UPDATE/DELETE（基于主键）
- ✅ LIMIT/OFFSET 分页
- ✅ WHERE 条件过滤
- ✅ ORDER BY 排序
- ✅ 流式结果集（防止内存溢出）
- ❌ JOIN 操作（规划中）
- ❌ DDL 操作（请使用 GraphQL）

**客户端兼容性**:
- JDBC Driver
- MySQL Workbench
- DataGrip
- DBeaver
- MySQL CLI

### 7.4 Elasticsearch 协议

**端口**: 9200 (默认)

**Bulk API**:
- 支持批量 index/update/delete 操作
- 兼容 Elasticsearch Bulk API 格式
- 自动路由到对应分区
- 支持批量错误处理

**Search API**:
- Match Query: 全文搜索
- Term Query: 精确匹配
- Range Query: 范围查询
- Bool Query: 布尔组合查询
- 分页参数: size/from

**支持的功能**:
- ✅ Bulk Index/Update/Delete
- ✅ Search API (基础)
- ✅ Match Query (全文搜索)
- ✅ Term Query (精确匹配)
- ✅ Range Query (范围查询)
- ❌ Aggregations (计划中)

---

## 8. 查询执行

### 8.1 DataFusion 集成

**版本**: DataFusion 51.0

**执行流程**:
```
SQL/GraphQL Query
      ↓
Logical Plan (DataFusion)
      ↓
Optimized Logical Plan
      ↓
Physical Plan
      ↓
Distributed Physical Plan
      ↓
Execution (Arrow Flight)
```

### 8.2 分区裁剪

**裁剪原理**:
- 分析查询条件中的分区键
- 计算需要扫描的分区范围
- 过滤掉不相关的分区

**适用场景**:
- Range 分区 + 范围查询
- Hash 分区 + 等值查询
- PKHash 分区 + 主键查询

**效果**:
- 12 个月分区 → 仅扫描 1 个分区
- 查询性能提升 10x+

### 8.3 全文搜索优化

**下推到 Segment 层**:
- 在 Segment 内部执行全文搜索
- 先通过倒排索引找到匹配文档ID
- 仅读取匹配的行数据
- 避免全表扫描

**性能优势**:
- 避免读取整个 Segment
- 减少网络传输
- 提高查询吞吐量

---

## 9. 配置与部署

### 9.1 单节点配置

**配置项**:
- `host`: 监听地址（默认 127.0.0.1）
- `user/password`: 认证信息
- `graphql_port`: GraphQL 服务端口（9567）
- `mysql_port`: MySQL 协议端口（3307）
- `es_port`: Elasticsearch 端口（9200）
- `engine.data_dir`: 数据存储目录
- `engine.persist_check_interval_secs`: 持久化检查间隔
- `engine.max_concurrent_persists`: 最大并发持久化数
- `log.level`: 日志级别（trace/debug/info/warn/error）
- `log.target`: 日志输出目标（console/file/both）

**启动方式**:
- 使用配置文件启动
- 支持命令行参数覆盖配置

### 9.2 集群配置

**集群配置项**:
- `cluster.cluster_id`: 集群唯一标识
- `cluster.node_id`: 节点唯一标识
- `cluster.listen_addr`: Gossip 监听地址
- `cluster.seed_nodes`: 种子节点列表（用于初始发现）
- `cluster.distributed.grpc_port`: gRPC 端口（可选，自动分配）
- `cluster.distributed.query_timeout_ms`: 查询超时时间

**多节点部署**:
- 每个节点使用独立的配置文件
- 第一个节点作为种子节点
- 其他节点通过 seed_nodes 加入集群
- 所有节点协议端口不能冲突

**集群管理**:
- 使用 `cluster_test.sh` 脚本管理集群
- 支持 start/stop/restart/logs 命令
- 自动分配端口避免冲突

### 9.3 Docker 部署

**容器化部署** (计划中):
- 多阶段构建：Builder 阶段 + Runtime 阶段
- 基础镜像：debian:bookworm-slim
- 暴露端口：9567（GraphQL）、3307（MySQL）、9200（ES）、52000+（gRPC）
- 支持环境变量覆盖配置
- 支持 Docker Compose 多节点部署

---

## 10. 技术栈

### 10.1 核心依赖

| 组件 | 版本 | 用途 |
|------|------|------|
| **Rust** | 1.75+ | 系统编程语言 |
| **DataFusion** | 51.0.0 | 查询引擎 |
| **Arrow** | 54.3.1 | 列式内存格式 |
| **Parquet** | 54.3.1 | 列式存储格式 |
| **Tonic** | 0.12 | gRPC 框架 |
| **Arrow Flight** | 54.3.1 | 数据传输协议 |
| **Chitchat** | 0.9 | Gossip 协议 |
| **Tokio** | 1.0 | 异步运行时 |
| **Poem** | 3.0 | Web 框架 (GraphQL) |
| **async-graphql** | 7.0 | GraphQL 服务器 |
| **msql-srv** | patched | MySQL 协议 |

### 10.2 存储与索引

| 组件 | 版本 | 用途 |
|------|------|------|
| **RoaringBitmap** | 0.11 | 删除位图 |
| **BloomFilter** | 3.0 | 布隆过滤器 |
| **mem_btree** | custom | 内存 B-Tree |
| **memmap2** | 0.9 | 内存映射文件 |

### 10.3 序列化与压缩

| 组件 | 版本 | 用途 |
|------|------|------|
| **serde** | 1.0 | 序列化框架 |
| **serde_json** | 1.0 | JSON 支持 |
| **zstd** | 0.13 | 压缩算法 |
| **byteorder** | 1.5 | 字节序处理 |

---

## 11. 性能特性

### 11.1 写入性能

**优化策略**:
- ✅ 批量插入 (Batch Insert)
- ✅ 异步持久化 (Async Persistence)
- ✅ 主键索引去重 (PK Deduplication)
- ✅ 分区并行写入 (Partition Parallelism)

**典型吞吐量** (单节点):
- 小数据 (100B/行): ~100K rows/s
- 大数据 (1KB/行): ~50K rows/s

### 11.2 查询性能

**优化策略**:
- ✅ 分区裁剪 (Partition Pruning)
- ✅ 列裁剪 (Column Pruning)
- ✅ 谓词下推 (Predicate Pushdown)
- ✅ 全文索引 (Fulltext Index)
- ✅ 并行扫描 (Parallel Scan)
- ✅ Arrow 零拷贝 (Zero-Copy)

**典型延迟**:
- 点查询 (主键): <1ms
- 范围查询 (单分区): 10-50ms
- 全表扫描 (10M 行): 1-5s
- 全文搜索: 50-200ms

### 11.3 内存管理

**策略**:
- 内存 Segment 达到阈值后自动持久化
- LRU 缓存淘汰冷数据
- 背压机制 (Backpressure) 防止 OOM

---

## 12. 未来规划

### 12.1 短期目标 (Q1 2025)

- [ ] **完善 ES 协议**: 支持 Aggregations
- [ ] **JOIN 支持**: 实现分布式 JOIN
- [ ] **事务支持**: 基于 MVCC 的事务
- [ ] **监控面板**: Grafana + Prometheus

### 12.2 中期目标 (Q2-Q3 2025)

- [ ] **自动扩缩容**: 基于负载的自动扩展
- [ ] **副本机制**: 多副本高可用
- [ ] **冷热分离**: S3 冷数据归档
- [ ] **SQL 优化器**: 基于统计信息的优化

### 12.3 长期目标 (Q4 2025+)

- [ ] **多租户隔离**: 资源配额和限流
- [ ] **流式计算**: 实时数据流处理
- [ ] **ML 集成**: 内置机器学习算法
- [ ] **Cloud Native**: Kubernetes Operator

---

## 13. 参考资料

### 13.1 项目文档

- [MCP Integration Guide](./mcp_integration.md)
- [GraphQL DDL Guide](./graphql_createtable_guide.md)
- [Distributed Architecture](./distributed_architecture_design.md)
- [Fulltext Index Design](./fulltext_unified_architecture.md)

### 13.2 技术博客

- [Engine 重构报告](../ENGINE_REFACTORING_REPORT.md)
- [Router 重构报告](../ROUTER_REFACTORING_COMPLETION.md)
- [统一 gRPC 报告](../UNIFIED_GRPC_COMPLETION_REPORT.md)

### 13.3 外部链接

- [DataFusion Documentation](https://arrow.apache.org/datafusion/)
- [Arrow Flight Protocol](https://arrow.apache.org/docs/format/Flight.html)
- [Chitchat Gossip Protocol](https://github.com/quickwit-oss/chitchat)

---

## 14. 贡献指南

### 14.1 开发环境

**环境准备**:
1. 安装 Rust 1.75+
2. 克隆项目仓库
3. 安装依赖并构建
4. 运行测试套件
5. 启动开发服务器

### 14.2 代码规范

- 使用 `rustfmt` 格式化代码
- 使用 `clippy` 进行代码检查
- 遵循 Rust API Guidelines
- 所有公共 API 必须有文档注释

### 14.3 提交规范

**提交格式**: `<type>(<scope>): <subject>`

**Type 类型**:
- `feat`: 新功能
- `fix`: Bug 修复
- `docs`: 文档更新
- `refactor`: 重构
- `perf`: 性能优化
- `test`: 测试相关

---

**文档维护**: 本文档由 Calm Team 维护，欢迎提交 PR 改进。
