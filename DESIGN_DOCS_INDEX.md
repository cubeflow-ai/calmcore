# CalmCore 存储引擎设计文档

## 文档索引

本项目包含完整的设计文档，涵盖存储引擎的所有核心模块：

### 1. 总体架构

📄 **[ARCHITECTURE_SUMMARY.md](ARCHITECTURE_SUMMARY.md)** - 系统总览

- 整体架构设计
- 数据流分析
- 核心算法详解
- 性能基准测试
- 配置调优指南
- 最佳实践

### 2. 核心模块

#### 2.1 BTree 模块

📄 **[BTREE_DESIGN.md](BTREE_DESIGN.md)** - 自研 B+树实现

- 双模式设计（Memory/Disk）
- floor() 方法实现
- TreeWriter/TreeReader 持久化机制
- mmap 零拷贝读取
- 性能：1000万 ops/秒（内存），100万 ops/秒（磁盘）

#### 2.2 Field 字段索引模块

📄 **[FIELD_DESIGN.md](FIELD_DESIGN.md)** - 字段索引系统

- IndexWriter 接口设计
- Keyword 字段类型
- PkWriter 主键去重
- 倒排索引实现
- BloomFilter 优化
- 序列化：StringRoaringSerializer
- 性能：35万条/秒（含主键去重）

#### 2.3 RowData 行数据模块

📄 **[ROWDATA_DESIGN.md](ROWDATA_DESIGN.md)** - 行数据存储

- RowDataStore 双模式
- RecordBatch 组织
- Batch 重组算法（100条/batch）
- 删除标记处理
- U32RecordBatchSerializer
- get_document() 和 floor() 查询
- 性能：3 µs 点查询

#### 2.4 Segment 段管理模块

📄 **[SEGMENT_DESIGN.md](SEGMENT_DESIGN.md)** - 段生命周期管理

- 三状态转换（Active → Frozen → Persisted）
- 四阶段持久化流程
- Phase 4 内存释放（节省94%）
- max_doc_id 跟踪
- segment-{start}-{end} 命名规范
- load_frozen() 恢复机制
- 性能：36万条/秒写入，4 µs 查询

#### 2.5 Partition 分区管理模块

📄 **[PARTITION_DESIGN.md](PARTITION_DESIGN.md)** - 多段协调

- 多Segment管理
- 查询路由（Active + Frozen）
- Flush 策略（100万条/segment）
- 后台持久化
- 重启恢复（1.8ms）
- Segment 清理和合并
- 并发控制

### 3. 实现细节文档

📄 **[KEYWORD_PERSIST_IMPLEMENTATION.md](KEYWORD_PERSIST_IMPLEMENTATION.md)** - Keyword 字段持久化实现

📄 **[SEGMENT_PERSIST_IMPLEMENTATION.md](SEGMENT_PERSIST_IMPLEMENTATION.md)** - Segment 持久化详细实现

## 快速导航

### 按角色阅读

**架构师/技术经理：**

1. [ARCHITECTURE_SUMMARY.md](ARCHITECTURE_SUMMARY.md) - 了解整体设计
2. [SEGMENT_DESIGN.md](SEGMENT_DESIGN.md) - 核心组件
3. [PARTITION_DESIGN.md](PARTITION_DESIGN.md) - 顶层架构

**后端开发工程师：**

1. [BTREE_DESIGN.md](BTREE_DESIGN.md) - 理解存储基础
2. [FIELD_DESIGN.md](FIELD_DESIGN.md) - 索引实现
3. [ROWDATA_DESIGN.md](ROWDATA_DESIGN.md) - 数据组织
4. [SEGMENT_DESIGN.md](SEGMENT_DESIGN.md) - 生命周期管理

**性能优化工程师：**

1. [ARCHITECTURE_SUMMARY.md](ARCHITECTURE_SUMMARY.md) §7 性能基准
2. [BTREE_DESIGN.md](BTREE_DESIGN.md) §8 性能优化
3. [FIELD_DESIGN.md](FIELD_DESIGN.md) §9 性能优化
4. [SEGMENT_DESIGN.md](SEGMENT_DESIGN.md) §14 性能优化

### 按主题阅读

**写入流程：**

- [ARCHITECTURE_SUMMARY.md](ARCHITECTURE_SUMMARY.md) §3.1 写入流程
- [SEGMENT_DESIGN.md](SEGMENT_DESIGN.md) §4 写入流程
- [FIELD_DESIGN.md](FIELD_DESIGN.md) §6 写入与去重

**查询流程：**

- [ARCHITECTURE_SUMMARY.md](ARCHITECTURE_SUMMARY.md) §3.2 查询流程
- [SEGMENT_DESIGN.md](SEGMENT_DESIGN.md) §7 查询流程
- [ROWDATA_DESIGN.md](ROWDATA_DESIGN.md) §5 查询机制

**持久化机制：**

- [SEGMENT_DESIGN.md](SEGMENT_DESIGN.md) §5 持久化流程
- [ROWDATA_DESIGN.md](ROWDATA_DESIGN.md) §6 持久化实现
- [FIELD_DESIGN.md](FIELD_DESIGN.md) §7 持久化

**内存管理：**

- [ARCHITECTURE_SUMMARY.md](ARCHITECTURE_SUMMARY.md) §6 内存管理
- [SEGMENT_DESIGN.md](SEGMENT_DESIGN.md) §9 内存管理
- [PARTITION_DESIGN.md](PARTITION_DESIGN.md) §8 内存管理

## 关键指标

### 性能指标（300万条记录，3个segment）

| 指标 | 值 |
|------|---|
| 写入吞吐 | 36.7万条/秒 |
| 点查询延迟 | 4.1 µs (P50) |
| Term查询延迟 | 8.5 µs (P50) |
| 持久化时间 | 13.9s/百万条 |
| 冷启动时间 | 1.8 ms |
| 内存占用 | 35 MB/百万条（活跃），2 MB/百万条（冻结） |
| 磁盘占用 | 18.2 bytes/条 |

### 架构特点

- ✅ 自研 B+Tree：双模式，floor() 快速定位
- ✅ Phase 4 优化：自动释放94%内存
- ✅ BloomFilter 去重：99.99%准确率
- ✅ mmap 零拷贝：减少内存拷贝
- ✅ Arrow 兼容：与大数据生态集成
- ✅ 逻辑删除：保持索引连续性
- ✅ 快速恢复：延迟加载，毫秒级启动

## 开发状态

**已完成：**

- ✅ 核心存储引擎（BTree, Segment, Partition）
- ✅ 字段索引系统（Keyword, 倒排索引）
- ✅ 主键去重（PkWriter, BloomFilter）
- ✅ 持久化机制（四阶段，Phase 4内存释放）
- ✅ 查询系统（点查询，term查询）
- ✅ 逻辑删除
- ✅ 重启恢复
- ✅ 性能测试（300万条多Segment测试）
- ✅ 完整文档（2500+ 行）

**进行中：**

- 🔄 WAL 日志（防止数据丢失）
- 🔄 查询缓存（LRU）
- 🔄 Compaction（自动合并）

**计划中：**

- ⏳ 列式存储（OLAP支持）
- ⏳ 分层存储（SSD + HDD）
- ⏳ 向量搜索
- ⏳ 分布式扩展

## 贡献指南

欢迎贡献代码或文档！

1. Fork 本项目
2. 创建特性分支
3. 提交代码变更
4. 更新相关文档
5. 提交 Pull Request

## 许可证

[待添加]

## 联系方式

[待添加]

---

**文档生成时间：** 2024年

**文档版本：** v1.0

**更新说明：** 完整的架构设计和实现文档
