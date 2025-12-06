# Requirements Document

## Introduction

本文档定义了 CalmDB 分布式查询系统的需求。该系统在现有单机查询引擎基础上，新增分布式查询能力，支持跨节点的数据扫描和聚合操作。核心设计原则是**零侵入**：单机模式的代码路径和性能完全不受影响。

## Glossary

- **Coordinator**: 接收客户端查询请求的节点，负责查询计划分发和结果合并
- **Worker**: 执行查询计划的节点，可以是 Coordinator 本身或其他集群节点
- **Shuffle**: 按 Hash 重新分发数据的过程，确保相同 key 的数据在同一节点
- **Partition**: 表的逻辑分区，每个 Partition 有一个 write_node
- **Local Partition**: 当前节点负责的 Partition
- **Remote Partition**: 其他节点负责的 Partition
- **Scatter-Gather**: 分发查询到多个节点，收集结果的模式
- **Two-Phase Aggregation**: 两阶段聚合，先 Partial 后 Final

## Requirements

### Requirement 1: 单机模式零影响

**User Story:** 作为现有用户，我希望升级后单机模式的性能和行为完全不变，不需要任何配置修改。

#### Acceptance Criteria

1. WHEN 集群模式未启用 (standalone mode), THE DistributedExecutor SHALL 直接调用现有的 DataFusionExecutor
2. WHEN 执行单机查询, THE system SHALL 使用与升级前完全相同的代码路径
3. WHEN 运行单机 benchmark, THE system SHALL 保持与升级前相同的性能 (误差 < 5%)
4. WHEN 单机模式运行, THE system SHALL NOT 加载任何分布式相关的网络连接

### Requirement 2: 分布式查询入口

**User Story:** 作为开发者，我希望有一个统一的查询入口，自动判断使用单机还是分布式执行。

#### Acceptance Criteria

1. WHEN 客户端提交 SQL 查询, THE QueryCoordinator SHALL 判断当前是单机还是集群模式
2. WHEN 集群模式启用, THE QueryCoordinator SHALL 根据表的 Partition 分布决定执行策略
3. WHEN 所有相关 Partition 都在本地, THE QueryCoordinator SHALL 使用本地执行路径
4. WHEN 存在远程 Partition, THE QueryCoordinator SHALL 使用分布式执行路径

### Requirement 3: 节点间通信

**User Story:** 作为系统，我需要在节点间传输查询请求和数据结果。

#### Acceptance Criteria

1. WHEN 分布式查询启动, THE NodeClient SHALL 与相关 Worker 节点建立连接
2. WHEN 发送查询请求, THE NodeClient SHALL 使用 gRPC 或 HTTP 传输 SQL 和执行参数
3. WHEN 接收查询结果, THE NodeClient SHALL 以 Arrow IPC 格式流式接收 RecordBatch
4. WHEN 连接失败, THE NodeClient SHALL 返回错误并标记节点不可用
5. WHEN 节点恢复, THE NodeClient SHALL 自动重新建立连接

### Requirement 4: 简单查询分布式执行 (无 GROUP BY)

**User Story:** 作为用户，我希望 SELECT/WHERE/LIMIT 查询能够自动分布式执行。

#### Acceptance Criteria

1. WHEN 执行无 GROUP BY 的 SELECT 查询, THE DistributedExecutor SHALL 并行发送查询到所有相关节点
2. WHEN 各节点返回结果, THE DistributedExecutor SHALL 流式合并 (UNION) 所有结果
3. WHEN 查询包含 LIMIT, THE DistributedExecutor SHALL 将 LIMIT 下推到各节点
4. WHEN 查询包含 WHERE, THE DistributedExecutor SHALL 将 WHERE 条件下推到各节点

### Requirement 5: 聚合查询分布式执行 (有 GROUP BY)

**User Story:** 作为用户，我希望 GROUP BY 查询能够正确分布式执行，包括 DISTINCT 操作。

#### Acceptance Criteria

1. WHEN 执行 GROUP BY 查询, THE DistributedExecutor SHALL 提取 GROUP BY 字段
2. WHEN 扫描数据, THE ShuffleExec SHALL 按 GROUP BY 字段的 Hash 值分发数据
3. WHEN Hash 值属于本节点, THE ShuffleExec SHALL 将数据发送到本地 channel
4. WHEN Hash 值属于其他节点, THE ShuffleExec SHALL 将数据发送到对应节点
5. WHEN 所有数据 Shuffle 完成, THE DistributedExecutor SHALL 在各节点本地执行聚合
6. WHEN 执行 COUNT(DISTINCT), THE system SHALL 因为 Shuffle 保证同一 key 在同一节点而返回正确结果

### Requirement 6: Shuffle 数据交换

**User Story:** 作为系统，我需要在 Scan 阶段按 Hash 重新分发数据。

#### Acceptance Criteria

1. WHEN ShuffleExec 执行, THE system SHALL 为每个目标节点创建发送 channel
2. WHEN 读取一行数据, THE ShuffleExec SHALL 计算 GROUP BY 字段的 Hash 值
3. WHEN Hash 值确定目标节点, THE ShuffleExec SHALL 将该行发送到对应 channel
4. WHEN 本地 channel 收到数据, THE ShuffleExec SHALL 直接传递给本地聚合
5. WHEN 远程 channel 收到数据, THE ShuffleExec SHALL 通过网络发送到目标节点
6. WHEN 所有 Partition 扫描完成, THE ShuffleExec SHALL 发送 EOF 信号

### Requirement 7: 查询结果合并

**User Story:** 作为 Coordinator，我需要合并各节点的查询结果。

#### Acceptance Criteria

1. WHEN 无 GROUP BY 查询完成, THE Coordinator SHALL 直接 UNION 所有节点结果
2. WHEN 有 GROUP BY 查询完成, THE Coordinator SHALL 直接 UNION 所有节点结果 (因为 Shuffle 保证不重复)
3. WHEN 查询包含 ORDER BY, THE Coordinator SHALL 对合并后的结果排序
4. WHEN 查询包含全局 LIMIT, THE Coordinator SHALL 在合并后应用 LIMIT

### Requirement 8: 错误处理

**User Story:** 作为用户，我希望分布式查询的错误能够被正确处理和报告。

#### Acceptance Criteria

1. WHEN 任一节点查询失败, THE Coordinator SHALL 取消其他节点的查询
2. WHEN 节点不可达, THE Coordinator SHALL 返回包含节点信息的错误
3. WHEN 查询超时, THE Coordinator SHALL 返回超时错误
4. WHEN Shuffle 过程中节点失败, THE system SHALL 返回 Shuffle 失败错误

### Requirement 9: 复用现有组件

**User Story:** 作为开发者，我希望分布式模块能够复用现有的优化组件。

#### Acceptance Criteria

1. WHEN 扫描本地 Partition, THE DistributedExecutor SHALL 复用现有的 SegmentScanner
2. WHEN 应用过滤条件, THE DistributedExecutor SHALL 复用现有的索引优化
3. WHEN 执行本地聚合, THE DistributedExecutor SHALL 复用 DataFusion 的聚合能力
4. WHEN 获取 Partition 信息, THE DistributedExecutor SHALL 复用 PartitionManager

### Requirement 10: 配置管理

**User Story:** 作为运维人员，我希望能够配置分布式查询的参数。

#### Acceptance Criteria

1. WHEN 配置文件包含 distributed 段, THE system SHALL 读取分布式查询配置
2. WHEN 设置 query_timeout, THE system SHALL 在超时后取消查询
3. WHEN 设置 shuffle_buffer_size, THE system SHALL 使用指定的缓冲区大小
4. WHEN 设置 max_concurrent_queries, THE system SHALL 限制并发查询数

