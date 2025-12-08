# CalmCore 双节点集群配置

## 目录结构

```
deploy/cluster/
├── node1.toml          # Node 1 配置文件
├── node2.toml          # Node 2 配置文件
├── start_cluster.sh    # 启动集群脚本
├── stop_cluster.sh     # 停止集群脚本
└── README.md           # 本文件
```

## 快速开始

### 1. 启动集群

```bash
cd deploy/cluster
chmod +x start_cluster.sh stop_cluster.sh
./start_cluster.sh
```

集群将启动两个节点：
- **Node 1** (Seed Node)
  - GraphQL: http://localhost:9567
  - Elasticsearch: http://localhost:9200
  - MySQL: localhost:3307
  - Gossip: 127.0.0.1:7946
  
- **Node 2** (Seed Node)
  - GraphQL: http://localhost:9568
  - Elasticsearch: http://localhost:9201
  - MySQL: localhost:3308
  - Gossip: 127.0.0.1:7947

### 2. 检查集群状态

```bash
# 查看日志
tail -f logs/node1/calm.log
tail -f logs/node2/calm.log

# 测试连接
curl http://localhost:9567/
curl http://localhost:9568/
```

### 3. 停止集群

```bash
./stop_cluster.sh
```

## 配置说明

### 集群配置 (cluster)

- `enabled`: 是否启用集群模式
- `node_id`: 节点唯一标识
- `cluster_id`: 集群 ID（同一集群所有节点必须相同）
- `listen_addr`: Gossip 监听地址（UDP）
- `seed_nodes`: 种子节点列表（用于节点发现）
- `gossip_interval_ms`: Gossip 协议心跳间隔
- `failure_timeout_secs`: 故障检测超时
- `suspect_timeout_secs`: 可疑节点超时
- `vote_timeout_secs`: 投票超时
- `min_cluster_size`: 最小集群大小

### 服务端口 (server)

- `graphql_port`: GraphQL API 端口
- `es_port`: Elasticsearch 兼容 API 端口
- `mysql_port`: MySQL 协议端口
- `rpc_port`: 内部 RPC 端口

### 存储配置 (storage)

- `data_dir`: 数据存储目录
- `log_dir`: 日志目录

## 网络拓扑

```
Node 1 (127.0.0.1:7946) ←→ Node 2 (127.0.0.1:7947)
    ↓                              ↓
Gossip Protocol (UDP)      Gossip Protocol (UDP)
    ↓                              ↓
共享集群状态                    共享集群状态
```

两个节点互为种子节点，通过 Gossip 协议保持集群状态同步。

## 故障转移测试

### 模拟 Node 1 故障

```bash
# 停止 Node 1
kill $(cat /tmp/calmcore_node1.pid)

# 观察 Node 2 日志，应该检测到 Node 1 故障
tail -f logs/node2/calm.log
```

### 模拟节点恢复

```bash
# 重启 Node 1
RUST_LOG=info cargo run --bin calm -- --config deploy/cluster/node1.toml &

# 观察集群重新同步
tail -f logs/node1/calm.log
```

## 生产环境部署

对于生产环境，建议：

1. 使用真实的 IP 地址而不是 127.0.0.1
2. 配置防火墙规则开放必要端口
3. 使用独立的物理/虚拟机运行每个节点
4. 配置至少 3 个种子节点以保证高可用
5. 启用 TLS/SSL 加密通信
6. 配置持久化存储和定期备份

## 故障排查

### 端口被占用

如果遇到端口冲突，编辑 `node1.toml` 或 `node2.toml` 修改端口：

```toml
[server]
graphql_port = 9567  # 改为其他可用端口
```

### 节点无法加入集群

1. 检查 `seed_nodes` 配置是否正确
2. 确认 Gossip 端口没有被防火墙阻止
3. 查看日志文件排查详细错误

### 日志级别调整

在启动脚本中修改 `RUST_LOG` 环境变量：

```bash
# 调试级别
RUST_LOG=debug cargo run --bin calm -- --config deploy/cluster/node1.toml

# 追踪级别（更详细）
RUST_LOG=trace cargo run --bin calm -- --config deploy/cluster/node1.toml
```
