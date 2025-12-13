# Calm 集群测试示例

本目录包含两节点集群的测试配置和脚本。

## 配置文件

### node1.toml - 种子节点
- **Node ID**: node1
- **GraphQL 端口**: 9567
- **Gossip 端口**: 7946 (UDP)
- **RPC 端口**: 7947 (TCP)
- **角色**: 种子节点（seed node）

### node2.toml - 加入节点
- **Node ID**: node2
- **GraphQL 端口**: 9568
- **Gossip 端口**: 7948 (UDP)
- **RPC 端口**: 7949 (TCP)
- **种子节点**: 127.0.0.1:7946（连接到 node1）

## 共享资源

两个节点共享以下目录：
- `./data` - 表数据存储
- `./logs` - 日志文件（node1.log, node2.log）

## 快速开始

### 使用管理脚本（推荐）

```bash
# 启动两个节点
./examples/cluster_test.sh start

# 查看状态
./examples/cluster_test.sh status

# 查看日志
./examples/cluster_test.sh logs

# 停止集群
./examples/cluster_test.sh stop

# 重启集群
./examples/cluster_test.sh restart

# 清理数据和日志
./examples/cluster_test.sh clean
```

### 手动启动

```bash
# 启动节点1
cargo run --bin calm -- --config examples/node1.toml

# 启动节点2（在另一个终端）
cargo run --bin calm -- --config examples/node2.toml
```

## 访问服务

- **节点1 GraphQL**: http://127.0.0.1:9567
- **节点2 GraphQL**: http://127.0.0.1:9568

## 配置说明

### 节点 1 配置
- **Node ID**: `node1`
- **Cluster ID**: `calm-test-cluster`
- **Gossip Port**: `7946` (UDP)
- **RPC Port**: `7947` (TCP)
- **Role**: 种子节点
- **Data Dir**: `./data` (共享)
- **Log File**: `./logs/node1.log`

### 节点 2 配置
- **Node ID**: `node2`
- **Cluster ID**: `calm-test-cluster`
- **Gossip Port**: `7948` (UDP)
- **RPC Port**: `7949` (TCP)
- **Seed Nodes**: `127.0.0.1:7946` (连接到节点1)
- **Data Dir**: `./data` (共享)
- **Log File**: `./logs/node2.log`

## 共享资源

两个节点共享以下目录：

- **数据目录**: `./data` - 存储表元数据和分区数据
- **日志目录**: `./logs` - 存储节点日志文件

## 测试场景

### 1. 集群发现测试
启动两个节点后，检查它们是否能相互发现：

```bash
# 查看节点1日志
tail -f logs/node1.log | grep "Live Nodes"

# 查看节点2日志
tail -f logs/node2.log | grep "Live Nodes"
```

应该看到每个节点都能看到2个存活节点。

### 2. DDL 操作测试
在节点1上创建表，在节点2上查询：

```bash
# TODO: 添加 GraphQL 客户端测试代码
```

### 3. 故障恢复测试
停止节点2，观察节点1的反应：

```bash
# 停止节点2
kill $(cat logs/node2.pid)

# 观察节点1日志
tail -f logs/node1.log
```

### 4. 选举测试
观察哪个节点被选为协调者（coordinator）：

```bash
# 查看日志中的选举信息
grep -i "coordinator\|election" logs/node*.log
```

## 日志级别

可以通过 `RUST_LOG` 环境变量调整日志级别：

```bash
# 仅显示警告和错误
RUST_LOG=warn cargo run --example cluster_node1

# 显示调试信息
RUST_LOG=debug cargo run --example cluster_node1

# 特定模块的日志
RUST_LOG=calm::cluster=debug,calm::service=info cargo run --example cluster_node1
```

## 故障排查

### 节点无法互相发现
1. 检查防火墙设置，确保 UDP 7946-7948 端口开放
2. 检查节点1是否完全启动（等待3秒）
3. 查看日志文件中的错误信息

### RPC 连接失败
1. 检查 TCP 7947-7949 端口是否开放
2. 确认节点的 internal_port 配置正确
3. 检查 gossip 是否正常工作（节点能互相发现）

### 数据不同步
1. 确认共享的 `data` 目录路径一致
2. 检查文件权限
3. 查看 DDL 操作的日志

## 清理

停止集群并清理所有数据：

```bash
# 停止集群
./examples/cluster_test.sh stop

# 删除日志
rm -rf logs/

# 删除数据（可选，会删除所有表数据）
rm -rf data/
```

## 扩展

要添加更多节点，可以：

1. 复制 `cluster_node2.rs` 为 `cluster_node3.rs`
2. 修改端口号（gossip_port: 7950, internal_port: 7951）
3. 保持 seed_nodes 指向节点1
4. 更新测试脚本添加新节点

## 注意事项

- 两个节点共享同一个数据目录，适合本地测试
- 生产环境中每个节点应该有独立的数据目录
- 确保端口不被其他程序占用
- 建议先启动节点1（种子节点），等待几秒后再启动节点2
