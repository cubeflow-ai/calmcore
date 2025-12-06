# CalmDB Docker Cluster Deployment

本目录包含 CalmDB 集群的 Docker 部署配置。

## 快速开始

### 1. 构建镜像并启动集群

```bash
# 一键启动 8 节点集群
./cluster.sh up

# 或者分步执行
./cluster.sh build   # 构建镜像
./cluster.sh up      # 启动集群
```

### 2. 查看集群状态

```bash
./cluster.sh status
./cluster.sh health
```

### 3. 连接数据库

```bash
# MySQL 连接
mysql -h 127.0.0.1 -P 3307 -u root -pcalm

# 或使用脚本
./cluster.sh mysql
```

### 4. 停止集群

```bash
./cluster.sh down
```

### 5. 清理数据

```bash
# 停止集群并删除所有数据
./cluster.sh clean
```

## 文件说明

| 文件 | 说明 |
|------|------|
| `Dockerfile` | Docker 镜像构建文件 |
| `docker-compose.yml` | 8 节点集群配置 |
| `cluster.sh` | 集群管理脚本 |
| `Makefile` | Make 命令封装 |

## 集群管理命令

```bash
./cluster.sh <command>

Commands:
  build       构建 Docker 镜像
  up          启动集群（8 节点）
  down        停止集群
  restart     重启集群
  status      查看集群状态
  logs        查看日志 (logs [node-name])
  clean       停止集群并删除所有数据
  clean-data  仅删除数据卷
  scale       扩缩容 (scale <num>)
  exec        在容器中执行命令
  mysql       连接 MySQL
  health      检查集群健康状态
```

## 端口映射

### 8 节点集群端口

| 节点 | GraphQL | Elasticsearch | MySQL |
|------|---------|---------------|-------|
| node-1 | 9567 | 9200 | 3307 |
| node-2 | 9568 | 9201 | 3308 |
| node-3 | 9569 | 9202 | 3309 |
| node-4 | 9570 | 9203 | 3310 |
| node-5 | 9571 | 9204 | 3311 |
| node-6 | 9572 | 9205 | 3312 |
| node-7 | 9573 | 9206 | 3313 |
| node-8 | 9574 | 9207 | 3314 |

### 内部端口

| 端口 | 协议 | 说明 |
|------|------|------|
| 7946 | UDP/TCP | Gossip 通信 |
| 7947 | TCP | RPC 通信 |

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `CALM_HOST` | 0.0.0.0 | 监听地址 |
| `CALM_NODE_ID` | auto | 节点 ID |
| `CALM_CLUSTER_ID` | calm-cluster | 集群 ID |
| `CALM_CLUSTER_ENABLED` | false | 启用集群模式 |
| `CALM_GOSSIP_ADDR` | 0.0.0.0:7946 | Gossip 监听地址 |
| `CALM_SEED_NODES` | - | 种子节点列表（逗号分隔） |
| `CALM_DATA_DIR` | /data | 数据目录 |
| `CALM_LOG_LEVEL` | info | 日志级别 |
| `CALM_LOG_FILE` | /logs/calm.log | 日志文件 |
| `CALM_GRAPHQL_PORT` | 9567 | GraphQL 端口 |
| `CALM_ES_PORT` | 9200 | Elasticsearch 端口 |
| `CALM_MYSQL_PORT` | 3307 | MySQL 端口 |

## 故障排除

### 查看节点日志

```bash
# 查看所有节点日志
./cluster.sh logs

# 查看特定节点日志
./cluster.sh logs calm-node-1
```

### 进入容器调试

```bash
./cluster.sh exec calm-node-1 /bin/bash
```

### 检查网络连通性

```bash
# 进入容器
./cluster.sh exec calm-node-1 /bin/bash

# 测试与其他节点的连接
curl http://calm-node-2:9567/
```

### 重置集群

```bash
# 完全重置（删除所有数据）
./cluster.sh clean

# 重新启动
./cluster.sh up
```

## 生产部署建议

1. **资源限制**: 在 docker-compose.yml 中添加资源限制
2. **持久化存储**: 使用外部存储卷或绑定挂载
3. **网络安全**: 配置防火墙规则，限制 Gossip 端口访问
4. **监控**: 集成 Prometheus/Grafana 监控
5. **备份**: 定期备份数据卷

```yaml
# 资源限制示例
services:
  calm-node-1:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G
```
