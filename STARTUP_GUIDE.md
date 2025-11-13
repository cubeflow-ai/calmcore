# Calm Database - 启动指南

Calm 是一个多协议数据库服务器，同时支持 GraphQL、Elasticsearch 和 MySQL 协议。

## 快速开始

### 1. 使用默认配置启动所有服务

```bash
cargo run --bin calm --release
```

这将启动：
- GraphQL 服务: `http://127.0.0.1:8080`
- Elasticsearch 服务: `http://127.0.0.1:9200`
- MySQL 服务: `127.0.0.1:3307`

### 2. 自定义端口

```bash
# 自定义 GraphQL 端口
cargo run --bin calm --release -- --graphql-port 9000

# 自定义所有端口
cargo run --bin calm --release -- --graphql-port 9000 --es-port 9300 --mysql-port 3308
```

### 3. 只启动特定服务

```bash
# 只启动 GraphQL 服务
cargo run --bin calm --release -- --no-es --no-mysql

# 只启动 Elasticsearch 服务
cargo run --bin calm --release -- --no-graphql --no-mysql

# 只启动 MySQL 服务
cargo run --bin calm --release -- --no-graphql --no-es
```

### 4. 使用配置文件

```bash
# TOML 格式
cargo run --bin calm --release -- --config config.toml

# JSON 格式
cargo run --bin calm --release -- --config config.json
```

配置文件示例见 `config.example.toml` 或 `config.example.json`。

### 5. 自定义数据目录

```bash
cargo run --bin calm --release -- --data-dir /path/to/data
```

## 命令行参数

```
OPTIONS:
    -h, --host <HOST>              监听地址 [default: 127.0.0.1]
    --graphql-port <PORT>          GraphQL 服务端口 [default: 8080]
    --es-port <PORT>               Elasticsearch 服务端口 [default: 9200]
    --mysql-port <PORT>            MySQL 服务端口 [default: 3307]
    --data-dir <DIR>               数据目录 [default: ./data]
    -c, --config <FILE>            配置文件路径 (.toml 或 .json)
    --no-graphql                   禁用 GraphQL 服务
    --no-es                        禁用 Elasticsearch 服务
    --no-mysql                     禁用 MySQL 服务
    --help                         显示帮助信息
```

## 环境变量

你也可以通过环境变量配置服务器：

```bash
export CALM_HOST=0.0.0.0
export CALM_GRAPHQL_PORT=9000
export CALM_ES_PORT=9300
export CALM_MYSQL_PORT=3308
export CALM_DATA_DIR=/var/lib/calm

cargo run --bin calm --release
```

环境变量的优先级低于命令行参数。

## 使用示例

### GraphQL

启动后访问 GraphQL Playground：

```
http://127.0.0.1:8080
```

示例查询：

```graphql
{
  label_event_v1 {
    id
    name
    updateTime
  }
}
```

### Elasticsearch

健康检查：

```bash
curl http://127.0.0.1:9200/_cluster/health
```

搜索数据：

```bash
curl -X GET "http://127.0.0.1:9200/label_event_v1/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "range": {
      "updateTime": {
        "gte": "2025-01-11",
        "lte": "2025-01-12"
      }
    }
  }
}'
```

### MySQL

连接数据库：

```bash
mysql -h 127.0.0.1 -P 3307 -u root
```

示例 SQL：

```sql
SHOW TABLES;
SELECT * FROM label_event_v1;
SELECT * FROM label_event_v1 WHERE updateTime >= 1736553600000;
```

## 开发模式

开发时使用非优化构建以加快编译速度：

```bash
cargo run --bin calm
```

## 生产部署

生产环境建议：

1. 使用 release 构建：
   ```bash
   cargo build --bin calm --release
   ```

2. 创建配置文件：
   ```bash
   cp config.example.toml /etc/calm/config.toml
   # 编辑配置文件
   ```

3. 运行：
   ```bash
   ./target/release/calm --config /etc/calm/config.toml
   ```

4. 使用 systemd 管理服务（Linux）或其他进程管理器。

## 故障排查

### 查看日志

启用详细日志：

```bash
RUST_LOG=debug cargo run --bin calm --release
```

### 端口被占用

如果默认端口被占用，使用自定义端口：

```bash
cargo run --bin calm --release -- --graphql-port 8081 --es-port 9201 --mysql-port 3308
```

### 数据目录权限

确保程序对数据目录有读写权限：

```bash
mkdir -p ./data
chmod 755 ./data
```

## 性能优化

1. **数据目录**：使用 SSD 存储以获得更好的性能
2. **持久化间隔**：根据数据写入频率调整 `persist_check_interval_secs`
3. **并发持久化**：增加 `max_concurrent_persists` 以利用多核 CPU

## 更多信息

- [架构文档](ARCHITECTURE_SUMMARY.md)
- [设计文档索引](DESIGN_DOCS_INDEX.md)
- [问题报告](https://github.com/cubeflow-ai/calmcore/issues)
