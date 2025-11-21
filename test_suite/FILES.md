# 文件说明

## 📄 文件列表

| 文件 | 用途 | 使用频率 |
|------|------|---------|
| `QUICKSTART.md` | 5分钟快速开始指南 | ⭐⭐⭐ 新手必读 |
| `README.md` | 完整文档和参考 | ⭐⭐ 详细信息 |
| `download_dataset.sh` | 下载 NYC Taxi 数据集 | ⭐⭐⭐ 首次使用 |
| `start_server.py` | 启动 Calm 测试服务器 | ⭐⭐⭐ 每次测试 |
| `load_dataset.py` | 加载数据集到数据库 | ⭐⭐⭐ 首次使用 |
| `test_queries.py` | 执行 SQL 测试用例 | ⭐⭐⭐ 每次测试 |
| `test_insert.py` | 测试 INSERT 功能 | ⭐⭐ 故障排查 |
| `TROUBLESHOOTING.md` | 故障排查指南 | ⭐⭐ 遇到问题时 |
| `FILES.md` | 本文件 | ⭐ 参考 |

## 🔄 典型工作流程

### 首次使用

```bash
# 1. 下载数据集（只需一次）
./download_dataset.sh nyc-taxi small

# 2. 启动服务器（终端1）
python3 start_server.py

# 3. 加载数据（终端2，只需一次）
python3 load_dataset.py nyc-taxi --limit 100000

# 4. 运行测试
python3 test_queries.py nyc-taxi
```

### 日常测试

如果数据已加载，只需：

```bash
# 1. 启动服务器（终端1）
python3 start_server.py

# 2. 运行测试（终端2）
python3 test_queries.py nyc-taxi
```

### 重新开始

清理所有数据，从头开始：

```bash
# 清理测试数据
rm -rf test_data datasets

# 然后按"首次使用"流程操作
```

## 📝 文件详细说明

### download_dataset.sh

**功能**: 从公开源下载数据集

**支持的数据集**:
- `nyc-taxi` - NYC 出租车数据（推荐）

**大小选项**:
- `small` - ~100万条记录（~100MB）
- `medium` - ~300万条记录（~300MB）
- `large` - ~1200万条记录（~1.2GB）

**示例**:
```bash
./download_dataset.sh nyc-taxi small
./download_dataset.sh nyc-taxi medium
```

### start_server.py

**功能**: 启动 Calm 数据库服务器

**参数**:
- `--data-dir DIR` - 数据目录（默认: `./test_data`）
- `--mysql-port PORT` - MySQL 端口（默认: `3306`）
- `--es-port PORT` - Elasticsearch 端口（默认: `9200`）
- `--log-level LEVEL` - 日志级别（默认: `info`）

**示例**:
```bash
# 默认配置
python3 start_server.py

# 自定义配置
python3 start_server.py --data-dir ./my_data --mysql-port 3307 --log-level debug
```

**注意**: 
- 服务器在前台运行，按 Ctrl+C 停止
- 首次运行会自动编译项目

### load_dataset.py

**功能**: 加载数据集到数据库

**参数**:
- `dataset` - 数据集名称（`nyc-taxi`）
- `--limit N` - 限制加载的记录数
- `--host HOST` - 数据库主机（默认: `127.0.0.1`）
- `--port PORT` - 数据库端口（默认: `3306`）

**示例**:
```bash
# 加载 10 万条记录
python3 load_dataset.py nyc-taxi --limit 100000

# 加载全部数据
python3 load_dataset.py nyc-taxi

# 连接到自定义端口
python3 load_dataset.py nyc-taxi --port 3307 --limit 50000
```

**注意**:
- 需要先下载数据集
- 需要服务器正在运行
- 加载时间取决于数据量（10万条约1-2分钟）

### test_queries.py

**功能**: 执行 SQL 测试用例并显示性能指标

**参数**:
- `dataset` - 数据集名称（`nyc-taxi`）
- `--host HOST` - 数据库主机（默认: `127.0.0.1`）
- `--port PORT` - 数据库端口（默认: `3306`）

**测试内容**:
- 12 个 SQL 查询
- 覆盖: SELECT, WHERE, ORDER BY, LIMIT, COUNT, AVG, GROUP BY
- 显示每个查询的执行时间和返回行数
- 显示前几行数据预览

**示例**:
```bash
# 运行测试
python3 test_queries.py nyc-taxi

# 连接到自定义端口
python3 test_queries.py nyc-taxi --port 3307
```

**输出**:
- 每个查询的成功/失败状态
- 执行时间（秒）
- 返回行数
- 数据预览
- 性能总结

## 🎯 快速参考

### 我想...

**...快速开始测试**
→ 阅读 `QUICKSTART.md`

**...了解详细信息**
→ 阅读 `README.md`

**...下载更多数据**
→ `./download_dataset.sh nyc-taxi medium`

**...测试特定查询**
→ 编辑 `test_queries.py`，添加自定义测试用例

**...查看服务器日志**
→ 服务器输出会显示在终端，或使用 `--log-level debug`

**...清理测试数据**
→ `rm -rf test_data datasets`

**...使用不同端口**
→ 所有脚本都支持 `--port` 参数

## 🐛 故障排查

### 找不到数据集文件

```bash
# 检查是否已下载
ls -lh datasets/

# 重新下载
./download_dataset.sh nyc-taxi small
```

### 服务器启动失败

```bash
# 检查端口是否被占用
lsof -i :3306
lsof -i :9200

# 使用不同端口
python3 start_server.py --mysql-port 3307 --es-port 9201
```

### 连接失败

```bash
# 确保服务器正在运行
ps aux | grep calm

# 检查端口
netstat -an | grep 3306
```

### 查询很慢

- 检查数据量（可能需要减少 `--limit`）
- 查看服务器日志确认索引被使用
- 尝试 `--log-level debug` 查看详细信息

## 📚 更多资源

- NYC Taxi 数据集: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Calm 项目文档: 查看项目根目录的 `*.md` 文件
- MySQL 协议: `../MYSQL_QUICKSTART.md`
- Elasticsearch API: `../ELASTICSEARCH_API.md`
