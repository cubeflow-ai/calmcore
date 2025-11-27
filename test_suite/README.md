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
python3 load_dataset.py nyc-taxi 

# 保证数据持久化
flush tables taxi_trips;

# 4. 运行测试
python3 test_queries.py nyc-taxi
```

### 日常测试

如果数据已加载，只需：

```bash
# 1. 启动服务器（终端1）
项目根目录下执行 
cargo run --release

# 2. 运行测试（终端2）
python3 test_queries.py nyc-taxi
```

### 重新开始

清理所有数据，从头开始：

```bash
重新执行,他会把表删掉 后重新插入
python3 load_dataset.py nyc-taxi 

插入结束后你需要调用

# 然后按"首次使用"流程操作
```

对比测试
```bash
我们利用和 mysql进行对比

calm 的连接方式 是 
mysql -h 127.0.0.1 -P 3307 -u root -pcalm 
不需要指定数据库

mysql的连接方式

mysql -h 127.0.0.1 -P 3306 -u root -pansjsun calm_test
```

日志位置
```bash
日志位置存放于 logs/calm.log
```