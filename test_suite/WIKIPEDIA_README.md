# Wikipedia 全文检索测试

这是一个使用 Wikipedia 文章数据集测试 CalmCore 全文检索功能的完整示例。

## 📋 数据集介绍

- **来源**: Hugging Face Datasets - Wikipedia (English, 2022-03-01 dump)
- **内容**: Wikipedia 英文文章的标题和摘要文本
- **规模**: 
  - Small: 10,000 篇文章 (~50MB)
  - Medium: 50,000 篇文章 (~250MB)
  - Large: 100,000 篇文章 (~500MB)

## 🚀 快速开始

### 1. 下载数据集

```bash
# 下载小型数据集（推荐用于测试）
./download_wikipedia.sh small

# 或下载中型数据集
./download_wikipedia.sh medium

# 或下载大型数据集
./download_wikipedia.sh large
```

**注意**: 首次下载需要安装 `datasets` 库：
```bash
pip3 install datasets
```

### 2. 启动 CalmCore 服务器

```bash
cd ..
cargo run --bin calm --release
```

服务器将在 `localhost:3307` 监听 MySQL 协议连接。

### 3. 加载数据

```bash
python3 load_wikipedia.py
```

加载过程会：
- 创建 `wikipedia_articles` 表
- 批量插入文章数据
- 每 100,000 条记录自动持久化
- 显示加载进度和性能统计

### 4. 测试搜索

```bash
# 运行预设测试查询
python3 test_wikipedia.py

# 或使用交互式搜索
python3 test_wikipedia.py interactive
```

## 📊 表结构

```sql
CREATE TABLE wikipedia_articles (
    id VARCHAR PRIMARY KEY,
    title TEXT,
    content TEXT FULLTEXT,    -- 全文索引字段
    url VARCHAR,
    timestamp TIMESTAMP
) ENGINE=MemBTree PARTITION=HASH(id, 4)
```

## 🔍 查询示例

### 1. 关键词搜索

```sql
SELECT id, title, _score 
FROM wikipedia_articles 
WHERE content = text('artificial intelligence', 1.0) 
ORDER BY _score DESC 
LIMIT 10;
```

### 2. 短语搜索

```sql
SELECT id, title, _score 
FROM wikipedia_articles 
WHERE content = phrase('machine learning', 1.0, 0) 
ORDER BY _score DESC 
LIMIT 10;
```

### 3. 多条件组合

```sql
SELECT id, title, _score 
FROM wikipedia_articles 
WHERE content = text('quantum physics', 1.0) 
  AND title LIKE '%Quantum%'
ORDER BY _score DESC 
LIMIT 10;
```

## 📈 性能特点

- **索引构建**: ~4.6M terms/sec（并行 Row Group 加载）
- **查询速度**: 批量查询 ~217µs/term
- **存储优化**: Parquet 列式存储 + Row Group 统计
- **并行处理**: 使用 rayon 并行加载多个 Row Groups

## 🧪 测试场景

`test_wikipedia.py` 包含以下测试：

1. **人工智能** - `artificial intelligence`
2. **量子物理** - `quantum physics`
3. **机器学习** - `machine learning` (短语搜索)
4. **Python 编程** - `python programming`
5. **气候变化** - `climate change`
6. **世界大战** - `world war`
7. **计算机科学** - `computer science`

## 💡 提示

### 交互式搜索模式

```bash
python3 test_wikipedia.py interactive
```

然后输入任何搜索词：
```
🔍 Search: quantum mechanics
🔍 Search: Albert Einstein
🔍 Search: neural networks
```

### 查看数据统计

```bash
mysql -h localhost -P 3307 -u root -e "SELECT COUNT(*) FROM wikipedia_articles"
```

### 手动持久化

```bash
mysql -h localhost -P 3307 -u root -e "FLUSH TABLES"
```

## 🔧 故障排除

### 连接失败

确保 CalmCore 服务器正在运行：
```bash
cargo run --bin calm --release
```

### 数据加载失败

检查数据文件是否存在：
```bash
ls -lh datasets/wikipedia/wikipedia_articles.jsonl
```

### 依赖缺失

安装所需的 Python 包：
```bash
pip3 install pymysql datasets
```

## 📚 相关文档

- [全文检索 SQL 语法](../docs/fulltext_sql_syntax.md)
- [Parquet 并行加载](../docs/parquet_parallel_loading.md)
- [性能优化报告](../docs/parquet_performance_report.md)
