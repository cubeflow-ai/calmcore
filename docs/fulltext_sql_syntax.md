# 全文搜索 SQL 语法设计（最新版本）

## 核心设计理念

**语法形式**：`field_name = function(query, params...)`

字段名在左边（比较左侧），查询函数在右边（比较右侧），这是更自然的 SQL 语法。

## SQL 语法

### 1. 文本查询 (Text Query)

**函数签名**:
```sql
text(query_text: String, boost: Float) -> Boolean
```

**使用方式**:
```sql
field_name = text(query_text, boost)
```

**参数**:
- `query_text`: 查询文本（会被分词为多个 term，使用 OR 组合）
- `boost`: 权重系数（默认 1.0）

**示例**:
```sql
-- 简单查询
SELECT * FROM docs WHERE content = text('rust programming', 1.0) ORDER BY _score DESC;

-- 多字段加权查询
SELECT * FROM articles 
WHERE title = text('database', 3.0)      -- title 权重 3x
   OR content = text('database', 1.0)    -- content 权重 1x
ORDER BY _score DESC;

-- 结合结构化查询
SELECT * FROM articles 
WHERE content = text('kubernetes', 1.0)
  AND category = 'devops'
  AND created_at > '2024-01-01'
ORDER BY _score DESC, created_at DESC;
```

### 2. 短语查询 (Phrase Query)

**函数签名**:
```sql
phrase(phrase_text: String, boost: Float, slop: Int) -> Boolean
```

**使用方式**:
```sql
field_name = phrase(phrase_text, boost, slop)
```

**参数**:
- `phrase_text`: 短语文本（词序重要）
- `boost`: 权重系数（默认 1.0）
- `slop`: 词间最大距离（0 = 精确短语）

**示例**:
```sql
-- 精确短语匹配
SELECT * FROM docs 
WHERE content = phrase('full text search', 1.0, 0) 
ORDER BY _score DESC;

-- 允许 2 个词的间隔
SELECT * FROM docs 
WHERE content = phrase('rust programming', 1.0, 2) 
ORDER BY _score DESC;
```

### 3. 虚拟列 _score

**类型**: `Float32`

**说明**: 
- 自动计算的相关性评分
- 基于 BM25 算法
- 支持 boost 权重
- 可用于 ORDER BY 和 SELECT

**示例**:
```sql
-- 返回评分
SELECT id, title, _score FROM articles 
WHERE content = text('machine learning', 1.0)
ORDER BY _score DESC 
LIMIT 10;

-- 多列排序
SELECT * FROM articles 
WHERE content = text('database', 1.0)
ORDER BY _score DESC, created_at DESC;
```

## 完整查询示例

### 示例 1: 简单全文搜索
```sql
SELECT * FROM articles 
WHERE content = text('rust programming', 1.0) 
ORDER BY _score DESC 
LIMIT 10;
```

**说明**: 
- 在 content 字段搜索 'rust' 或 'programming'
- 按相关性评分降序排序
- 返回前 10 条

### 示例 2: 多字段加权搜索
```sql
SELECT title, content, _score 
FROM articles 
WHERE title = text('machine learning', 3.0) 
   OR content = text('machine learning', 1.0)
ORDER BY _score DESC 
LIMIT 20;
```

**说明**:
- title 字段权重 3x（更重要）
- content 字段权重 1x（标准）
- 返回评分最高的 20 条

### 示例 3: 精确短语查询
```sql
SELECT * FROM docs 
WHERE content = phrase('full text search', 1.0, 0)
ORDER BY _score DESC;
```

**说明**:
- 查找精确短语 "full text search"
- slop=0 表示不允许词间隔
- 词序必须完全匹配

### 示例 4: 短语查询 + 间隔容忍
```sql
SELECT * FROM docs 
WHERE content = phrase('rust programming', 1.0, 2)
ORDER BY _score DESC;
```

**说明**:
- 允许 'rust' 和 'programming' 之间最多 2 个词的间隔
- 例如可以匹配 "rust systems programming" (间隔 1 词)

### 示例 5: 布尔组合查询
```sql
SELECT * FROM docs 
WHERE (content = text('rust', 1.0) AND content = text('async', 1.0))
   OR (content = text('python', 1.0) AND content = text('asyncio', 1.0))
ORDER BY _score DESC;
```

**说明**:
- (rust AND async) OR (python AND asyncio)
- 标准 SQL 布尔逻辑
- 每个条件都会累加评分

### 示例 6: 混合查询（全文 + 结构化）
```sql
SELECT * FROM articles 
WHERE content = text('microservices', 1.0)
  AND category IN ('architecture', 'devops')
  AND author = 'John Doe'
  AND created_at > '2024-01-01'
  AND views > 1000
ORDER BY _score DESC, views DESC
LIMIT 50;
```

**说明**:
- 全文搜索 + 多个结构化过滤条件
- 多列排序：先按相关性，再按浏览量
- 展示了全文搜索与传统 SQL 的无缝集成

### 示例 7: 子查询 + 全文搜索
```sql
SELECT a.*, author_info.name 
FROM articles a
JOIN author_info ON a.author_id = author_info.id
WHERE a.content = text('distributed systems', 1.0)
  AND author_info.verified = true
ORDER BY a._score DESC;
```

**说明**:
- 全文搜索可以与 JOIN、子查询等标准 SQL 特性结合
- _score 使用表别名前缀

## 与 Elasticsearch 对比

| 功能 | Elasticsearch DSL | CalmCore SQL |
|------|-------------------|--------------|
| 文本查询 | `{"match": {"content": "rust"}}` | `content = text('rust', 1.0)` |
| 短语查询 | `{"match_phrase": {"content": {"query": "rust programming", "slop": 2}}}` | `content = phrase('rust programming', 1.0, 2)` |
| 布尔查询 | `{"bool": {"must": [...], "should": [...]}}` | `WHERE ... AND ... OR ...` |
| 权重 | `{"match": {"content": {"query": "rust", "boost": 2.0}}}` | `content = text('rust', 2.0)` |
| 评分 | `"sort": ["_score"]` | `ORDER BY _score DESC` |
| 过滤 | `{"bool": {"filter": [...]}}` | `AND category = 'tech'` |

**优势**:
- ✅ 标准 SQL 语法，无需学习 JSON DSL
- ✅ 与 SQL 特性无缝集成（JOIN、子查询、聚合）
- ✅ 更简洁直观
- ✅ 类型安全（编译时检查）
- ✅ 工具支持（SQL 客户端、IDE）

## 实现架构

### 语法解析流程

```
用户 SQL:
  WHERE content = text('rust', 1.0)
         ↓
DataFusion Parser:
  检测到比较表达式: content = ...
  右侧是 UDF 调用: text('rust', 1.0)
         ↓
逻辑计划:
  Filter(content = text_udf('rust', 1.0))
         ↓
物理执行:
  1. 从比较上下文提取字段名 'content'
  2. 查找 FullTextField 索引
  3. 执行 term_query('rust')
  4. 计算 BM25 评分
  5. 累加到 doc_scores
  6. 返回匹配的 doc_ids 布尔数组
         ↓
_score 注入:
  检测到 ORDER BY _score 或 SELECT _score
  从 doc_scores 获取评分
  添加为虚拟列
```

### UDF 注册方式

```rust
// 为每个字段创建专门的 UDF
let context = Arc::new(FullTextContext::new());
context.register_index("content".into(), content_index);

// 注册 UDFs
let text_content_udf = create_text_udf(context.clone(), "content".to_string());
let phrase_content_udf = create_phrase_udf(context.clone(), "content".to_string());

session.register_udf(text_content_udf);
session.register_udf(phrase_content_udf);

// SQL 解析时：
// content = text(...) -> 调用 text_content UDF
```

## BM25 评分算法

### 公式

```
score(D, Q) = Σ IDF(qi) × (f(qi, D) × (k1 + 1)) / (f(qi, D) + k1 × (1 - b + b × |D| / avgdl))

其中:
- qi: 查询中的第 i 个词
- f(qi, D): qi 在文档 D 中的词频
- |D|: 文档 D 的长度
- avgdl: 平均文档长度
- k1 = 1.2: 词频饱和参数
- b = 0.75: 长度归一化参数

IDF(qi) = log((N - df(qi) + 0.5) / (df(qi) + 0.5))

其中:
- N: 文档总数
- df(qi): 包含 qi 的文档数
```

### Boost 应用

```
final_score = base_score × boost
```

### 多字段评分

```sql
WHERE title = text('rust', 3.0) OR content = text('rust', 1.0)
```

- 每个匹配的字段独立计算 BM25
- 应用各自的 boost
- 累加到 doc_scores
- OR: 评分累加
- AND: 评分也累加（都匹配的文档评分更高）

## 实现计划

### Phase 1: 核心 UDF ✅
- [x] FullTextContext 结构
- [x] create_text_udf() 
- [x] create_phrase_udf()
- [x] 基础评分逻辑

### Phase 2: SQL 集成 (进行中)
- [ ] SQL 解析器修改
- [ ] 从比较表达式提取字段名
- [ ] UDF 注册到 SessionContext
- [ ] _score 列检测和注入
- [ ] 集成测试

### Phase 3: 评分优化
- [ ] 完整 BM25 实现
- [ ] TermStats 和 FieldStats 集成
- [ ] 评分缓存
- [ ] 并行评分

### Phase 4: 高级特性
- [ ] fuzzy() - 模糊查询
- [ ] wildcard() - 通配符
- [ ] regexp() - 正则表达式
- [ ] highlight() - 高亮
- [ ] 多语言分词器

## 性能考虑

### 索引预过滤

```rust
// 优化前：全表扫描 + 过滤
SELECT * FROM docs WHERE content = text('rust', 1.0);
// 扫描所有行，每行执行 UDF

// 优化后：索引先过滤
1. 执行全文查询获取 doc_id 位图
2. 将位图传给 SegmentExec 作为过滤条件
3. 只扫描匹配的文档
```

### 评分缓存

```rust
// 同一个查询重复执行
WHERE content = text('rust', 1.0) AND category = 'tech'

// 缓存 term -> score 映射
// 避免重复计算 IDF 和 BM25
```

### 并行评分

```rust
// 多个 term 并行计算
text('rust programming async')
// 'rust', 'programming', 'async' 并行查询索引和计算评分
```

## 总结

新的 SQL 语法设计：
- ✅ 更自然：`field = text(query, boost)`
- ✅ 更简洁：字段名在左侧（SQL 惯例）
- ✅ 更灵活：支持所有 SQL 特性
- ✅ 更强大：BM25 评分 + _score 虚拟列
- ✅ 更易学：标准 SQL，无需 JSON DSL

下一步：实现 SQL 解析器集成和字段名提取逻辑。
