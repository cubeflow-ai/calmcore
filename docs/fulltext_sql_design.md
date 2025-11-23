# 全文搜索 SQL 集成设计

## 概述

将全文搜索功能集成到 SQL 查询中，提供 Elasticsearch 风格的查询语法，同时保持 SQL 的标准性。

## SQL 语法设计

### 1. 文本查询 (Text Query)

**语法**:
```sql
WHERE text(field_name, query_text, boost)
```

**参数**:
- `field_name`: 字段名（必须是已建立全文索引的字段）
- `query_text`: 查询文本（会被分词）
- `boost`: 权重系数（可选，默认 1.0）

**示例**:
```sql
-- 基础查询
SELECT * FROM articles WHERE text(content, 'rust programming', 1.0);

-- 多字段查询（OR 组合）
SELECT * FROM articles 
WHERE text(title, 'rust', 2.0) OR text(content, 'rust', 1.0)
ORDER BY _score DESC;

-- 与其他条件组合
SELECT * FROM articles 
WHERE text(content, 'database', 1.0) 
  AND category = 'tech'
  AND created_at > '2024-01-01'
ORDER BY _score DESC;
```

### 2. 短语查询 (Phrase Query)

**语法**:
```sql
WHERE phrase(field_name, phrase_text, boost, slop)
```

**参数**:
- `field_name`: 字段名
- `phrase_text`: 短语文本（词序重要）
- `boost`: 权重系数（可选，默认 1.0）
- `slop`: 词间最大距离（可选，默认 0 = 精确短语）

**示例**:
```sql
-- 精确短语匹配
SELECT * FROM articles WHERE phrase(content, 'rust programming language', 1.0, 0);

-- 近似短语匹配（slop=2 表示允许中间插入最多 2 个词）
SELECT * FROM articles WHERE phrase(content, 'rust programming', 1.0, 2);
-- 可以匹配: "rust programming", "rust systems programming", "rust safe programming"

-- 短语 + 过滤
SELECT * FROM articles 
WHERE phrase(title, 'full text search', 1.5, 1)
  AND language = 'en'
ORDER BY _score DESC LIMIT 10;
```

### 3. _score 虚拟列

**用法**:
- 自动添加到 SELECT 结果中（当使用全文查询时）
- 可用于 ORDER BY 排序
- 可用于 SELECT 投影

**示例**:
```sql
-- 按相关性排序
SELECT title, _score FROM articles 
WHERE text(content, 'machine learning', 1.0)
ORDER BY _score DESC;

-- 组合排序
SELECT title, _score, created_at FROM articles 
WHERE text(content, 'AI', 1.0)
ORDER BY _score DESC, created_at DESC;

-- 过滤低分文档
SELECT * FROM articles 
WHERE text(content, 'rust', 1.0) AND _score > 0.5
ORDER BY _score DESC;
```

---

## 架构设计

### 1. 组件结构

```
┌─────────────────────────────────────────────────────┐
│                   SQL Layer                         │
│  SELECT * FROM docs WHERE text(content, 'rust')    │
└────────────────┬────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────┐
│              SQL Parser & Analyzer                  │
│  - Parse SQL AST                                    │
│  - Detect fulltext UDFs (text, phrase)             │
│  - Inject _score column if needed                  │
└────────────────┬────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────┐
│           FullTextContext (Query State)             │
│  - indexes: HashMap<FieldName, FullTextField>      │
│  - doc_scores: HashMap<DocId, Score>               │
└────────────────┬────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────┐
│              Query Execution                        │
│  1. Execute fulltext UDFs → populate doc_scores    │
│  2. Execute SQL filters                            │
│  3. Add _score column to results                   │
│  4. Apply ORDER BY _score                          │
└─────────────────────────────────────────────────────┘
```

### 2. 执行流程

```rust
// 伪代码
async fn execute_sql_with_fulltext(sql: &str) -> Result<RecordBatch> {
    // 1. 解析 SQL
    let (statement, normalized_sql) = SqlNormalizer::normalize(sql)?;
    
    // 2. 检测全文查询
    let has_fulltext = detect_fulltext_functions(&statement)?;
    let has_score_order = detect_score_ordering(&statement)?;
    
    if !has_fulltext {
        // 普通 SQL，直接执行
        return execute_normal_sql(normalized_sql).await;
    }
    
    // 3. 创建全文上下文
    let context = Arc::new(FullTextContext::new());
    
    // 4. 注册索引
    for field in get_fulltext_fields(&table_name) {
        let index = load_fulltext_index(&table_name, &field)?;
        context.register_index(field.clone(), index);
    }
    
    // 5. 注册 UDFs
    let session = SessionContext::new();
    session.register_udf(create_text_udf(context.clone()));
    session.register_udf(create_phrase_udf(context.clone()));
    
    // 6. 执行查询
    let mut result = session.sql(&normalized_sql).await?;
    
    // 7. 添加 _score 列（如果需要）
    if has_score_order {
        result = add_score_column(&result, &context, "doc_id")?;
    }
    
    Ok(result)
}
```

---

## 评分算法

### BM25 评分公式

```
score(D, Q) = Σ IDF(qi) × (f(qi, D) × (k1 + 1)) / (f(qi, D) + k1 × (1 - b + b × |D| / avgdl))

其中:
- D: 文档
- Q: 查询 (q1, q2, ..., qn)
- f(qi, D): 词项 qi 在文档 D 中的频率
- |D|: 文档长度
- avgdl: 平均文档长度
- k1: 词频饱和参数 (通常 1.2)
- b: 长度归一化参数 (通常 0.75)
- IDF(qi): 逆文档频率
```

### IDF 计算

```
IDF(qi) = log((N - df(qi) + 0.5) / (df(qi) + 0.5))

其中:
- N: 总文档数
- df(qi): 包含 qi 的文档数
```

### Boost 应用

```
final_score = base_score × boost
```

---

## 实现细节

### 1. FullTextContext

```rust
pub struct FullTextContext {
    // 字段索引映射
    indexes: Arc<RwLock<HashMap<String, Arc<FullTextField>>>>,
    
    // 文档评分（累加多个查询的分数）
    doc_scores: Arc<RwLock<HashMap<u32, f32>>>,
    
    // 查询元数据（用于 explain 等）
    query_stats: Arc<RwLock<QueryStats>>,
}

impl FullTextContext {
    // 注册索引
    pub fn register_index(&self, field: String, index: Arc<FullTextField>);
    
    // 累加评分
    pub fn add_score(&self, doc_id: u32, score: f32);
    
    // 获取评分
    pub fn get_score(&self, doc_id: u32) -> f32;
    
    // 清空评分（新查询）
    pub fn clear_scores(&self);
}
```

### 2. UDF 实现

```rust
// text() UDF
pub fn create_text_udf(context: Arc<FullTextContext>) -> ScalarUDF {
    // 闭包捕获 context
    let text_fn = move |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
        let field = extract_string_arg(&args[0])?;
        let query = extract_string_arg(&args[1])?;
        let boost = extract_float_arg(&args[2]).unwrap_or(1.0);
        
        // 获取索引
        let index = context.get_index(&field)?;
        
        // 执行查询
        let matches = index.term_query(&query)?;
        
        // 计算评分并存储
        for doc_id in matches.iter() {
            let score = calculate_bm25(doc_id, &query, &index, boost);
            context.add_score(doc_id, score);
        }
        
        // 返回匹配结果（Boolean 数组）
        Ok(ColumnarValue::Array(matches_to_boolean_array(matches)))
    };
    
    ScalarUDF::new("text", signature, return_type, text_fn)
}
```

### 3. _score 列注入

```rust
pub fn add_score_column(
    batch: &RecordBatch,
    context: &FullTextContext,
    doc_id_column: &str,
) -> DataFusionResult<RecordBatch> {
    // 1. 提取 doc_id 列
    let doc_ids = batch.column_by_name(doc_id_column)?;
    
    // 2. 为每个 doc_id 查找评分
    let scores: Vec<f32> = doc_ids
        .as_any()
        .downcast_ref::<UInt32Array>()?
        .iter()
        .map(|id| context.get_score(id.unwrap_or(0)))
        .collect();
    
    // 3. 创建 _score 列
    let score_array = Float32Array::from(scores);
    
    // 4. 添加到 RecordBatch
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Field::new("_score", DataType::Float32, false));
    
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(score_array));
    
    let new_schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(new_schema, columns)
}
```

---

## 查询优化

### 1. 索引预过滤

```sql
-- 优化前: 全表扫描 + 全文过滤
SELECT * FROM articles WHERE text(content, 'rust', 1.0);

-- 优化后: 全文索引直接返回 doc_ids
1. 执行全文查询 → RoaringBitmap {1, 5, 10, 20}
2. 只扫描这些文档（不是全表）
```

### 2. 多条件下推

```sql
-- 查询
SELECT * FROM articles 
WHERE text(content, 'rust', 1.0) 
  AND category = 'tech' 
  AND created_at > '2024-01-01';

-- 执行计划
1. 全文索引 → doc_ids: {1, 5, 10, 20, 50}
2. 下推过滤: category = 'tech' AND created_at > '2024-01-01'
3. 最终只扫描满足全文 + 结构化条件的行
```

### 3. 评分缓存

```rust
// 对于相同的查询，缓存评分结果
impl FullTextContext {
    cache: Arc<RwLock<HashMap<QueryKey, HashMap<u32, f32>>>>,
    
    pub fn get_or_compute_scores(&self, query: &Query) -> HashMap<u32, f32> {
        let cache = self.cache.read().unwrap();
        if let Some(scores) = cache.get(&query.key()) {
            return scores.clone();
        }
        
        // 计算评分
        let scores = compute_scores(query);
        
        // 缓存
        let mut cache = self.cache.write().unwrap();
        cache.insert(query.key(), scores.clone());
        
        scores
    }
}
```

---

## 使用示例

### 示例 1: 简单文本搜索

```sql
-- 查询包含 "rust" 的文章，按相关性排序
SELECT title, author, _score 
FROM articles 
WHERE text(content, 'rust', 1.0)
ORDER BY _score DESC 
LIMIT 10;
```

### 示例 2: 多字段加权搜索

```sql
-- 标题权重更高
SELECT title, _score 
FROM articles 
WHERE text(title, 'machine learning', 3.0) 
   OR text(content, 'machine learning', 1.0)
ORDER BY _score DESC;
```

### 示例 3: 短语搜索

```sql
-- 查找包含精确短语 "full text search" 的文档
SELECT * FROM docs 
WHERE phrase(content, 'full text search', 1.0, 0)
ORDER BY _score DESC;
```

### 示例 4: 复杂 Boolean 查询

```sql
-- (rust AND programming) OR (systems AND language)
SELECT * FROM articles 
WHERE (
    text(content, 'rust', 1.0) 
    AND text(content, 'programming', 1.0)
) OR (
    text(content, 'systems', 1.0) 
    AND text(content, 'language', 1.0)
)
ORDER BY _score DESC;
```

### 示例 5: 结合结构化过滤

```sql
-- 全文 + 时间范围 + 分类
SELECT title, category, created_at, _score 
FROM articles 
WHERE text(content, 'database optimization', 1.0)
  AND category IN ('tech', 'tutorial')
  AND created_at >= '2024-01-01'
  AND created_at < '2025-01-01'
ORDER BY _score DESC, created_at DESC
LIMIT 20;
```

---

## 对比 Elasticsearch

### 相似之处

| 特性 | CalmCore SQL | Elasticsearch |
|------|-------------|---------------|
| 文本查询 | `text(field, query, boost)` | `{"match": {"field": {"query": "...", "boost": 1.0}}}` |
| 短语查询 | `phrase(field, query, boost, slop)` | `{"match_phrase": {"field": {"query": "...", "slop": 0}}}` |
| 评分 | `ORDER BY _score DESC` | `"sort": [{"_score": "desc"}]` |
| Boolean | `WHERE ... AND/OR/NOT` | `{"bool": {"must": [...], "should": [...]}}` |
| 过滤 | `WHERE category = 'tech'` | `{"filter": {"term": {"category": "tech"}}}` |

### 优势

1. **标准 SQL**: 无需学习 JSON DSL
2. **与关系数据结合**: `JOIN`, `GROUP BY`, `HAVING` 等
3. **类型安全**: 编译时类型检查
4. **工具支持**: 任何 SQL 客户端都能用

### 劣势

1. **表达能力**: Boolean 查询不如 ES 灵活
2. **聚合**: 缺少 ES 的复杂聚合
3. **高亮**: 需要额外实现

---

## 下一步实现计划

### Phase 1: 核心功能 ✅
- [x] FullTextContext 设计
- [x] text() UDF
- [x] phrase() UDF
- [x] _score 列注入

### Phase 2: SQL 集成
- [ ] 修改 SqlNormalizer 识别全文 UDF
- [ ] 在 DistributedExecutor 中注册 UDFs
- [ ] 实现 _score 列自动添加
- [ ] 测试用例

### Phase 3: 性能优化
- [ ] 索引预过滤（避免全表扫描）
- [ ] 评分缓存
- [ ] 并行评分计算
- [ ] 查询计划优化

### Phase 4: 高级特性
- [ ] 模糊查询 `fuzzy(field, query, fuzziness)`
- [ ] 通配符 `wildcard(field, pattern)`
- [ ] 正则表达式 `regexp(field, pattern)`
- [ ] 高亮 `highlight(field, query) -> String`

---

## 总结

这个设计:

✅ **标准 SQL 语法** - 无需学习新的查询语言
✅ **Elasticsearch 风格** - 熟悉的全文搜索概念
✅ **性能优秀** - 利用 Parquet 索引和 Roaring Bitmap
✅ **易于集成** - 基于 DataFusion UDF 机制
✅ **可扩展** - 易于添加新的查询类型

相比 Lucene/ES:
- **简单**: 无需复杂的 JSON DSL
- **统一**: 全文和结构化查询在同一语言
- **强大**: 可以 JOIN 其他表，GROUP BY 聚合等

是一个**创新且实用**的设计！ 🎯
