# 全文索引设计方案

## 1. 整体架构

### 1.1 核心组件

```
FullTextIndex
├── InvertedIndex (倒排索引)
│   ├── term → posting_list (词项 → 倒排列表)
│   │   └── BTree<String, PostingList>
│   └── doc_lengths (文档长度)
│       └── BTree<u32, DocMetadata>
│
├── ForwardIndex (正排索引 - 用于短语查询)
│   └── doc_id → term_positions
│       └── BTree<u32, Vec<(term_id, position)>>
│
├── TermDictionary (词项字典)
│   ├── term → term_id
│   │   └── BTree<String, u32>
│   └── term_id → term
│       └── BTree<u32, String>
│
├── FieldStatistics (字段统计)
│   ├── total_docs: u32
│   ├── avg_doc_length: f32
│   └── total_terms: u64
│
└── Analyzer (分词器 - 复用已有的)
    ├── Tokenizer
    ├── Filters (lowercase, stemming, stopwords)
    └── Synonyms
```

## 2. 核心数据结构

### 2.1 PostingList (倒排列表)

```rust
/// 倒排列表 - 存储某个词项出现在哪些文档中
pub struct PostingList {
    /// 文档频率 (Document Frequency)
    pub df: u32,
    
    /// 倒排项列表
    pub postings: RoaringBitmap,  // doc_ids
    
    /// 词项频率 (Term Frequency) - 每个文档中该词出现的次数
    pub term_freqs: BTree<u32, u32>,  // doc_id → tf
    
    /// 位置信息 (用于短语查询和邻近度查询)
    pub positions: BTree<u32, Vec<u32>>,  // doc_id → [positions]
}
```

### 2.2 DocMetadata (文档元数据)

```rust
/// 文档元数据
pub struct DocMetadata {
    /// 文档 ID
    pub doc_id: u32,
    
    /// 文档长度 (词项数量)
    pub length: u32,
    
    /// 文档中唯一词项数
    pub unique_terms: u32,
    
    /// 字段长度归一化因子 (可选)
    pub norm: f32,
}
```

### 2.3 TermInfo (词项信息)

```rust
/// 词项信息
pub struct TermInfo {
    /// 词项 ID
    pub term_id: u32,
    
    /// 词项字符串
    pub term: String,
    
    /// 文档频率
    pub df: u32,
    
    /// 总词频 (所有文档中该词出现的总次数)
    pub total_tf: u64,
}
```

## 3. 评分算法

### 3.1 BM25 算法 (推荐)

BM25 是目前最流行的相关性评分算法，优于 TF-IDF。

**公式：**
```
score(D, Q) = Σ IDF(qi) * (f(qi, D) * (k1 + 1)) / (f(qi, D) + k1 * (1 - b + b * |D| / avgdl))

其中：
- D: 文档
- Q: 查询
- qi: 查询中的第 i 个词项
- f(qi, D): qi 在文档 D 中的词频
- |D|: 文档 D 的长度
- avgdl: 平均文档长度
- k1: 词频饱和参数 (默认 1.2)
- b: 长度归一化参数 (默认 0.75)
- IDF(qi) = log((N - n(qi) + 0.5) / (n(qi) + 0.5) + 1)
  - N: 总文档数
  - n(qi): 包含 qi 的文档数
```

**实现：**
```rust
pub struct BM25Scorer {
    k1: f32,      // 默认 1.2
    b: f32,       // 默认 0.75
    delta: f32,   // BM25+ 的增量参数，默认 1.0
}

impl BM25Scorer {
    pub fn score(
        &self,
        tf: u32,           // 词频
        df: u32,           // 文档频率
        doc_len: u32,      // 文档长度
        avg_doc_len: f32,  // 平均文档长度
        total_docs: u32,   // 总文档数
    ) -> f32 {
        // IDF 计算
        let idf = ((total_docs - df + 0.5) as f32 / (df + 0.5) as f32 + 1.0).ln();
        
        // 词频归一化
        let norm = 1.0 - self.b + self.b * (doc_len as f32 / avg_doc_len);
        let tf_norm = tf as f32 / (self.k1 * norm + tf as f32);
        
        // BM25+ (加入 delta 避免长文档惩罚过度)
        idf * (tf_norm * (self.k1 + 1.0) + self.delta)
    }
}
```

### 3.2 TF-IDF 算法 (备选)

**公式：**
```
score(D, Q) = Σ TF(qi, D) * IDF(qi)

TF(qi, D) = freq(qi, D) / |D|
IDF(qi) = log(N / n(qi))
```

## 4. 查询类型支持

### 4.1 术语查询 (Term Query)

最基本的查询，匹配包含某个词项的文档。

```rust
pub struct TermQuery {
    pub term: String,
    pub boost: f32,  // 权重提升
}
```

### 4.2 布尔查询 (Boolean Query)

支持 AND、OR、NOT 组合。

```rust
pub enum BooleanClause {
    Must(Box<Query>),      // AND
    Should(Box<Query>),    // OR
    MustNot(Box<Query>),   // NOT
}

pub struct BooleanQuery {
    pub clauses: Vec<BooleanClause>,
    pub minimum_should_match: usize,  // 至少匹配几个 Should
}
```

### 4.3 短语查询 (Phrase Query)

精确匹配一个短语，词项顺序和位置必须一致。

```rust
pub struct PhraseQuery {
    pub terms: Vec<String>,
    pub slop: u32,  // 允许的词项间隔，0 表示严格相邻
}

// 示例：
// "quick brown fox" with slop=0 → 必须严格相邻
// "quick brown fox" with slop=1 → 允许中间插入 1 个词
```

**实现思路：**
1. 获取每个词项的位置列表
2. 对于每个文档，检查是否存在位置序列满足：
   - pos[term[i+1]] = pos[term[i]] + 1 + offset (offset <= slop)

### 4.4 邻近度查询 (Proximity Query)

类似短语查询，但词项顺序可以不固定。

```rust
pub struct ProximityQuery {
    pub terms: Vec<String>,
    pub max_distance: u32,  // 最大词项距离
    pub ordered: bool,      // 是否保持顺序
}
```

### 4.5 前缀查询 (Prefix Query)

匹配以某个前缀开头的词项。

```rust
pub struct PrefixQuery {
    pub prefix: String,
}

// 利用 BTree 的范围查询
// seek(prefix) 直到 key 不再匹配前缀
```

### 4.6 通配符查询 (Wildcard Query)

支持 * 和 ? 通配符。

```rust
pub struct WildcardQuery {
    pub pattern: String,  // "hel?o", "test*"
}
```

### 4.7 模糊查询 (Fuzzy Query)

基于编辑距离的相似匹配。

```rust
pub struct FuzzyQuery {
    pub term: String,
    pub max_edits: u8,     // 最大编辑距离 (通常 1 或 2)
    pub prefix_length: u8, // 前缀必须精确匹配的长度
}
```

## 5. 核心实现

### 5.1 FullTextIndex 主结构

```rust
pub struct FullTextIndex {
    /// 倒排索引: term → posting_list
    inverted_index: Arc<RwLock<BTree<String, PostingList>>>,
    
    /// 正排索引: doc_id → term_positions (用于短语查询)
    forward_index: Arc<RwLock<BTree<u32, Vec<(u32, u32)>>>>,
    
    /// 词项字典: term → term_id
    term_to_id: Arc<RwLock<BTree<String, u32>>>,
    
    /// 词项字典: term_id → term
    id_to_term: Arc<RwLock<BTree<u32, String>>>,
    
    /// 文档元数据: doc_id → metadata
    doc_metadata: Arc<RwLock<BTree<u32, DocMetadata>>>,
    
    /// 字段统计
    stats: Arc<RwLock<FieldStatistics>>,
    
    /// 分词器
    analyzer: Arc<Analyzer>,
    
    /// 评分器
    scorer: Arc<BM25Scorer>,
    
    /// 下一个词项 ID
    next_term_id: Arc<RwLock<u32>>,
}
```

### 5.2 索引构建

```rust
impl FullTextIndex {
    /// 索引一个文档
    pub fn index_document(&mut self, doc_id: u32, text: &str) -> CoreResult<()> {
        // 1. 分词
        let tokens = self.analyzer.analyzer_index(text);
        
        // 2. 构建词项位置映射
        let mut term_positions: HashMap<String, Vec<u32>> = HashMap::new();
        for token in &tokens {
            term_positions
                .entry(token.name.clone())
                .or_default()
                .push(token.index as u32);
        }
        
        // 3. 更新倒排索引
        let mut inverted = self.inverted_index.write().unwrap();
        let mut term_to_id = self.term_to_id.write().unwrap();
        let mut id_to_term = self.id_to_term.write().unwrap();
        let mut next_id = self.next_term_id.write().unwrap();
        
        let mut forward_positions = Vec::new();
        
        for (term, positions) in term_positions {
            // 分配 term_id
            let term_id = match term_to_id.get(&term) {
                Some(id) => *id,
                None => {
                    let id = *next_id;
                    *next_id += 1;
                    term_to_id.put(term.clone(), id);
                    id_to_term.put(id, term.clone());
                    id
                }
            };
            
            // 更新倒排列表
            let mut posting_list = inverted.get(&term)
                .cloned()
                .unwrap_or_default();
            
            posting_list.postings.insert(doc_id);
            posting_list.df = posting_list.postings.len() as u32;
            posting_list.term_freqs.put(doc_id, positions.len() as u32);
            posting_list.positions.put(doc_id, positions.clone());
            
            inverted.put(term, posting_list);
            
            // 构建正排索引
            for pos in positions {
                forward_positions.push((term_id, pos));
            }
        }
        
        // 4. 排序正排索引 (按位置排序)
        forward_positions.sort_by_key(|(_, pos)| *pos);
        
        // 5. 更新正排索引
        let mut forward = self.forward_index.write().unwrap();
        forward.put(doc_id, forward_positions);
        
        // 6. 更新文档元数据
        let mut metadata = self.doc_metadata.write().unwrap();
        metadata.put(doc_id, DocMetadata {
            doc_id,
            length: tokens.len() as u32,
            unique_terms: term_positions.len() as u32,
            norm: 1.0,
        });
        
        // 7. 更新统计信息
        let mut stats = self.stats.write().unwrap();
        stats.total_docs += 1;
        stats.total_terms += tokens.len() as u64;
        stats.avg_doc_length = stats.total_terms as f32 / stats.total_docs as f32;
        
        Ok(())
    }
}
```

### 5.3 查询执行

```rust
impl FullTextIndex {
    /// 执行术语查询
    pub fn term_query(&self, term: &str) -> CoreResult<Vec<(u32, f32)>> {
        let inverted = self.inverted_index.read().unwrap();
        let metadata = self.doc_metadata.read().unwrap();
        let stats = self.stats.read().unwrap();
        
        // 获取倒排列表
        let posting_list = match inverted.get(term) {
            Some(pl) => pl,
            None => return Ok(Vec::new()),
        };
        
        let mut results = Vec::new();
        
        // 对每个匹配的文档计算分数
        for doc_id in posting_list.postings.iter() {
            let tf = *posting_list.term_freqs.get(&doc_id).unwrap_or(&0);
            let doc_meta = metadata.get(&doc_id).unwrap();
            
            let score = self.scorer.score(
                tf,
                posting_list.df,
                doc_meta.length,
                stats.avg_doc_length,
                stats.total_docs,
            );
            
            results.push((doc_id, score));
        }
        
        // 按分数降序排序
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        
        Ok(results)
    }
    
    /// 执行短语查询
    pub fn phrase_query(&self, terms: &[String], slop: u32) -> CoreResult<Vec<(u32, f32)>> {
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        
        let inverted = self.inverted_index.read().unwrap();
        
        // 1. 获取所有词项的倒排列表
        let mut posting_lists = Vec::new();
        for term in terms {
            match inverted.get(term) {
                Some(pl) => posting_lists.push(pl),
                None => return Ok(Vec::new()),  // 任何词项不存在都返回空
            }
        }
        
        // 2. 找出所有词项都出现的文档 (交集)
        let mut candidate_docs = posting_lists[0].postings.clone();
        for pl in &posting_lists[1..] {
            candidate_docs &= &pl.postings;
        }
        
        if candidate_docs.is_empty() {
            return Ok(Vec::new());
        }
        
        // 3. 对每个候选文档，检查是否满足短语约束
        let mut results = Vec::new();
        
        for doc_id in candidate_docs.iter() {
            // 获取每个词项在该文档中的位置
            let mut term_positions: Vec<&Vec<u32>> = Vec::new();
            for pl in &posting_lists {
                if let Some(positions) = pl.positions.get(&doc_id) {
                    term_positions.push(positions);
                } else {
                    continue;  // 跳过这个文档
                }
            }
            
            // 检查是否存在满足短语约束的位置序列
            if self.check_phrase_match(&term_positions, slop) {
                // 计算分数 (可以基于 BM25 或其他算法)
                let score = self.score_phrase_match(doc_id, &posting_lists);
                results.push((doc_id, score));
            }
        }
        
        // 按分数降序排序
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        
        Ok(results)
    }
    
    /// 检查是否满足短语匹配
    fn check_phrase_match(&self, term_positions: &[&Vec<u32>], slop: u32) -> bool {
        if term_positions.is_empty() {
            return false;
        }
        
        // 对于第一个词项的每个位置
        for &start_pos in term_positions[0] {
            let mut current_pos = start_pos;
            let mut matched = true;
            
            // 检查后续词项是否在允许的距离内
            for positions in &term_positions[1..] {
                let min_pos = current_pos + 1;
                let max_pos = current_pos + 1 + slop;
                
                // 查找是否有位置在 [min_pos, max_pos] 范围内
                let found = positions.iter().any(|&pos| pos >= min_pos && pos <= max_pos);
                
                if !found {
                    matched = false;
                    break;
                }
                
                // 更新当前位置为找到的最小位置
                current_pos = *positions.iter()
                    .filter(|&&pos| pos >= min_pos && pos <= max_pos)
                    .min()
                    .unwrap();
            }
            
            if matched {
                return true;
            }
        }
        
        false
    }
    
    /// 布尔查询
    pub fn boolean_query(&self, clauses: &[BooleanClause]) -> CoreResult<Vec<(u32, f32)>> {
        let mut must_docs: Option<RoaringBitmap> = None;
        let mut should_docs = RoaringBitmap::new();
        let mut must_not_docs = RoaringBitmap::new();
        
        let mut doc_scores: HashMap<u32, f32> = HashMap::new();
        
        for clause in clauses {
            match clause {
                BooleanClause::Must(query) => {
                    let results = self.execute_query(query)?;
                    let docs: RoaringBitmap = results.iter().map(|(id, _)| *id).collect();
                    
                    must_docs = Some(match must_docs {
                        Some(existing) => existing & docs,
                        None => docs,
                    });
                    
                    for (doc_id, score) in results {
                        *doc_scores.entry(doc_id).or_insert(0.0) += score;
                    }
                }
                BooleanClause::Should(query) => {
                    let results = self.execute_query(query)?;
                    for (doc_id, score) in results {
                        should_docs.insert(doc_id);
                        *doc_scores.entry(doc_id).or_insert(0.0) += score;
                    }
                }
                BooleanClause::MustNot(query) => {
                    let results = self.execute_query(query)?;
                    for (doc_id, _) in results {
                        must_not_docs.insert(doc_id);
                    }
                }
            }
        }
        
        // 计算最终结果集
        let mut final_docs = must_docs.unwrap_or_else(|| should_docs.clone());
        final_docs -= must_not_docs;
        
        // 构建结果
        let mut results: Vec<(u32, f32)> = final_docs
            .iter()
            .map(|doc_id| (doc_id, *doc_scores.get(&doc_id).unwrap_or(&0.0)))
            .collect();
        
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        
        Ok(results)
    }
}
```

## 6. 持久化方案

利用 mem_btree 的持久化功能：

```rust
impl FullTextIndex {
    /// 持久化索引
    pub fn persist(&self, base_path: &Path) -> CoreResult<()> {
        // 1. 持久化倒排索引
        let inverted = self.inverted_index.read().unwrap();
        inverted.persist(&base_path.join("inverted_index"))?;
        
        // 2. 持久化正排索引
        let forward = self.forward_index.read().unwrap();
        forward.persist(&base_path.join("forward_index"))?;
        
        // 3. 持久化词项字典
        let term_to_id = self.term_to_id.read().unwrap();
        term_to_id.persist(&base_path.join("term_to_id"))?;
        
        let id_to_term = self.id_to_term.read().unwrap();
        id_to_term.persist(&base_path.join("id_to_term"))?;
        
        // 4. 持久化文档元数据
        let metadata = self.doc_metadata.read().unwrap();
        metadata.persist(&base_path.join("doc_metadata"))?;
        
        // 5. 持久化统计信息
        let stats = self.stats.read().unwrap();
        let stats_json = serde_json::to_vec(&*stats)?;
        std::fs::write(base_path.join("stats.json"), stats_json)?;
        
        Ok(())
    }
    
    /// 加载索引
    pub fn load(base_path: &Path, analyzer: Arc<Analyzer>) -> CoreResult<Self> {
        // 加载逻辑类似...
    }
}
```

## 7. 优化策略

### 7.1 压缩优化

1. **倒排列表压缩**：
   - 使用 RoaringBitmap 压缩 doc_ids
   - 位置信息使用增量编码 + VarInt

2. **词项字典压缩**：
   - 使用前缀树(Trie)或 FST 压缩词项

### 7.2 查询优化

1. **跳表加速**：
   - 在长倒排列表中使用跳表加速合并

2. **提前终止**：
   - Top-K 查询时，使用堆维护，提前终止

3. **缓存**：
   - 缓存热门查询的结果

### 7.3 内存优化

1. **延迟加载**：
   - 位置信息按需加载（仅短语查询时加载）

2. **分段索引**：
   - 大索引分成多个 segment，并行查询后合并

## 8. 使用示例

```rust
// 创建索引
let analyzer = Arc::new(Analyzer::default());
let mut index = FullTextIndex::new(analyzer, BM25Scorer::default());

// 索引文档
index.index_document(1, "The quick brown fox jumps over the lazy dog")?;
index.index_document(2, "A quick brown dog runs fast")?;
index.index_document(3, "The lazy cat sleeps")?;

// 术语查询
let results = index.term_query("quick")?;
// 结果: [(1, 0.85), (2, 0.82)]

// 短语查询
let results = index.phrase_query(&["quick", "brown"], 0)?;
// 结果: [(1, 0.95), (2, 0.93)]

// 布尔查询
let query = BooleanQuery {
    clauses: vec![
        BooleanClause::Must(Box::new(TermQuery { term: "quick" })),
        BooleanClause::Should(Box::new(TermQuery { term: "lazy" })),
        BooleanClause::MustNot(Box::new(TermQuery { term: "cat" })),
    ],
    minimum_should_match: 0,
};
let results = index.boolean_query(&query.clauses)?;
// 结果: [(1, 1.2), (2, 0.82)]

// 持久化
index.persist(Path::new("./data/fulltext_index"))?;

// 加载
let index = FullTextIndex::load(Path::new("./data/fulltext_index"), analyzer)?;
```

## 9. 后续扩展

1. **高亮支持**：返回匹配片段和高亮位置
2. **拼写纠正**：基于编辑距离和词频
3. **查询建议**：基于前缀树的自动补全
4. **多字段查询**：支持在多个字段上查询并合并结果
5. **字段权重**：不同字段有不同的权重
6. **分布式支持**：索引分片和查询聚合
7. **实时更新**：支持增量更新和删除

## 10. 性能目标

- **索引速度**：10,000+ docs/sec
- **查询延迟**：< 10ms (p99)
- **内存占用**：~100MB/million docs
- **压缩率**：50-70% (相比原始数据)
