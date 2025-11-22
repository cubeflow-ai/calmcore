# Full-Text Index - Unified Architecture Design

## 概述

按照 Lucene/Tantivy 的正确设计,重构全文索引为**统一的 posting list 架构**,消除了之前分离的 `inverted_index` 和 `position_index` 两个索引的问题。

## 核心设计原则

### ❌ 旧设计 (错误)
```rust
pub struct FullTextField {
    // 问题:两个分离的索引
    inverted_index: InvertedIndex<String>,           // term -> doc_ids
    position_index: BTree<String, BTree<u32, Vec<u32>>>,  // term -> doc -> positions
}
```

**问题所在:**
1. 需要两次查询:先查 doc_ids,再查 positions
2. 嵌套的 BTree 结构,三层查找
3. 不符合 Lucene/Tantivy 的实际设计
4. 维护两个索引的一致性复杂

### ✅ 新设计 (正确)
```rust
/// 统一的 Posting Entry - 包含所有信息
pub struct PostingEntry {
    pub doc_id: u32,       // 文档 ID
    pub term_freq: u32,    // 词频(positions 数组长度)
    pub positions: Vec<u32>, // 位置列表
}

pub struct FullTextField {
    // 单一的统一索引结构
    posting_lists: Arc<RwLock<BTree<String, Vec<PostingEntry>>>>,
    term_stats: Arc<RwLock<HashMap<String, TermStats>>>,
    field_stats: Arc<RwLock<FieldStats>>,
}
```

**优势:**
1. **单一数据结构**: term -> Vec<PostingEntry>,一次查询获取所有信息
2. **顺序访问**: PostingEntry 按 doc_id 排序,支持高效迭代
3. **符合 Lucene/Tantivy 设计**: 真正的统一 posting list
4. **简化维护**: 只需维护一个索引结构

## 架构对比

### Lucene 设计
```
Term Dictionary (FST)
  |
  +--> .doc file: [doc_id, term_freq] (delta encoded)
  +--> .pos file: [positions] (delta encoded)
  
存储分离,但逻辑上是统一的 posting list
```

### Tantivy 设计
```rust
pub struct InvertedIndex {
    terms: TermDictionary,        // FST 压缩
    postings: FileSlice,          // 所有 posting lists 顺序存储
}

// PostingList 格式: [(doc_id, freq, [positions])]
// 统一存储,顺序访问
```

### CalmCore 新设计
```rust
// Memory: BTree<String, Vec<PostingEntry>>
posting_lists.get("rust") 
  => Some([
      PostingEntry { doc_id: 0, term_freq: 1, positions: [0] },
      PostingEntry { doc_id: 2, term_freq: 2, positions: [5, 12] },
      PostingEntry { doc_id: 5, term_freq: 1, positions: [3] },
  ])

// 一次查询,获取所有信息
// 按 doc_id 排序,支持快速迭代和合并
```

## 数据流

### 写入流程
```
Text → Analyzer → Tokens
  ↓
Build: HashMap<String, HashMap<u32, Vec<u32>>>
       (term -> doc_id -> positions)
  ↓
Convert: Vec<PostingEntry> (sorted by doc_id)
  ↓
BTree<String, Vec<PostingEntry>>
```

### 查询流程

#### Term Query
```rust
term_query("rust")
  ↓
posting_lists.get("rust")
  ↓
Extract doc_ids: [0, 2, 5]
  ↓
RoaringBitmap::from_sorted_iter()
```

#### Phrase Query
```rust
phrase_query(["rust", "programming"], slop=0)
  ↓
1. Get posting lists for all terms
2. Find docs containing all terms (intersection)
3. Check position constraints for each doc
  ↓
Result: docs where terms appear in sequence
```

#### BM25 Scoring
```rust
for each doc in results:
  posting_entry = posting_lists.get(term)[doc]
  tf = posting_entry.term_freq
  positions = posting_entry.positions  // 可选,用于位置相关评分
  score = bm25(tf, df, doc_length, avg_doc_length)
```

## 性能优势

### 1. 单次查询
- **旧设计**: 查 inverted_index → 查 position_index (两次)
- **新设计**: 查 posting_lists (一次)

### 2. 顺序访问
```rust
// 直接迭代 posting list
for entry in posting_list {
    process(entry.doc_id, entry.term_freq, &entry.positions);
}
```

### 3. 高效合并
```rust
// Boolean AND: 合并多个 posting lists
let docs1 = posting_lists.get("rust").unwrap();
let docs2 = posting_lists.get("programming").unwrap();

// 两路归并,O(n+m)
let intersection = merge_sorted(docs1, docs2);
```

## 持久化设计 (TODO)

### PostingListSerializer
```rust
// 序列化格式:
// [length: u32] [entry1] [entry2] ... [entryN]
// 
// 每个 entry:
// [doc_id: u32] [term_freq: u32] [positions_len: u32] [pos1] [pos2] ...

// Delta 编码优化:
// doc_ids: delta encoding (0, +5, +3, +7, ...)
// positions: delta encoding per doc ([0, +5, +7], [0, +3], ...)
```

### TreeWriter 集成
```rust
let serializer = PostingListSerializer::new(zstd_level);
let writer = TreeWriter::new(path, chunk_size);

writer.persist::<String, Vec<PostingEntry>, CompressedPostingList>(
    len,
    Box::new(serializer),
    None,  // No union_leaf for full-text
    posting_lists.iter(),
)?;
```

## 代码示例

### 索引文档
```rust
let field = FullTextField::new(&field_opt);

// 批量写入
let batch = create_record_batch(texts);
field.write(&batch, 0)?;

// 内部会构建统一的 posting lists
```

### Term 查询
```rust
let results = field.term_query("rust");
// 返回 RoaringBitmap 包含所有匹配文档
```

### Phrase 查询
```rust
let results = field.phrase_query(&["rust", "programming"], 0);
// 返回精确短语匹配的文档
```

### 获取 Posting List
```rust
let postings = field.get_postings("rust");
// 返回 Vec<PostingEntry>,包含所有信息
for entry in postings.unwrap() {
    println!("Doc {}: tf={}, positions={:?}", 
        entry.doc_id, entry.term_freq, entry.positions);
}
```

## 与 KeywordField 的区别

| 特性 | KeywordField | FullTextField |
|------|-------------|---------------|
| 索引结构 | `term -> Vec<doc_id>` | `term -> Vec<PostingEntry>` |
| 位置信息 | ❌ 无 | ✅ 有 (positions) |
| 短语查询 | ❌ 不支持 | ✅ 支持 |
| 评分 | 简单匹配 | BM25/TF-IDF |
| 存储 | Vec → Bitmap | PostingEntry (delta 编码) |

## 未来优化

1. **Delta 编码**
   - doc_ids: 存储差值而非绝对值
   - positions: 每个文档内存储位置差值

2. **压缩**
   - VInt 编码 (Variable-length integers)
   - 或使用 RoaringBitmap 压缩 doc_ids

3. **分层存储**
   - 热数据: 内存 (Vec<PostingEntry>)
   - 冷数据: 磁盘 (delta encoded + zstd compressed)

4. **跳表 (Skip List)**
   - Lucene 风格的跳表加速长 posting list 的遍历

## 总结

新架构完全遵循 Lucene/Tantivy 的设计理念:

✅ **统一的 Posting List**: 一个 term 对应一个包含所有信息的列表  
✅ **顺序访问**: 按 doc_id 排序,支持高效迭代和合并  
✅ **简化维护**: 单一索引结构,无需同步多个索引  
✅ **功能完整**: 支持 term/phrase/boolean 查询和 BM25 评分  
✅ **性能优秀**: 单次查询,O(1) 查找 + O(n) 顺序扫描  

这才是真正的倒排索引设计! 🎉
