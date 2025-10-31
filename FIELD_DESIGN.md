# Field 字段存储设计文档

## 1. 概述

Field 模块负责单个字段的索引存储和查询，是 Segment 的核心组成部分。每个字段都维护一个倒排索引（Inverted Index），支持高效的关键词查询和主键去重。

## 2. 架构设计

### 2.1 核心结构

```rust
pub struct Keyword {
    field_option: FieldOption,
    inverted_index: InvertedIndex<String>,
}

pub enum InvertedIndex<K> {
    Memory(BTree<K, Arc<RwLock<Vec<u32>>>>),  // 内存模式
    Disk(TreeReader<K, RoaringBitmap>),       // 磁盘模式
}
```

### 2.2 设计理念

**双模式存储：**

- **Memory 模式**：活跃写入，使用 BTree 存储 term → doc_ids 映射
- **Disk 模式**：持久化后，使用 TreeReader 零拷贝读取

**倒排索引结构：**

```
Term (String) → Posting List (Vec<u32> / RoaringBitmap)
  "apple"     → [1, 5, 10, 100, 500]
  "banana"    → [2, 6, 11, 101]
  "cherry"    → [3, 7, 12, 102]
```

## 3. Field 类型

### 3.1 Keyword 字段

**特点：**

- 支持精确匹配查询
- 可以作为主键字段
- 支持数组和单值
- 构建倒排索引

**配置：**

```rust
pub enum FieldOption {
    Keyword {
        name: String,
        is_array: bool,
        index: bool,
    }
}
```

**使用场景：**

- 主键字段（id）
- 标签字段（tags）
- 分类字段（category）
- 状态字段（status）

### 3.2 未来扩展字段类型

1. **Text 字段**
   - 支持全文检索
   - 分词处理
   - TF-IDF 评分

2. **Numeric 字段**
   - 支持范围查询
   - 聚合统计
   - 数值排序

3. **Vector 字段**
   - 向量相似度搜索
   - HNSW 索引
   - ANN 查询

## 4. IndexWriter Trait

所有字段类型必须实现的核心接口：

```rust
pub trait IndexWriter: Sync + 'static {
    fn name(&self) -> &str;
    fn field_type(&self) -> FieldType;
    fn as_any(&self) -> &dyn Any;
    
    // 查询接口
    fn mget_internal_id(
        &self,
        pk_filter: &RwLock<RoaringBitmap>,
        column: &ArrayRef
    ) -> Vec<u32>;
    
    // 持久化接口
    fn persist(&self, path: &str) -> CoreResult<Self>
    where
        Self: Sized;
}
```

## 5. 主键处理（PkWriter）

### 5.1 主键字段特殊性

主键字段除了倒排索引，还需要：

1. **去重检测**：确保主键唯一性
2. **更新处理**：相同主键的新数据覆盖旧数据
3. **BloomFilter**：快速判断主键是否存在

### 5.2 PkWriter Trait

```rust
pub trait PkWriter: IndexWriter {
    fn write_pk(
        &self,
        data: &RecordBatch,
        info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<RoaringBitmap>;
}
```

**write_pk 流程：**

1. **提取主键列**

   ```rust
   let pk_column = data.column_by_name(self.name())?;
   let pk_values = extract_string_array(pk_column);
   ```

2. **查询历史数据**

   ```rust
   if let Some(info) = info {
       for old_segment in info.0 {
           let old_ids = old_segment.mget_internal_id(pk_hash, pk_column);
           deleted_ids.extend(old_ids);
       }
   }
   ```

3. **标记删除旧数据**
   - 返回需要删除的 doc_ids
   - 上层 Segment 维护 deleted bitmap

4. **写入新数据索引**

   ```rust
   for (idx, term) in pk_values.iter().enumerate() {
       let doc_id = start_id + idx as u32;
       self.inverted_index.insert(term, doc_id);
   }
   ```

### 5.3 主键去重性能

**测试数据：300万唯一主键**

- 写入速度：约 35万条/秒
- 内存占用：约 18 MB（倒排索引）
- 去重检测：O(log n) 每次
- BloomFilter 加速：约 30% 性能提升

## 6. 内存模式实现

### 6.1 数据结构

```rust
InvertedIndex::Memory(BTree<String, Arc<RwLock<Vec<u32>>>>)
```

**关键设计：**

- Key: term（关键词）
- Value: Arc<RwLock<Vec<u32>>>（doc_ids 列表）
- Arc: 允许多个引用
- RwLock: 支持并发读写

### 6.2 写入流程

```rust
pub fn write(&self, data: &RecordBatch) -> CoreResult<()> {
    let column = data.column_by_name(self.name())?;
    let start_id = get_internal_id_start(data)?;
    
    match self.inverted_index {
        InvertedIndex::Memory(ref tree) => {
            for (idx, term) in extract_terms(column).enumerate() {
                let doc_id = start_id + idx as u32;
                
                if let Some(posting_list) = tree.get(&term) {
                    // term 已存在，追加 doc_id
                    posting_list.write().unwrap().push(doc_id);
                } else {
                    // 新 term，创建 posting list
                    tree.insert(term, Arc::new(RwLock::new(vec![doc_id])));
                }
            }
        }
        InvertedIndex::Disk(_) => {
            return Err(CoreError::InvalidOperation(
                "Cannot write to disk index".into()
            ));
        }
    }
    
    Ok(())
}
```

### 6.3 查询流程

```rust
pub fn search(&self, term: &str) -> Option<Vec<u32>> {
    match &self.inverted_index {
        InvertedIndex::Memory(tree) => {
            tree.get(term).map(|posting_list| {
                posting_list.read().unwrap().clone()
            })
        }
        InvertedIndex::Disk(reader) => {
            reader.get(term).map(|bitmap| {
                bitmap.iter().collect()
            })
        }
    }
}
```

## 7. 持久化机制

### 7.1 持久化流程

```rust
pub fn persist(&self, path: &str) -> CoreResult<Keyword> {
    let memory_tree = match &self.inverted_index {
        InvertedIndex::Memory(tree) => tree,
        InvertedIndex::Disk(_) => {
            return Err(CoreError::InvalidOperation(
                "Already persisted".into()
            ));
        }
    };
    
    // 1. 转换 posting list 格式
    let iter = memory_tree.iter().map(|item| {
        let (term, posting_list, _ttl) = &*item;
        
        // Vec<u32> → RoaringBitmap（压缩）
        let doc_ids = posting_list.read().unwrap();
        let bitmap = RoaringBitmap::from_iter(doc_ids.iter().copied());
        
        Arc::new((term.clone(), bitmap, None))
    });
    
    // 2. 持久化到磁盘
    let serializer = StringRoaringSerializer::default();
    let writer = TreeWriter::new(
        PathBuf::from(path),
        128,  // chunk_size
        0,    // variable key length
    );
    
    writer.persist(memory_tree.len(), Box::new(serializer), iter)?;
    
    // 3. 创建磁盘版本
    let disk_index = InvertedIndex::new_disk(path, serializer)?;
    
    Ok(Keyword {
        field_option: self.field_option.clone(),
        inverted_index: disk_index,
    })
}
```

### 7.2 格式转换优化

**内存格式：**

```rust
Vec<u32>: [1, 5, 10, 100, 500, 501, 502, ...]
内存占用: 4 * n bytes
```

**磁盘格式：**

```rust
RoaringBitmap: 压缩整数集合
压缩比: 约 60-80%
内存占用: 0.8-1.6 * n bytes（取决于数据分布）
```

**优势：**

- 减少磁盘空间占用
- 支持快速集合运算（AND、OR、NOT）
- 保持查询性能

## 8. 磁盘模式实现

### 8.1 加载流程

```rust
pub fn load_from_disk(path: &str) -> CoreResult<Keyword> {
    let deserializer = StringRoaringSerializer::default();
    let reader = TreeReader::new(path, Box::new(deserializer))?;
    
    let disk_index = InvertedIndex::Disk(reader);
    
    Ok(Keyword {
        field_option: load_field_option(path)?,
        inverted_index: disk_index,
    })
}
```

**特点：**

- 加载速度：< 1ms（仅加载元数据）
- 内存占用：极少（依赖 mmap）
- 查询性能：接近内存模式

### 8.2 查询优化

```rust
// 磁盘模式下的高效查询
impl Keyword {
    pub fn search_disk(&self, term: &str) -> Option<RoaringBitmap> {
        match &self.inverted_index {
            InvertedIndex::Disk(reader) => {
                // 直接返回 RoaringBitmap，无需拷贝
                reader.get(term)
            }
            _ => None,
        }
    }
}
```

## 9. 并发控制

### 9.1 内存模式并发

```rust
// 写入需要写锁
posting_list.write().unwrap().push(doc_id);

// 查询只需读锁
posting_list.read().unwrap().clone()
```

**并发特性：**

- 多读单写
- 不同 term 可以并发写入
- BTree 本身的并发控制

### 9.2 磁盘模式并发

- 只读操作，天然支持并发
- 多线程并发查询
- 无锁设计

## 10. 内存优化

### 10.1 Posting List 压缩

**内存模式：**

```rust
// 超过阈值时自动排序去重
if posting_list.len() > 1000 {
    posting_list.sort_unstable();
    posting_list.dedup();
}
```

**磁盘模式：**

```rust
// 使用 RoaringBitmap 自动压缩
let bitmap = RoaringBitmap::from_iter(doc_ids);
```

### 10.2 内存使用估算

**测试数据：100万文档，1000个唯一 term**

| 组件 | 内存占用 |
|------|---------|
| BTree 结构 | ~2 MB |
| Term 字符串 | ~1 MB |
| Posting Lists | ~4 MB |
| **总计** | **~7 MB** |

## 11. 性能特征

### 11.1 写入性能

**测试场景：300万文档，300万唯一主键**

- 吞吐量：35万条/秒
- 平均延迟：2.8 µs/条
- 内存峰值：~18 MB

### 11.2 查询性能

**内存模式：**

- 点查询：< 1 µs
- 范围查询：O(log n + m)

**磁盘模式：**

- 点查询：~2-5 µs（依赖页面缓存）
- 冷查询：~100 µs（磁盘 IO）

### 11.3 持久化性能

**300万唯一 term：**

- 持久化时间：~1.8 秒
- 磁盘空间：~18 MB
- 压缩比：~65%

## 12. 序列化器实现

### 12.1 StringRoaringSerializer

```rust
pub struct StringRoaringSerializer {
    zstd_level: i32,
}

impl Serializer<String, RoaringBitmap> for StringRoaringSerializer {
    fn serialize(
        &self,
        key: &String,
        value: &RoaringBitmap,
        writer: &mut dyn Write
    ) -> Result<()> {
        // 1. 写入 key 长度
        writer.write_u32::<LittleEndian>(key.len() as u32)?;
        
        // 2. 写入 key 内容
        writer.write_all(key.as_bytes())?;
        
        // 3. 序列化 RoaringBitmap
        let mut bitmap_bytes = Vec::new();
        value.serialize_into(&mut bitmap_bytes)?;
        
        // 4. 可选压缩
        if self.zstd_level > 0 {
            bitmap_bytes = zstd::encode_all(
                &bitmap_bytes[..],
                self.zstd_level
            )?;
        }
        
        // 5. 写入 value 长度和内容
        writer.write_u32::<LittleEndian>(bitmap_bytes.len() as u32)?;
        writer.write_all(&bitmap_bytes)?;
        
        Ok(())
    }
}
```

### 12.2 反序列化器

```rust
impl Deserializer<String, RoaringBitmap> for StringRoaringSerializer {
    fn deserialize(
        &self,
        buffer: &[u8],
        pos: &mut usize
    ) -> Result<(String, RoaringBitmap)> {
        // 1. 读取 key 长度
        let key_len = read_u32(buffer, pos)?;
        
        // 2. 读取 key 内容
        let key = String::from_utf8(
            buffer[*pos..*pos + key_len as usize].to_vec()
        )?;
        *pos += key_len as usize;
        
        // 3. 读取 value 长度
        let value_len = read_u32(buffer, pos)?;
        
        // 4. 读取 value 内容
        let value_bytes = &buffer[*pos..*pos + value_len as usize];
        *pos += value_len as usize;
        
        // 5. 解压缩（如果需要）
        let bitmap_bytes = if self.zstd_level > 0 {
            zstd::decode_all(value_bytes)?
        } else {
            value_bytes.to_vec()
        };
        
        // 6. 反序列化 RoaringBitmap
        let bitmap = RoaringBitmap::deserialize_from(&bitmap_bytes[..])?;
        
        Ok((key, bitmap))
    }
}
```

## 13. 错误处理

```rust
pub enum FieldError {
    InvalidFieldType(String),
    FieldNotFound(String),
    SerializationError(String),
    QueryError(String),
    PersistError(String),
}
```

## 14. 使用示例

### 14.1 创建和写入

```rust
// 创建 keyword 字段
let field = FieldOption::Keyword {
    name: "tags".to_string(),
    is_array: false,
    index: true,
};

let keyword = Keyword::new(&field);

// 写入数据
let batch = create_record_batch()?;
keyword.write(&batch)?;
```

### 14.2 主键字段处理

```rust
// 创建主键字段
let pk_field = FieldOption::Keyword {
    name: "id".to_string(),
    is_array: false,
    index: true,
};

let keyword = Keyword::new(&pk_field);

// 写入时处理去重
let deleted_ids = keyword.write_pk(&batch, Some(info), &lock)?;
segment.mark_del(deleted_ids);
```

### 14.3 查询

```rust
// 查询 term
if let Some(doc_ids) = keyword.search("apple") {
    println!("Found in documents: {:?}", doc_ids);
}
```

### 14.4 持久化和加载

```rust
// 持久化
let disk_keyword = keyword.persist("/path/to/field")?;

// 从磁盘加载
let loaded_keyword = Keyword::from_disk(
    &field_option,
    InvertedIndex::new_disk("/path/to/field", serializer)?
)?;
```

## 15. 最佳实践

### 15.1 字段设计

1. **主键字段**
   - 使用字符串类型
   - 确保唯一性
   - 索引必须开启

2. **标签字段**
   - 使用 Keyword 类型
   - 考虑是否需要数组
   - 高基数字段开启索引

3. **分类字段**
   - 低基数字段适合 Keyword
   - 可以不开启索引（全表扫描）

### 15.2 性能优化

1. **写入优化**
   - 批量写入（10000条/批）
   - 减少锁竞争
   - 预分配内存

2. **查询优化**
   - 使用 BloomFilter 过滤
   - 缓存热点 term
   - 并发查询多个字段

3. **内存优化**
   - 定期持久化释放内存
   - 使用磁盘模式查询
   - 限制 posting list 大小

## 16. 未来优化

### 16.1 索引优化

1. **Skip List**
   - 加速 posting list 遍历
   - 支持快速跳跃

2. **压缩算法**
   - PForDelta 编码
   - Variable Byte 编码
   - Frame of Reference

### 16.2 功能增强

1. **模糊查询**
   - 前缀匹配
   - 通配符支持
   - 正则表达式

2. **聚合统计**
   - Term 频率统计
   - Top-K 查询
   - 基数估算

3. **实时更新**
   - 增量索引
   - 在线合并
   - 版本控制

## 17. 总结

Field 模块提供了：

- ✅ 高效的倒排索引
- ✅ 灵活的字段类型
- ✅ 主键去重机制
- ✅ 内存磁盘双模式
- ✅ 优秀的并发性能
- ✅ 可靠的持久化方案

是 Segment 的核心组件，直接影响整个搜索引擎的查询性能。
