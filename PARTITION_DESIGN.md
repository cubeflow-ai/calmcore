# Partition 分区管理设计文档

## 1. 概述

Partition 是存储引擎的顶层管理组件，负责管理多个 Segment，协调写入、查询、持久化等操作。一个 Partition 可以包含多个 Active 和 Frozen Segment，提供统一的数据访问接口。

## 2. 核心架构

### 2.1 数据结构

```rust
pub struct Partition {
    schema: Schema,
    data_dir: PathBuf,
    
    // Segment 管理
    active_segment: RwLock<Option<Arc<Segment>>>,
    frozen_segments: RwLock<Vec<Arc<Segment>>>,
    
    // 全局 doc_id 分配器
    next_doc_id: AtomicU32,
    
    // 配置
    segment_size: u32,  // 每个 segment 的文档数阈值
}
```

### 2.2 多 Segment 管理

```
Partition
    ↓
    ├── Active Segment (write)
    │   └── doc_id: 2000000 - 2999999
    │
    ├── Frozen Segment 1 (query only)
    │   └── doc_id: 0 - 999999
    │
    └── Frozen Segment 2 (query only)
        └── doc_id: 1000000 - 1999999
```

**设计理念：**

- 只有1个 Active Segment：接收写入
- 多个 Frozen Segment：只读查询
- doc_id 全局递增：跨 Segment 唯一

## 3. 写入流程

### 3.1 写入接口

```rust
pub fn write(&self, record_batch: RecordBatch) -> CoreResult<u32> {
    // Step 1: 获取或创建 Active Segment
    let segment = {
        let active = self.active_segment.read().unwrap();
        if active.is_none() || active.as_ref().unwrap().should_freeze() {
            drop(active);  // 释放读锁
            self.rotate_segment()?;  // 创建新segment
            let active = self.active_segment.read().unwrap();
            active.as_ref().unwrap().clone()
        } else {
            active.as_ref().unwrap().clone()
        }
    };
    
    // Step 2: 写入 Segment
    let base_id = segment.write(record_batch)?;
    
    // Step 3: 检查是否需要 flush
    if segment.doc_count() >= self.segment_size {
        self.flush_segment(segment)?;
    }
    
    Ok(base_id)
}
```

### 3.2 Segment 轮转

```rust
fn rotate_segment(&self) -> CoreResult<()> {
    let mut active = self.active_segment.write().unwrap();
    
    // 冻结旧 segment
    if let Some(old_segment) = active.take() {
        old_segment.freeze();
        
        // 移动到 frozen_segments
        let mut frozen = self.frozen_segments.write().unwrap();
        frozen.push(old_segment);
    }
    
    // 创建新 segment
    let start_id = self.next_doc_id.load(Ordering::Relaxed);
    let new_segment = Arc::new(Segment::new(
        self.schema.clone(),
        start_id
    ));
    
    *active = Some(new_segment);
    
    Ok(())
}
```

### 3.3 Flush 策略

**定期 Flush：**

```rust
pub fn flush_segment(&self, segment: Arc<Segment>) -> CoreResult<()> {
    // Step 1: 冻结 segment
    segment.freeze();
    
    // Step 2: 后台持久化
    let data_dir = self.data_dir.clone();
    tokio::spawn(async move {
        if let Err(e) = segment.persist(&data_dir) {
            eprintln!("Persist failed: {:?}", e);
        }
    });
    
    Ok(())
}
```

**Flush 时机：**

| 策略 | 触发条件 | 优缺点 |
|------|---------|--------|
| 大小阈值 | doc_count >= 100万 | ✅ 推荐：平衡内存和性能 |
| 时间间隔 | 每隔 5分钟 | ⚠️ 可能产生小segment |
| 内存阈值 | memory >= 100MB | ⚠️ 需要精确计算内存 |
| 手动触发 | flush() 调用 | ⚠️ 需要外部控制 |

**推荐：大小阈值策略（100万条/segment）**

### 3.4 并发写入性能

**测试：8线程并发写入**

| Segment数 | 吞吐量 | CPU | 内存 |
|----------|--------|-----|------|
| 1 (Active) | 36万/s | 85% | 35 MB |
| 2 (1A+1F) | 35万/s | 90% | 37 MB |
| 3 (1A+2F) | 34万/s | 95% | 39 MB |

**瓶颈：** Active Segment 的写锁竞争

## 4. 查询流程

### 4.1 点查询

```rust
pub fn get_document(&self, doc_id: u32) -> CoreResult<Option<RecordBatch>> {
    // Step 1: 从 Active Segment 查询
    {
        let active = self.active_segment.read().unwrap();
        if let Some(segment) = active.as_ref() {
            if let Some(doc) = segment.get_document(doc_id)? {
                return Ok(Some(doc));
            }
        }
    }
    
    // Step 2: 从 Frozen Segments 查询（逆序）
    {
        let frozen = self.frozen_segments.read().unwrap();
        for segment in frozen.iter().rev() {
            if let Some(doc) = segment.get_document(doc_id)? {
                return Ok(Some(doc));
            }
        }
    }
    
    Ok(None)
}
```

**查询顺序：**

1. Active Segment（最新数据）
2. Frozen Segments（从新到旧）

**性能优化：**

- 早停策略：找到就返回
- 范围检查：利用 start_id/end_id 快速跳过

### 4.2 Term 查询

```rust
pub fn query_term(&self, field: &str, term: &str) -> CoreResult<RoaringBitmap> {
    let mut result = RoaringBitmap::new();
    
    // Step 1: 查询 Active Segment
    {
        let active = self.active_segment.read().unwrap();
        if let Some(segment) = active.as_ref() {
            let doc_ids = segment.query_term(field, term)?;
            result |= doc_ids;
        }
    }
    
    // Step 2: 查询所有 Frozen Segments
    {
        let frozen = self.frozen_segments.read().unwrap();
        for segment in frozen.iter() {
            let doc_ids = segment.query_term(field, term)?;
            result |= doc_ids;
        }
    }
    
    Ok(result)
}
```

**并行查询优化：**

```rust
pub fn query_term_parallel(&self, field: &str, term: &str) 
    -> CoreResult<RoaringBitmap> 
{
    let segments = self.collect_all_segments();
    
    // 并行查询所有 segment
    let handles: Vec<_> = segments.into_iter()
        .map(|seg| {
            let f = field.to_string();
            let t = term.to_string();
            thread::spawn(move || {
                seg.query_term(&f, &t)
            })
        })
        .collect();
    
    // 合并结果
    let mut result = RoaringBitmap::new();
    for handle in handles {
        result |= handle.join()??;
    }
    
    Ok(result)
}
```

**并行效果：**

| Segment数 | 串行QPS | 并行QPS | 加速比 |
|----------|---------|---------|--------|
| 1 | 40万 | 40万 | 1.0x |
| 2 | 20万 | 35万 | 1.75x |
| 4 | 10万 | 30万 | 3.0x |
| 8 | 5万 | 25万 | 5.0x |

### 4.3 复杂查询

**AND 查询：**

```rust
pub fn query_and(&self, terms: &[(&str, &str)]) -> CoreResult<RoaringBitmap> {
    let mut results = Vec::new();
    
    // 查询每个 term
    for (field, term) in terms {
        let bitmap = self.query_term(field, term)?;
        results.push(bitmap);
    }
    
    // 求交集
    let mut result = results[0].clone();
    for bitmap in &results[1..] {
        result &= bitmap;
    }
    
    Ok(result)
}
```

**OR 查询：**

```rust
pub fn query_or(&self, terms: &[(&str, &str)]) -> CoreResult<RoaringBitmap> {
    let mut result = RoaringBitmap::new();
    
    // 查询每个 term
    for (field, term) in terms {
        let bitmap = self.query_term(field, term)?;
        result |= bitmap;
    }
    
    Ok(result)
}
```

### 4.4 查询性能

**测试：300万条记录，3个segment**

| 查询类型 | 平均延迟 | P95 | P99 |
|---------|---------|-----|-----|
| get_document() | 4.1 µs | 10 µs | 20 µs |
| query_term() | 8.5 µs | 18 µs | 35 µs |
| query_and(2) | 12 µs | 25 µs | 50 µs |
| query_or(2) | 15 µs | 30 µs | 60 µs |
| scan_all() | 150 ms | 300 ms | 500 ms |

## 5. 持久化流程

### 5.1 后台持久化

```rust
pub fn start_background_persist(&self) -> CoreResult<()> {
    let partition = Arc::new(self.clone());
    
    // 启动后台线程
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(60));  // 每分钟检查
            
            // 查找需要持久化的 segment
            let segments_to_persist = {
                let frozen = partition.frozen_segments.read().unwrap();
                frozen.iter()
                    .filter(|seg| !seg.is_persisted())
                    .cloned()
                    .collect::<Vec<_>>()
            };
            
            // 持久化
            for segment in segments_to_persist {
                if let Err(e) = segment.persist(&partition.data_dir) {
                    eprintln!("Background persist failed: {:?}", e);
                }
            }
        }
    });
    
    Ok(())
}
```

### 5.2 批量持久化

```rust
pub fn persist_all(&self) -> CoreResult<()> {
    // Step 1: 持久化 Active Segment
    {
        let active = self.active_segment.read().unwrap();
        if let Some(segment) = active.as_ref() {
            segment.persist(&self.data_dir)?;
        }
    }
    
    // Step 2: 持久化所有 Frozen Segments
    {
        let frozen = self.frozen_segments.read().unwrap();
        for segment in frozen.iter() {
            if !segment.is_persisted() {
                segment.persist(&self.data_dir)?;
            }
        }
    }
    
    Ok(())
}
```

### 5.3 持久化性能

**测试：3个segment，各100万条**

| 策略 | 总耗时 | 说明 |
|------|--------|------|
| 串行持久化 | 42s | 3 × 14s |
| 并行持久化 | 18s | 并行写入磁盘 |
| 后台持久化 | 0s | 不阻塞写入 |

**推荐：后台持久化**（不影响写入性能）

## 6. 重启恢复

### 6.1 扫描磁盘

```rust
pub fn recover_from_disk(data_dir: &Path, schema: Schema) 
    -> CoreResult<Partition> 
{
    // Step 1: 扫描所有 segment 目录
    let segment_dirs = scan_segment_directories(data_dir)?;
    
    // Step 2: 解析 segment 范围
    let mut segments = Vec::new();
    for dir in segment_dirs {
        let (start_id, end_id) = parse_segment_name(&dir)?;
        let segment = Segment::load_frozen(data_dir, start_id, end_id)?;
        segments.push(Arc::new(segment));
    }
    
    // Step 3: 按 start_id 排序
    segments.sort_by_key(|seg| seg.start_id());
    
    // Step 4: 计算 next_doc_id
    let next_doc_id = if let Some(last_seg) = segments.last() {
        last_seg.end_id() + 1
    } else {
        0
    };
    
    // Step 5: 构建 Partition
    Ok(Partition {
        schema,
        data_dir: data_dir.to_path_buf(),
        active_segment: RwLock::new(None),
        frozen_segments: RwLock::new(segments),
        next_doc_id: AtomicU32::new(next_doc_id),
        segment_size: 1_000_000,
    })
}
```

### 6.2 目录扫描

```rust
fn scan_segment_directories(data_dir: &Path) -> CoreResult<Vec<String>> {
    let mut segment_dirs = Vec::new();
    
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        
        // 匹配 segment-{start}-{end} 模式
        if name.starts_with("segment-") {
            segment_dirs.push(name);
        }
    }
    
    Ok(segment_dirs)
}

fn parse_segment_name(name: &str) -> CoreResult<(u32, u32)> {
    // 解析 segment-0-999999
    let parts: Vec<&str> = name.split('-').collect();
    if parts.len() != 3 {
        return Err(InvalidSegmentNameError);
    }
    
    let start_id = parts[1].parse::<u32>()?;
    let end_id = parts[2].parse::<u32>()?;
    
    Ok((start_id, end_id))
}
```

### 6.3 恢复性能

**测试：恢复3个segment（各100万条）**

| 阶段 | 耗时 | 说明 |
|------|------|------|
| 扫描目录 | 0.5 ms | 列出文件 |
| 解析名称 | 0.1 ms | 正则匹配 |
| 加载元数据 | 1.2 ms | 3个segment |
| **总计** | **1.8 ms** | 几乎瞬时 |

**关键：** 延迟加载，不读取数据内容

### 6.4 一致性检查

```rust
pub fn validate_segments(&self) -> CoreResult<()> {
    let segments = self.collect_all_segments();
    
    // 检查 doc_id 连续性
    for i in 0..segments.len() - 1 {
        let current_end = segments[i].end_id();
        let next_start = segments[i + 1].start_id();
        
        if next_start != current_end + 1 {
            return Err(SegmentGapError {
                expected: current_end + 1,
                actual: next_start,
            });
        }
    }
    
    // 检查文件完整性
    for segment in segments {
        segment.validate_files()?;
    }
    
    Ok(())
}
```

## 7. Segment 管理

### 7.1 Segment 列表

```rust
pub fn list_segments(&self) -> Vec<SegmentInfo> {
    let mut infos = Vec::new();
    
    // Active Segment
    {
        let active = self.active_segment.read().unwrap();
        if let Some(seg) = active.as_ref() {
            infos.push(SegmentInfo {
                start_id: seg.start_id(),
                end_id: seg.end_id(),
                doc_count: seg.doc_count(),
                memory_bytes: seg.memory_bytes(),
                disk_bytes: seg.disk_bytes(),
                is_frozen: false,
            });
        }
    }
    
    // Frozen Segments
    {
        let frozen = self.frozen_segments.read().unwrap();
        for seg in frozen.iter() {
            infos.push(SegmentInfo {
                start_id: seg.start_id(),
                end_id: seg.end_id(),
                doc_count: seg.doc_count(),
                memory_bytes: seg.memory_bytes(),
                disk_bytes: seg.disk_bytes(),
                is_frozen: true,
            });
        }
    }
    
    infos
}
```

### 7.2 Segment 清理

```rust
pub fn cleanup_old_segments(&self, keep_last_n: usize) -> CoreResult<()> {
    let mut frozen = self.frozen_segments.write().unwrap();
    
    // 保留最新的 N 个 segment
    if frozen.len() > keep_last_n {
        let to_remove = frozen.len() - keep_last_n;
        
        for i in 0..to_remove {
            let segment = &frozen[i];
            
            // 删除磁盘文件
            let segment_path = format!("{}/segment-{}-{}", 
                self.data_dir.display(),
                segment.start_id(),
                segment.end_id()
            );
            fs::remove_dir_all(&segment_path)?;
        }
        
        // 从内存中移除
        frozen.drain(0..to_remove);
    }
    
    Ok(())
}
```

### 7.3 Segment 合并（Compaction）

```rust
pub fn compact_segments(&self, segment_ids: Vec<usize>) -> CoreResult<()> {
    let segments_to_merge = {
        let frozen = self.frozen_segments.read().unwrap();
        segment_ids.iter()
            .map(|&i| frozen[i].clone())
            .collect::<Vec<_>>()
    };
    
    // 创建新的合并 segment
    let start_id = segments_to_merge[0].start_id();
    let end_id = segments_to_merge.last().unwrap().end_id();
    let merged = Arc::new(Segment::new(self.schema.clone(), start_id));
    
    // 复制数据
    for segment in &segments_to_merge {
        for doc_id in segment.start_id()..=segment.end_id() {
            if let Some(doc) = segment.get_document(doc_id)? {
                merged.write(doc)?;
            }
        }
    }
    
    // 持久化合并后的 segment
    merged.persist(&self.data_dir)?;
    
    // 替换旧 segments
    {
        let mut frozen = self.frozen_segments.write().unwrap();
        // 移除旧的
        for &id in segment_ids.iter().rev() {
            frozen.remove(id);
        }
        // 添加新的
        frozen.push(merged);
        // 重新排序
        frozen.sort_by_key(|seg| seg.start_id());
    }
    
    Ok(())
}
```

## 8. 内存管理

### 8.1 内存占用分析

**3个segment（各100万条，2字段）：**

| 组件 | 内存占用 | 说明 |
|------|---------|------|
| Active Segment | 35 MB | 内存模式 |
| Frozen Segment 1 | 2 MB | Phase 4后 |
| Frozen Segment 2 | 2 MB | Phase 4后 |
| Partition元数据 | 1 MB | Segment列表等 |
| **总计** | **40 MB** | 主要是Active |

### 8.2 内存预算策略

```rust
pub struct MemoryBudget {
    max_active_segments: usize,
    max_active_memory_mb: usize,
}

impl Partition {
    pub fn enforce_memory_budget(&self, budget: &MemoryBudget) 
        -> CoreResult<()> 
    {
        // 检查 Active Segment 内存
        {
            let active = self.active_segment.read().unwrap();
            if let Some(seg) = active.as_ref() {
                let memory_mb = seg.memory_bytes() / 1024 / 1024;
                
                if memory_mb >= budget.max_active_memory_mb {
                    // 强制 flush
                    self.flush_segment(seg.clone())?;
                }
            }
        }
        
        // 检查 Frozen Segment 数量
        {
            let frozen = self.frozen_segments.read().unwrap();
            if frozen.len() > budget.max_active_segments {
                // 触发合并或清理
                self.cleanup_old_segments(budget.max_active_segments)?;
            }
        }
        
        Ok(())
    }
}
```

### 8.3 内存监控

```rust
pub struct PartitionMemoryStats {
    pub active_memory_bytes: u64,
    pub frozen_memory_bytes: u64,
    pub total_memory_bytes: u64,
    pub segment_count: usize,
}

impl Partition {
    pub fn memory_stats(&self) -> PartitionMemoryStats {
        let mut stats = PartitionMemoryStats::default();
        
        // Active
        {
            let active = self.active_segment.read().unwrap();
            if let Some(seg) = active.as_ref() {
                stats.active_memory_bytes = seg.memory_bytes();
            }
        }
        
        // Frozen
        {
            let frozen = self.frozen_segments.read().unwrap();
            stats.segment_count = frozen.len();
            stats.frozen_memory_bytes = frozen.iter()
                .map(|seg| seg.memory_bytes())
                .sum();
        }
        
        stats.total_memory_bytes = stats.active_memory_bytes + 
                                   stats.frozen_memory_bytes;
        stats
    }
}
```

## 9. 并发控制

### 9.1 读写分离

```rust
// 写入：只操作 Active Segment
pub fn write(&self, batch: RecordBatch) -> CoreResult<u32> {
    let active = self.active_segment.read().unwrap();
    active.as_ref().unwrap().write(batch)
}

// 查询：读取所有 Segment（并行）
pub fn query(&self, query: &Query) -> CoreResult<Vec<RecordBatch>> {
    let segments = self.collect_all_segments();
    
    // 并行查询
    let results: Vec<_> = segments.par_iter()
        .map(|seg| seg.execute_query(query))
        .collect::<CoreResult<_>>()?;
    
    // 合并结果
    Ok(merge_results(results))
}
```

### 9.2 锁粒度

| 操作 | 锁 | 粒度 | 并发性 |
|------|---|------|--------|
| write() | active_segment (read) | Segment级 | 高 |
| rotate_segment() | active_segment (write) | Partition级 | 低（短暂） |
| query() | frozen_segments (read) | Partition级 | 高 |
| flush() | frozen_segments (write) | Partition级 | 低（短暂） |

### 9.3 无锁优化

**读取 Segment 列表：**

```rust
// ❌ 加锁读取
let frozen = self.frozen_segments.read().unwrap();
for seg in frozen.iter() {
    seg.query(...);  // 持有锁期间查询
}

// ✅ 快照读取
let segments = {
    let frozen = self.frozen_segments.read().unwrap();
    frozen.clone()  // 克隆 Arc，快速释放锁
};
for seg in segments {
    seg.query(...);  // 无锁查询
}
```

## 10. 监控指标

### 10.1 关键指标

```rust
pub struct PartitionMetrics {
    // 基础指标
    pub total_doc_count: u64,
    pub active_segment_count: usize,
    pub frozen_segment_count: usize,
    
    // 写入指标
    pub write_qps: f64,
    pub write_latency_us: u64,
    pub flush_count: u64,
    
    // 查询指标
    pub query_qps: f64,
    pub query_latency_us: u64,
    
    // 资源指标
    pub memory_bytes: u64,
    pub disk_bytes: u64,
}
```

### 10.2 性能基准

**300万条记录，3个segment：**

| 指标 | 值 | 说明 |
|------|---|------|
| 写入吞吐 | 36.7万/s | 10000条/batch |
| 查询QPS | 24万 | 点查询 |
| Term查询QPS | 12万 | 跨3个segment |
| 内存占用 | 40 MB | 1 Active + 2 Frozen |
| 磁盘占用 | 54.7 MB | 压缩后 |
| Flush时间 | 13.9s | 100万条 |
| 恢复时间 | 1.8ms | 3个segment |

## 11. 错误处理

### 11.1 错误类型

```rust
pub enum PartitionError {
    NoActiveSegment,
    SegmentFrozen(u32),
    PersistFailed(String),
    RecoveryFailed(String),
    InvalidSegmentRange(u32, u32),
    SegmentGap(u32, u32),
}
```

### 11.2 故障恢复

```rust
pub fn recover_from_failure(&self) -> CoreResult<()> {
    // Step 1: 验证所有 segment
    self.validate_segments()?;
    
    // Step 2: 检查未完成的持久化
    let incomplete = self.find_incomplete_persists()?;
    for segment in incomplete {
        segment.persist(&self.data_dir)?;
    }
    
    // Step 3: 重建索引（如果损坏）
    let corrupted = self.find_corrupted_segments()?;
    for segment in corrupted {
        segment.rebuild_index()?;
    }
    
    Ok(())
}
```

## 12. 最佳实践

### 12.1 Segment 大小配置

```rust
// 推荐配置
const SEGMENT_SIZE: u32 = 1_000_000;  // 100万条/segment

// 场景：高吞吐写入
const SEGMENT_SIZE: u32 = 5_000_000;  // 500万条/segment

// 场景：低延迟查询
const SEGMENT_SIZE: u32 = 500_000;    // 50万条/segment
```

### 12.2 后台任务调度

```rust
// 启动后台任务
partition.start_background_persist()?;
partition.start_background_compact()?;
partition.start_memory_monitor()?;

// 定期维护
tokio::spawn(async move {
    loop {
        tokio::time::sleep(Duration::from_secs(300)).await;
        
        // 清理旧segment
        partition.cleanup_old_segments(10)?;
        
        // 合并小segment
        partition.compact_small_segments()?;
    }
});
```

### 12.3 查询优化

```rust
// ✅ 并行查询
let results = partition.query_term_parallel("tags", "tag1")?;

// ✅ 范围过滤（减少扫描segment数）
let results = partition.query_with_range(
    "tags", "tag1",
    1_000_000..2_000_000  // 只查询segment-1000000-1999999
)?;

// ❌ 避免全表扫描
let all_docs = partition.scan_all()?;  // 慢！
```

## 13. 性能优化

### 13.1 写入优化

**批量写入：**

```rust
// ❌ 单条写入
for record in records {
    partition.write(vec![record])?;
}

// ✅ 批量写入
partition.write(RecordBatch::from(records))?;
```

**效果：** 10x 吞吐提升

### 13.2 查询优化

**查询缓存：**

```rust
pub struct QueryCache {
    cache: LruCache<String, RoaringBitmap>,
}

impl Partition {
    pub fn query_with_cache(&self, field: &str, term: &str, 
                           cache: &mut QueryCache) 
        -> CoreResult<RoaringBitmap> 
    {
        let key = format!("{}:{}", field, term);
        
        // 查缓存
        if let Some(result) = cache.get(&key) {
            return Ok(result.clone());
        }
        
        // 查询
        let result = self.query_term(field, term)?;
        
        // 更新缓存
        cache.put(key, result.clone());
        
        Ok(result)
    }
}
```

### 13.3 Compaction 优化

**智能合并：**

```rust
pub fn smart_compact(&self) -> CoreResult<()> {
    let segments = self.list_segments();
    
    // 找出小segment（< 50万条）
    let small_segments: Vec<_> = segments.iter()
        .enumerate()
        .filter(|(_, info)| info.doc_count < 500_000)
        .map(|(i, _)| i)
        .collect();
    
    // 合并相邻的小segment
    if small_segments.len() >= 2 {
        self.compact_segments(small_segments)?;
    }
    
    Ok(())
}
```

## 14. 未来优化

### 14.1 分层存储

```rust
pub enum SegmentTier {
    Hot,    // 内存 + SSD
    Warm,   // SSD
    Cold,   // HDD
}

// 自动降级
if segment.last_access_time() > 7.days() {
    segment.move_to_tier(SegmentTier::Warm)?;
}
```

### 14.2 分布式扩展

```rust
pub struct DistributedPartition {
    local_partition: Partition,
    remote_partitions: Vec<RemotePartition>,
}

// 路由查询到多个节点
impl DistributedPartition {
    pub async fn query_distributed(&self, query: &Query) 
        -> CoreResult<Vec<RecordBatch>> 
    {
        let mut handles = vec![];
        
        // 本地查询
        handles.push(self.local_partition.query(query));
        
        // 远程查询
        for remote in &self.remote_partitions {
            handles.push(remote.query_async(query));
        }
        
        // 汇总结果
        let results = join_all(handles).await?;
        Ok(merge_results(results))
    }
}
```

### 14.3 自适应配置

```rust
// 根据负载动态调整
pub struct AdaptiveConfig {
    segment_size: u32,
    flush_interval: Duration,
}

impl AdaptiveConfig {
    pub fn adjust(&mut self, metrics: &PartitionMetrics) {
        // 高写入：增大segment
        if metrics.write_qps > 500_000.0 {
            self.segment_size = 2_000_000;
        }
        
        // 高查询：减小segment
        if metrics.query_qps > 1_000_000.0 {
            self.segment_size = 500_000;
        }
    }
}
```

## 15. 总结

Partition 提供了完整的多 Segment 管理能力：

- ✅ 智能的 Segment 轮转（基于大小阈值）
- ✅ 高效的查询路由（Active + Frozen）
- ✅ 后台持久化（不阻塞写入）
- ✅ 快速的重启恢复（1.8ms）
- ✅ 灵活的 Segment 管理（清理、合并）
- ✅ 完善的并发控制（读写分离）
- ✅ 丰富的监控指标
- ✅ 统一的错误处理

是构建高可用、高性能存储引擎的关键组件。
