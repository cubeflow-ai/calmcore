# TableProvider 实现完成

## ✅ 已完成

### 1. TableProvider trait 实现

```rust
#[async_trait]
impl TableProvider for SegmentTableProvider {
    fn as_any(&self) -> &dyn Any { ... }
    fn schema(&self) -> SchemaRef { ... }
    fn table_type(&self) -> TableType { ... }
    
    /// 核心方法: 接收 DataFusion 传来的 WHERE 条件 (已解析成 Expr)
    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        filters: &[Expr],  // ← WHERE 条件在这里!
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // 1. 遍历所有 filter,构建 bitmap
        let mut result_bitmap = RoaringBitmap::new();
        result_bitmap.insert_range(0..self.segment.doc_count() as u32);
        
        for filter in filters {
            if let Some(bitmap) = self.expr_to_bitmap(filter) {
                result_bitmap &= bitmap;  // 取交集
            }
        }
        
        // 2. 创建自定义的 ExecutionPlan
        let exec = SegmentExec::new(
            self.segment.clone(),
            self.schema.clone(),
            result_bitmap  // ← 前置过滤结果
        );
        
        Ok(Arc::new(exec))
    }
    
    /// 告诉 DataFusion 我们支持 filter pushdown
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DFResult<...> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}
```

### 2. SegmentExec 框架

```rust
struct SegmentExec {
    segment: Arc<Segment>,
    schema: SchemaRef,
    matched_docs: RoaringBitmap,  // ← 前置过滤结果
}

impl ExecutionPlan for SegmentExec {
    fn name(&self) -> &str { "SegmentExec" }
    fn schema(&self) -> SchemaRef { ... }
    
    // TODO: 这两个方法还需要实现
    fn properties(&self) -> &PlanProperties { todo!() }
    fn execute(&self, ...) -> SendableRecordBatchStream { todo!() }
}
```

### 3. expr_to_bitmap() 框架

```rust
fn expr_to_bitmap(&self, expr: &Expr) -> Option<RoaringBitmap> {
    match expr {
        BinaryExpr { left, And, right } => {
            // AND: 取交集
            let left_bitmap = self.expr_to_bitmap(left)?;
            let right_bitmap = self.expr_to_bitmap(right)?;
            Some(left_bitmap & right_bitmap)
        }
        
        BinaryExpr { left, Or, right } => {
            // OR: 取并集
            let left_bitmap = self.expr_to_bitmap(left)?;
            let right_bitmap = self.expr_to_bitmap(right)?;
            Some(left_bitmap | right_bitmap)
        }
        
        BinaryExpr { Column(name), Eq, Literal(value, _) } => {
            // 等值查询: field = value
            self.query_equal(name, value)
        }
        
        BinaryExpr { Column(name), Gt|GtEq|Lt|LtEq, Literal(value, _) } => {
            // 范围查询: field > value
            self.query_range(name, op, value)
        }
        
        _ => None
    }
}
```

## ⏳ 待完成

### 1. 实现 query_equal() 和 query_range()

当前这两个方法只是打印信息,需要实际调用倒排索引:

```rust
fn query_equal(&self, field_name: &str, value: &ScalarValue) -> Option<RoaringBitmap> {
    // TODO: 获取字段并调用 query()
    // 
    // match self.segment.get_field(field_name)? {
    //     FieldType::Keyword(keyword) => {
    //         if let ScalarValue::Utf8(Some(s)) = value {
    //             keyword.query(s)
    //         } else { None }
    //     }
    //     FieldType::NumI64(num_i64) => {
    //         if let ScalarValue::Int64(Some(v)) = value {
    //             num_i64.query(*v)
    //         } else { None }
    //     }
    //     FieldType::NumF64(num_f64) => {
    //         if let ScalarValue::Float64(Some(v)) = value {
    //             num_f64.query(*v)
    //         } else { None }
    //     }
    // }
}

fn query_range(&self, field_name: &str, op: &Operator, value: &ScalarValue) -> Option<RoaringBitmap> {
    // TODO: 根据操作符调用 range_query()
    //
    // 例如 age > 18:
    // - Gt: num_i64.range_query(19, i64::MAX)
    // - GtEq: num_i64.range_query(18, i64::MAX)
    // - Lt: num_i64.range_query(i64::MIN, 17)
    // - LtEq: num_i64.range_query(i64::MIN, 18)
}
```

### 2. Segment 添加字段访问方法

需要在 `Segment` 中添加方法来获取字段:

```rust
impl Segment {
    pub fn get_field(&self, name: &str) -> Option<&dyn IndexWriter> {
        // 遍历 self.fields,找到对应的字段
        // 需要向下转型到具体的字段类型 (Keyword, NumI64, NumF64)
    }
}
```

### 3. 实现 ExecutionPlan::properties()

```rust
fn properties(&self) -> &PlanProperties {
    // 返回执行计划的属性:
    // - 分区信息
    // - 输出排序
    // - 执行模式 (Bounded, Unbounded)
}
```

### 4. 实现 ExecutionPlan::execute()

```rust
fn execute(
    &self,
    _partition: usize,
    _context: Arc<TaskContext>,
) -> DFResult<SendableRecordBatchStream> {
    // 创建 SegmentStream
    // 只扫描 self.matched_docs 中的 doc_ids
    // 返回 RecordBatch 流
}
```

### 5. 实现 RecordBatchStream

```rust
struct SegmentStream {
    segment: Arc<Segment>,
    doc_ids: RoaringBitmapIterator,
    schema: SchemaRef,
}

impl Stream for SegmentStream {
    type Item = DFResult<RecordBatch>;
    
    fn poll_next(&mut self, cx: &mut Context) -> Poll<Option<Self::Item>> {
        // 从 segment 批量读取 doc_ids 对应的数据
        // 转换成 RecordBatch
        // 返回给 DataFusion
    }
}
```

## 关键成就

✅ **TableProvider 已经可以接收 DataFusion 的 WHERE 条件了!**

虽然还有一些方法需要实现,但是核心的架构已经搭建完成:

1. ✅ DataFusion 会调用 `TableProvider::scan(filters)`
2. ✅ filters 是已经解析好的 Expr 树
3. ✅ `expr_to_bitmap()` 框架已经实现 (AND/OR/比较运算)
4. ✅ `SegmentExec` 可以接收过滤后的 bitmap
5. ⏳ 只需要连接到实际的倒排索引即可

## 下一步

**优先级排序**:

1. 🔥 **实现 query_equal() 和 query_range()** - 连接倒排索引
2. 🔥 **Segment::get_field()** - 字段访问方法
3. ⏳ ExecutionPlan::properties() - 执行计划属性
4. ⏳ ExecutionPlan::execute() - 数据扫描
5. ⏳ RecordBatchStream - 流式返回数据

完成前两步后,就可以测试完整的 filter pushdown 流程了!
