# WHERE 条件前置过滤方案

## 你的目标

1. **解析 SQL WHERE 条件** → 获取过滤条件
2. **利用倒排索引** → 构建 RoaringBitmap (符合条件的 doc_ids)
3. **前置过滤** → TableProvider 只返回匹配的行

## ✅ 推荐方案: 利用 DataFusion 的 Expr 树

### 核心思路

**不需要手动解析 SQL!** DataFusion 已经为你做了这个工作。

```
SQL String 
  ↓ (DataFusion 自动解析)
LogicalPlan (包含 Filter 节点)
  ↓
Expr 树 (WHERE 条件的 AST)
  ↓ (你的工作: 遍历 Expr)
RoaringBitmap (倒排索引查询结果)
  ↓
ExecutionPlan (只扫描匹配的 doc_ids)
```

## 实现步骤

### 第一步: 理解 Expr 结构 ✅ 已完成

DataFusion 将 WHERE 条件解析成 `Expr` 枚举:

```rust
pub enum Expr {
    // 二元表达式: col op value, expr AND expr 等
    BinaryExpr(BinaryExpr {
        left: Box<Expr>,
        op: Operator,      // Eq, Gt, GtEq, Lt, LtEq, And, Or 等
        right: Box<Expr>,
    }),
    
    // 列引用
    Column(Column {
        name: String,
        relation: Option<TableReference>,
    }),
    
    // 常量值
    Literal(
        ScalarValue,           // Int64(18), Utf8("Alice"), Float64(3.14) 等
        Option<FieldMetadata>
    ),
    
    // ... 其他类型
}
```

### 示例: `age > 18 AND name = 'Alice'`

```rust
BinaryExpr {
    left: BinaryExpr {
        left: Column { name: "age" },
        op: Gt,
        right: Literal(Int64(18), None)
    },
    op: And,
    right: BinaryExpr {
        left: Column { name: "name" },
        op: Eq,
        right: Literal(Utf8("Alice"), None)
    }
}
```

**已验证**: 运行 `cargo test --lib schema::compute::tests::test_parse_where_conditions` 可以看到完整的 Expr 树结构。

### 第二步: 实现 expr_to_bitmap() ⏳ 下一步

遍历 Expr 树,调用倒排索引,构建 RoaringBitmap:

```rust
impl SegmentTableProvider {
    fn expr_to_bitmap(&self, expr: &Expr) -> Option<RoaringBitmap> {
        match expr {
            // 处理 AND: 两个条件都满足 → 取交集
            BinaryExpr { left, op: And, right } => {
                let left_bitmap = self.expr_to_bitmap(left)?;
                let right_bitmap = self.expr_to_bitmap(right)?;
                Some(left_bitmap & right_bitmap)
            }
            
            // 处理 OR: 任一条件满足 → 取并集
            BinaryExpr { left, op: Or, right } => {
                let left_bitmap = self.expr_to_bitmap(left)?;
                let right_bitmap = self.expr_to_bitmap(right)?;
                Some(left_bitmap | right_bitmap)
            }
            
            // 处理等值查询: field = value
            BinaryExpr { 
                left: Column { name },
                op: Eq,
                right: Literal(value, _)
            } => {
                self.query_equal(name, value)
            }
            
            // 处理范围查询: field > value, field >= value 等
            BinaryExpr {
                left: Column { name },
                op: Gt | GtEq | Lt | LtEq,
                right: Literal(value, _)
            } => {
                self.query_range(name, op, value)
            }
            
            _ => None
        }
    }
    
    fn query_equal(&self, field_name: &str, value: &ScalarValue) -> Option<RoaringBitmap> {
        // 根据字段类型调用对应的 query 方法
        match self.segment.get_field(field_name)? {
            Field::Keyword(keyword) => {
                if let ScalarValue::Utf8(Some(s)) = value {
                    keyword.query(s)
                } else {
                    None
                }
            }
            Field::NumI64(num_i64) => {
                if let ScalarValue::Int64(Some(v)) = value {
                    num_i64.query(*v)
                } else {
                    None
                }
            }
            Field::NumF64(num_f64) => {
                if let ScalarValue::Float64(Some(v)) = value {
                    num_f64.query(*v)
                } else {
                    None
                }
            }
        }
    }
    
    fn query_range(&self, field_name: &str, op: &Operator, value: &ScalarValue) -> Option<RoaringBitmap> {
        // 根据操作符和字段类型调用对应的 range_query
        match self.segment.get_field(field_name)? {
            Field::NumI64(num_i64) => {
                if let ScalarValue::Int64(Some(v)) = value {
                    match op {
                        Operator::Gt => num_i64.range_query(v + 1, i64::MAX),
                        Operator::GtEq => num_i64.range_query(*v, i64::MAX),
                        Operator::Lt => num_i64.range_query(i64::MIN, v - 1),
                        Operator::LtEq => num_i64.range_query(i64::MIN, *v),
                        _ => None
                    }
                } else {
                    None
                }
            }
            Field::NumF64(num_f64) => {
                if let ScalarValue::Float64(Some(v)) = value {
                    match op {
                        Operator::Gt => num_f64.range_query(*v, f64::MAX),
                        Operator::GtEq => num_f64.range_query(*v, f64::MAX),
                        Operator::Lt => num_f64.range_query(f64::MIN, *v),
                        Operator::LtEq => num_f64.range_query(f64::MIN, *v),
                        _ => None
                    }
                } else {
                    None
                }
            }
            _ => None
        }
    }
}
```

### 第三步: 实现 TableProvider ⏳ 待完成

```rust
use datafusion::datasource::TableProvider;

impl TableProvider for SegmentTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    
    /// 关键方法: DataFusion 会调用这个方法,并传入 filters
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],  // ← WHERE 条件在这里!
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 1. 构建初始 bitmap (所有文档)
        let mut result_bitmap = RoaringBitmap::new();
        result_bitmap.insert_range(0..self.segment.doc_count() as u32);
        
        // 2. 遍历所有 filter,逐个应用
        for filter in filters {
            if let Some(bitmap) = self.expr_to_bitmap(filter) {
                result_bitmap &= bitmap;  // 取交集
            }
        }
        
        println!("Filter pushdown: {} docs matched", result_bitmap.len());
        
        // 3. 创建自定义的 ExecutionPlan
        // 这个 ExecutionPlan 只扫描 result_bitmap 中的 doc_ids
        let exec = SegmentExec::new(
            self.segment.clone(),
            self.schema.clone(),
            result_bitmap,  // ← 前置过滤结果
            projection,
            limit,
        );
        
        Ok(Arc::new(exec))
    }
    
    /// 告诉 DataFusion 我们支持 filter pushdown
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        // Exact 表示我们会完全处理这些 filter
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}
```

### 第四步: 实现 ExecutionPlan ⏳ 待完成

```rust
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream};

struct SegmentExec {
    segment: Arc<Segment>,
    schema: SchemaRef,
    matched_docs: RoaringBitmap,  // 前置过滤结果
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
}

impl ExecutionPlan for SegmentExec {
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        // 只扫描 matched_docs 中的 doc_ids
        let stream = SegmentStream::new(
            self.segment.clone(),
            self.matched_docs.clone(),
            self.projection.clone(),
            self.limit,
        );
        
        Ok(Box::pin(stream))
    }
    
    // ... 其他方法
}

struct SegmentStream {
    segment: Arc<Segment>,
    doc_ids: RoaringBitmapIterator,  // 只遍历匹配的 doc_ids
    // ...
}

impl RecordBatchStream for SegmentStream {
    fn poll_next(&mut self, cx: &mut Context) -> Poll<Option<Result<RecordBatch>>> {
        // 从 segment 读取 doc_ids 对应的数据
        // 使用 segment.get_docs(&self.doc_ids) 批量读取
        // ...
    }
}
```

## 性能优化

### 前置过滤的优势

```
传统方案:
  扫描 100万 docs → 加载所有数据 → 过滤 → 返回 1000 docs
  ❌ 浪费: 加载和传输了 99.9% 的无用数据

前置过滤方案:
  倒排索引查询 → 得到 1000 doc_ids → 只加载这 1000 docs → 返回
  ✅ 优势: 只加载需要的数据,减少 99.9% 的 I/O
```

### 倒排索引的查询复杂度

- **等值查询**: `O(1)` HashMap lookup
- **范围查询**: `O(log N + K)` BTree range scan
- **AND 操作**: `O(min(N, M))` Bitmap intersection
- **OR 操作**: `O(N + M)` Bitmap union

## 测试验证

### 当前进度

✅ **测试通过**: `test_parse_where_conditions`

- 验证了 DataFusion 正确解析 SQL WHERE 条件
- 打印了完整的 Expr 树结构
- 确认了 `BinaryExpr`, `Column`, `Literal` 的格式

### 运行测试

```bash
cargo test --lib schema::compute::tests::test_parse_where_conditions -- --nocapture
```

### 示例输出

```
=== SQL ===
SELECT * FROM users WHERE age > 18 AND name = 'Alice'

=== Logical Plan ===
Filter {
  predicate: BinaryExpr {
    left: BinaryExpr {
      left: Column { name: "age" },
      op: Gt,
      right: Literal(Int64(18), None)
    },
    op: And,
    right: BinaryExpr {
      left: Column { name: "name" },
      op: Eq,
      right: Literal(Utf8("Alice"), None)
    }
  },
  input: TableScan { ... }
}

✅ 测试通过!
```

## 总结

### 你的方案是正确的

1. ✅ **利用 AST 树** - DataFusion 已经提供了 Expr 树
2. ✅ **倒排索引查询** - 通过 expr_to_bitmap() 调用
3. ✅ **前置过滤** - TableProvider 的 scan() 方法

### 不需要手动解析 SQL

- ❌ 不要使用 `DFParser` 手动解析
- ❌ 不要使用 `sqlparser` crate
- ✅ 使用 `SessionContext.create_logical_plan()` 获取 Expr
- ✅ 使用 `TableProvider.scan(filters: &[Expr])` 接收 WHERE 条件

### 下一步工作

1. ⏳ 实现 `expr_to_bitmap()` 完整逻辑
2. ⏳ 添加 Segment 的字段访问方法 (`get_field()`)
3. ⏳ 实现 `TableProvider` trait
4. ⏳ 实现 `ExecutionPlan` 和 `RecordBatchStream`
5. ⏳ 编写端到端测试

## 参考代码位置

- **当前实现**: `src/schema/compute/mod.rs`
- **倒排索引**:
  - `src/segment/field_store/keyword.rs` - Keyword 字段的 query()
  - `src/segment/field_store/num_i64.rs` - I64 字段的 query() 和 range_query()
  - `src/segment/field_store/num_f64.rs` - F64 字段的 query() 和 range_query()
- **Segment**: `src/segment/mod.rs`

## 相关文档

- [DataFusion LogicalPlan](https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html)
- [DataFusion Expr](https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html)
- [DataFusion TableProvider](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html)
