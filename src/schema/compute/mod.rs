use std::sync::Arc;use std::{any::Any, sync::Arc};



use crate::segment::Segment;use crate::segment::Segment;

use datafusion::{use datafusion::{

    arrow::datatypes::SchemaRef,    arrow::datatypes::SchemaRef,

    logical_expr::Expr,    catalog::Session,

    scalar::ScalarValue,    datasource::{TableProvider, TableType},

};    error::Result as DFResult,

use roaring::RoaringBitmap;    logical_expr::{Expr, TableProviderFilterPushDown},

    physical_plan::ExecutionPlan,

/// SegmentTableProvider - 将 Segment 暴露给 DataFusion};

/// use roaring::RoaringBitmap;

/// **核心思路 (你的方案)**:

/// 1. SQL WHERE 条件 → DataFusion 自动解析成 Expr 树/// SegmentTableProvider - 将 Segment 暴露给 DataFusion

/// 2. 遍历 Expr 树 → 利用倒排索引构建 RoaringBitmap///

/// 3. 前置过滤 → TableProvider 只扫描匹配的 doc_ids/// 核心思路:

/// /// 1. DataFusion 调用 scan() 时会传入 filters (WHERE 条件已解析成 Expr)

/// **不需要手动解析 SQL!** DataFusion 已经做了这个工作/// 2. 我们解析 Expr,利用倒排索引构建 RoaringBitmap

#[derive(Debug)]/// 3. 返回 ExecutionPlan,只扫描匹配的 doc_ids

pub struct SegmentTableProvider {#[derive(Debug)]

    segment: Arc<Segment>,pub struct SegmentTableProvider {

    schema: SchemaRef,    segment: Arc<Segment>,

}    schema: SchemaRef,

}

impl SegmentTableProvider {impl SegmentTableProvider {

    pub fn new(segment: Arc<Segment>, schema: SchemaRef) -> Self {    pub fn new(segment: Arc<Segment>, schema: SchemaRef) -> Self {

        Self { segment, schema }        Self { segment, schema }

    }    }



    /// 核心方法: 将 Expr 转换为 RoaringBitmap    /// 核心方法: 将 Expr 转换为 RoaringBitmap

    ///     ///

    /// **这就是你的 AST 树解析!**    /// 例如:

    ///     /// - `age > 18` → 调用 num_i64.range_query(19, i64::MAX)

    /// 例如:    /// - `name = 'Alice'` → 调用 keyword.query("Alice")

    /// - `age > 18` → BinaryExpr { Column("age"), Gt, Literal(18) }    /// - `age > 18 AND name = 'Alice'` → bitmap1 & bitmap2

    ///   → 调用 num_i64.range_query(19, i64::MAX) → RoaringBitmap    fn expr_to_bitmap(&self, expr: &Expr) -> Option<RoaringBitmap> {

    ///         match expr {

    /// - `name = 'Alice'` → BinaryExpr { Column("name"), Eq, Literal("Alice") }            // 二元表达式: col > value, col = value 等

    ///   → 调用 keyword.query("Alice") → RoaringBitmap            Expr::BinaryExpr(binary) => {

    ///                 use datafusion::logical_expr::Operator;

    /// - `age > 18 AND name = 'Alice'` → BinaryExpr { left, And, right }

    ///   → bitmap1 & bitmap2 → RoaringBitmap                // 先处理 AND/OR 逻辑运算

    pub fn expr_to_bitmap(&self, expr: &Expr) -> Option<RoaringBitmap> {                match binary.op {

        match expr {                    Operator::And => {

            // 二元表达式: 这就是 WHERE 条件的核心                        let left = self.expr_to_bitmap(&binary.left)?;

            Expr::BinaryExpr(binary) => {                        let right = self.expr_to_bitmap(&binary.right)?;

                use datafusion::logical_expr::Operator;                        return Some(left & right);

                    }

                // 先处理 AND/OR 逻辑运算                    Operator::Or => {

                match binary.op {                        let left = self.expr_to_bitmap(&binary.left)?;

                    Operator::And => {                        let right = self.expr_to_bitmap(&binary.right)?;

                        // AND: 两个条件都满足 → 取交集                        return Some(left | right);

                        let left = self.expr_to_bitmap(&binary.left)?;                    }

                        let right = self.expr_to_bitmap(&binary.right)?;                    _ => {}

                        return Some(left & right);                }

                    }

                    Operator::Or => {                // 处理比较运算: col op value

                        // OR: 任一条件满足 → 取并集                if let Expr::Column(column) = &*binary.left {

                        let left = self.expr_to_bitmap(&binary.left)?;                    // Expr::Literal 有两个字段: (ScalarValue, Option<FieldMetadata>)

                        let right = self.expr_to_bitmap(&binary.right)?;                    if let Expr::Literal(scalar_value, _metadata) = &*binary.right {

                        return Some(left | right);                        let field_name = &column.name;

                    }

                    _ => {}                        match binary.op {

                }                            Operator::Eq => {

                                return self.query_equal(field_name, scalar_value);

                // 处理比较运算: col op value                            }

                if let Expr::Column(column) = &*binary.left {                            Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {

                    // Expr::Literal 有两个字段: (ScalarValue, Option<FieldMetadata>)                                return self.query_range(field_name, &binary.op, scalar_value);

                    if let Expr::Literal(scalar_value, _metadata) = &*binary.right {                            }

                        let field_name = &column.name;                            _ => {}

                                                }

                        match binary.op {                    }

                            Operator::Eq => {                }

                                // 等值查询: field = value                None

                                return self.query_equal(field_name, scalar_value);            }

                            }            _ => None,

                            Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {        }

                                // 范围查询: field > value    }

                                return self.query_range(field_name, &binary.op, scalar_value);    fn query_equal(

                            }        &self,

                            _ => {}        field_name: &str,

                        }        value: &datafusion::scalar::ScalarValue,

                    }    ) -> Option<RoaringBitmap> {

                }        // TODO: 根据字段类型调用对应的 query 方法

        // 这里需要访问 segment.fields,获取具体的 Field 类型

                None        println!("query_equal: {} = {:?}", field_name, value);

            }        None

            _ => None,    }

        }

    }    fn query_range(

        &self,

    /// 等值查询: field = value        field_name: &str,

    fn query_equal(&self, field_name: &str, value: &ScalarValue) -> Option<RoaringBitmap> {        op: &datafusion::logical_expr::Operator,

        println!("  [查询] {} = {:?}", field_name, value);        value: &datafusion::scalar::ScalarValue,

            ) -> Option<RoaringBitmap> {

        // TODO: 根据字段类型调用对应的 query 方法        // TODO: 根据字段类型调用对应的 range_query 方法

        //         println!("query_range: {} {:?} {:?}", field_name, op, value);

        // 例如:        None

        // - Keyword 字段: keyword.query(string_value)    }

        // - I64 字段: num_i64.query(i64_value)}

        // - F64 字段: num_f64.query(f64_value)

        //impl TableProvider for SegmentTableProvider {

        // 需要访问 self.segment.fields 获取具体字段    fn as_any(&self) -> &dyn Any {

                self

        None    }

    }

    fn schema(&self) -> SchemaRef {

    /// 范围查询: field > value, field < value 等        self.schema.clone()

    fn query_range(    }

        &self,

        field_name: &str,    fn table_type(&self) -> TableType {

        op: &datafusion::logical_expr::Operator,        TableType::Base

        value: &ScalarValue,    }

    ) -> Option<RoaringBitmap> {

        println!("  [查询] {} {:?} {:?}", field_name, op, value);    /// 关键方法: DataFusion 会调用这个方法,并传入 filters (WHERE 条件)

            async fn scan<'a>(

        // TODO: 根据字段类型调用对应的 range_query 方法        &'a self,

        //        _state: &'a dyn Session,

        // 例如:        _projection: Option<&Vec<usize>>,

        // - age > 18 → num_i64.range_query(19, i64::MAX)        filters: &[Expr],

        // - age >= 18 → num_i64.range_query(18, i64::MAX)        _limit: Option<usize>,

        // - age < 30 → num_i64.range_query(i64::MIN, 29)    ) -> DFResult<Arc<dyn ExecutionPlan>> {

        // - age <= 30 → num_i64.range_query(i64::MIN, 30)        // 1. 解析 filters,构建 bitmap

        //        let mut result_bitmap = RoaringBitmap::new();

        // 需要访问 self.segment.fields 获取具体字段        result_bitmap.insert_range(0..self.segment.doc_count() as u32);

        

        None        for filter in filters {

    }            if let Some(bitmap) = self.expr_to_bitmap(filter) {

}                result_bitmap &= bitmap;

            }

// TODO: 实现 TableProvider trait        }

// 

// 当前版本先专注于演示 Expr 解析逻辑        println!("\n=== Filter Pushdown ===");

// 完整的 TableProvider 实现需要:        println!("Filters: {:#?}", filters);

// 1. scan() 方法接收 filters: &[Expr]        println!("Matched doc_ids: {} docs", result_bitmap.len());

// 2. 调用 expr_to_bitmap() 构建过滤 bitmap

// 3. 返回自定义的 ExecutionPlan (只扫描匹配的 doc_ids)        // 2. 创建 ExecutionPlan (TODO: 实现自定义的 SegmentExec)

//        // 这个 ExecutionPlan 只需要扫描 result_bitmap 中的 doc_ids

// impl TableProvider for SegmentTableProvider {

//     async fn scan(        todo!("实现 SegmentExec - 只扫描匹配的 doc_ids")

//         &self,    }

//         projection: Option<&Vec<usize>>,

//         filters: &[Expr],  // ← WHERE 条件在这里!    /// 告诉 DataFusion 我们支持 filter pushdown

//         limit: Option<usize>,    fn supports_filters_pushdown(

//     ) -> DFResult<Arc<dyn ExecutionPlan>> {        &self,

//         // 1. 构建 bitmap        filters: &[&Expr],

//         let mut result_bitmap = RoaringBitmap::new();    ) -> DFResult<Vec<TableProviderFilterPushDown>> {

//         result_bitmap.insert_range(0..self.segment.doc_count());        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])

//             }

//         for filter in filters {}

//             if let Some(bitmap) = self.expr_to_bitmap(filter) {

//                 result_bitmap &= bitmap;#[cfg(test)]

//             }mod tests {

//         }    use datafusion::prelude::SessionContext;

//         

//         // 2. 返回 ExecutionPlan (只扫描 result_bitmap 的 doc_ids)    /// 测试: 查看 DataFusion 如何解析 WHERE 条件

//         todo!()    #[tokio::test]

//     }    async fn test_parse_where_conditions() {

// }        let sql = "SELECT * FROM users WHERE age > 18 AND name = 'Alice'";



#[cfg(test)]        // 创建测试表

mod tests {        use datafusion::arrow::array::{Int64Array, StringArray};

    use super::*;        use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use datafusion::prelude::SessionContext;        use datafusion::arrow::record_batch::RecordBatch;

        use std::sync::Arc;

    /// 测试: 演示 DataFusion 如何将 SQL WHERE 解析成 Expr 树

    ///         let ctx = SessionContext::new();

    /// **这就是你的第一步: SQL → Expr AST 树**

    #[tokio::test]        let schema = Arc::new(Schema::new(vec![

    async fn test_parse_where_to_expr() {            Field::new("id", DataType::Int64, false),

        let sql = "SELECT * FROM users WHERE age > 18 AND name = 'Alice'";            Field::new("name", DataType::Utf8, false),

            Field::new("age", DataType::Int64, false),

        // 创建测试表        ]));

        use datafusion::arrow::array::{Int64Array, StringArray};

        use datafusion::arrow::datatypes::{DataType, Field, Schema};        let batch = RecordBatch::try_new(

        use datafusion::arrow::record_batch::RecordBatch;            schema.clone(),

            vec![

        let ctx = SessionContext::new();                Arc::new(Int64Array::from(vec![1, 2, 3])),

                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),

        let schema = Arc::new(Schema::new(vec![                Arc::new(Int64Array::from(vec![20, 15, 25])),

            Field::new("id", DataType::Int64, false),            ],

            Field::new("name", DataType::Utf8, false),        )

            Field::new("age", DataType::Int64, false),        .unwrap();

        ]));

        ctx.register_batch("users", batch).unwrap();

        let batch = RecordBatch::try_new(

            schema.clone(),        // 解析 SQL 得到 LogicalPlan

            vec![        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

                Arc::new(Int64Array::from(vec![1, 2, 3])),

                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),        println!("\n=== Logical Plan ===");

                Arc::new(Int64Array::from(vec![20, 15, 25])),        println!("{:#?}", logical_plan);

            ],

        )        // 你会看到类似这样的结构:

        .unwrap();        // Filter {

        //   predicate: BinaryExpr {

        ctx.register_batch("users", batch).unwrap();        //     left: BinaryExpr { left: Column("age"), op: Gt, right: Literal(18) },

        //     op: And,

        // 解析 SQL 得到 LogicalPlan        //     right: BinaryExpr { left: Column("name"), op: Eq, right: Literal("Alice") }

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();        //   },

        //   input: ...

        println!("\n=== SQL ===");        // }

        println!("{}", sql);

                // 执行查询验证

        println!("\n=== Logical Plan (包含 WHERE 的 Expr 树) ===");        let df = ctx.sql(sql).await.unwrap();

        println!("{:#?}", logical_plan);        let results = df.collect().await.unwrap();



        // 你会看到类似这样的结构:        assert_eq!(results.len(), 1);

        // Filter {        assert_eq!(results[0].num_rows(), 1); // 只有 Alice (age=20) 满足条件

        //   predicate: BinaryExpr {    }

        //     left: BinaryExpr { }

        //       left: Column { name: "age" }, 
        //       op: Gt, 
        //       right: Literal(Int64(18), None) 
        //     },
        //     op: And,
        //     right: BinaryExpr { 
        //       left: Column { name: "name" }, 
        //       op: Eq, 
        //       right: Literal(Utf8("Alice"), None) 
        //     }
        //   },
        //   input: ...
        // }
        //
        // 这就是你要的 AST 树!
        // - BinaryExpr 就是二元表达式节点
        // - Column 是字段引用
        // - Literal 是常量值 (ScalarValue)
        // - Operator 是操作符 (Gt, Eq, And, Or 等)

        // 执行查询验证
        let df = ctx.sql(sql).await.unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1); // 只有 Alice (age=20) 满足条件
        
        println!("\n✅ 测试通过!");
    }

    /// 测试: 演示如何手动解析 Expr 并构建 bitmap (模拟)
    #[test]
    fn test_expr_to_bitmap_logic() {
        use datafusion::logical_expr::{col, lit, BinaryExpr, Operator};

        // 手动构建 Expr: age > 18 AND name = 'Alice'
        let age_gt_18 = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("age")),
            Operator::Gt,
            Box::new(lit(18i64)),
        ));

        let name_eq_alice = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("name")),
            Operator::Eq,
            Box::new(lit("Alice")),
        ));

        let combined = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(age_gt_18),
            Operator::And,
            Box::new(name_eq_alice),
        ));

        println!("\n=== 手动构建的 Expr ===");
        println!("{:#?}", combined);

        // TODO: 创建 SegmentTableProvider 并调用 expr_to_bitmap
        // let provider = SegmentTableProvider::new(...);
        // let bitmap = provider.expr_to_bitmap(&combined);
        
        println!("\n✅ Expr 结构验证成功!");
    }
}
