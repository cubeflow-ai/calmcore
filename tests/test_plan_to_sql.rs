use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{col, lit, Expr, LogicalPlanBuilder};
use datafusion::sql::unparser::plan_to_sql;
use std::sync::Arc;

#[tokio::test]
async fn test_plan_to_sql_full_projection() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("user_id", DataType::UInt64, false),
        Field::new("ts_ms", DataType::Int64, false),
        Field::new("source", DataType::Utf8, false),
    ]));

    // 1. 完整投影：SELECT event_id, user_id, ts_ms, source FROM table
    let plan = LogicalPlanBuilder::scan(
        "label_event_v1_mix",
        datafusion::datasource::provider_as_source(Arc::new(
            datafusion::datasource::empty::EmptyTable::new(schema.clone()),
        )),
        None,
    )
    .unwrap()
    .project(vec![
        col("event_id"),
        col("user_id"),
        col("ts_ms"),
        col("source"),
    ])
    .unwrap()
    .build()
    .unwrap();

    let sql = plan_to_sql(&plan).unwrap();
    println!("\n=== 完整投影 ===");
    println!("{}", sql);
}

#[tokio::test]
async fn test_plan_to_sql_constant_projection() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("user_id", DataType::UInt64, false),
        Field::new("ts_ms", DataType::Int64, false),
        Field::new("source", DataType::Utf8, false),
    ]));

    // 2. 常量投影：SELECT 1 FROM table
    let plan = LogicalPlanBuilder::scan(
        "label_event_v1_mix",
        datafusion::datasource::provider_as_source(Arc::new(
            datafusion::datasource::empty::EmptyTable::new(schema.clone()),
        )),
        None,
    )
    .unwrap()
    .project(vec![lit(1i32)])
    .unwrap()
    .build()
    .unwrap();

    let sql = plan_to_sql(&plan).unwrap();
    println!("\n=== 常量投影 SELECT 1 ===");
    println!("{}", sql);
}

#[tokio::test]
async fn test_plan_to_sql_empty_projection() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("user_id", DataType::UInt64, false),
        Field::new("ts_ms", DataType::Int64, false),
        Field::new("source", DataType::Utf8, false),
    ]));

    // 3. 空投影：project(vec![])
    let plan = LogicalPlanBuilder::scan(
        "label_event_v1_mix",
        datafusion::datasource::provider_as_source(Arc::new(
            datafusion::datasource::empty::EmptyTable::new(schema.clone()),
        )),
        None,
    )
    .unwrap()
    .project(Vec::<Expr>::new())
    .unwrap()
    .build()
    .unwrap();

    let sql = plan_to_sql(&plan).unwrap();
    println!("\n=== 空投影 project([]) ===");
    println!("{}", sql);
}

#[tokio::test]
async fn test_plan_to_sql_count_aggregate() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("user_id", DataType::UInt64, false),
        Field::new("ts_ms", DataType::Int64, false),
        Field::new("source", DataType::Utf8, false),
    ]));

    // 4. COUNT(*) 聚合
    let plan = LogicalPlanBuilder::scan(
        "label_event_v1_mix",
        datafusion::datasource::provider_as_source(Arc::new(
            datafusion::datasource::empty::EmptyTable::new(schema.clone()),
        )),
        None,
    )
    .unwrap()
    .aggregate(
        Vec::<Expr>::new(),  // 无 GROUP BY
        vec![count(lit(1))], // COUNT(1)
    )
    .unwrap()
    .build()
    .unwrap();

    let sql = plan_to_sql(&plan).unwrap();
    println!("\n=== COUNT(*) 聚合 ===");
    println!("{}", sql);
}

#[tokio::test]
async fn test_plan_to_sql_no_projection() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("user_id", DataType::UInt64, false),
        Field::new("ts_ms", DataType::Int64, false),
        Field::new("source", DataType::Utf8, false),
    ]));

    // 5. 无投影：直接 scan（相当于 SELECT * FROM table）
    let plan = LogicalPlanBuilder::scan(
        "label_event_v1_mix",
        datafusion::datasource::provider_as_source(Arc::new(
            datafusion::datasource::empty::EmptyTable::new(schema.clone()),
        )),
        None,
    )
    .unwrap()
    .build()
    .unwrap();

    let sql = plan_to_sql(&plan).unwrap();
    println!("\n=== 无投影（SELECT *） ===");
    println!("{}", sql);
}
