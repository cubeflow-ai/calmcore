use calm::partition::Partition;
use calm::schema::field::FieldOption;
use calm::schema::{PersistPolicy, Schema};
use serde_json::json;
use std::{fs, path::PathBuf, sync::Arc, time::Instant};
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
struct TestRecord {
    id: u64,
    title: String,
    category: String,
    content: String,
    tags: String,
    score: i64,
    timestamp: i64,
}

fn generate_test_data(count: usize) -> Vec<TestRecord> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let categories = vec!["Technology", "Business", "Science", "Health", "Sports"];
    let tags_pool = vec!["news", "trending", "breaking", "analysis", "tutorial"];

    let mut records = Vec::new();
    let base_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    for i in 0..count {
        let category = categories[i % categories.len()].to_string();
        let tag_idx = (i * 7) % tags_pool.len();
        let tags = tags_pool[tag_idx].to_string();
        let content = format!("Article content {}. Lorem ipsum dolor sit amet", i);

        records.push(TestRecord {
            id: i as u64,
            title: format!("Article-{}-Title", i),
            category,
            content,
            tags,
            score: (i as i64 * 7) % 1000,
            timestamp: base_time as i64 - (i as i64 * 3600),
        });
    }

    records
}

mod calm_test {
    use super::*;

    pub struct CalmBenchmark {
        partition: Arc<Partition>,
    }

    impl CalmBenchmark {
        pub fn new(test_data_dir: &PathBuf, records: &[TestRecord]) -> Self {
            let schema = Schema {
                name: "benchmark".to_string(),
                primary_key: None,
                store_source: false,
                fields: vec![
                    FieldOption::I64 {
                        name: "id".to_string(),
                        index: true,
                    },
                    FieldOption::Keyword {
                        name: "title".to_string(),
                        index: true,
                        is_array: false,
                        persist_option: None,
                        case_sensitive: true,
                    },
                    FieldOption::Keyword {
                        name: "category".to_string(),
                        index: true,
                        is_array: false,
                        persist_option: None,
                        case_sensitive: true,
                    },
                    FieldOption::Keyword {
                        name: "content".to_string(),
                        index: false,
                        is_array: false,
                        persist_option: None,
                        case_sensitive: true,
                    },
                    FieldOption::Keyword {
                        name: "tags".to_string(),
                        index: true,
                        is_array: false,
                        persist_option: None,
                        case_sensitive: true,
                    },
                    FieldOption::I64 {
                        name: "score".to_string(),
                        index: true,
                    },
                    FieldOption::I64 {
                        name: "timestamp".to_string(),
                        index: true,
                    },
                ],
                persist_policy: PersistPolicy::default(),
            };

            let calm_dir = test_data_dir.join("calm_query_benchmark");
            fs::create_dir_all(&calm_dir).ok();

            let (tx, _rx) = mpsc::unbounded_channel();

            let partition = Arc::new(Partition::new(1, calm_dir, schema, tx));

            // 分批写入数据
            let batch_size = 10000;
            for chunk in records.chunks(batch_size) {
                let mut batch_data = Vec::new();
                for record in chunk {
                    batch_data.push(json!({
                        "id": record.id,
                        "title": record.title,
                        "category": record.category,
                        "content": record.content,
                        "tags": record.tags,
                        "score": record.score,
                        "timestamp": record.timestamp,
                    }));
                }
                let _ = partition.upsert_json(&batch_data);
                let _ = partition.flush(false);
            }

            // 持久化所有 segments
            loop {
                let unpersisted = partition.get_unpersisted_segments();
                if unpersisted.is_empty() {
                    break;
                }
                for (seg_id, _) in unpersisted {
                    let _ = partition.persist_segment(seg_id);
                }
            }

            CalmBenchmark { partition }
        }

        // 查询 1: 按 category 精确匹配
        pub fn query_by_category(&self, category: &str, iterations: usize) -> (u32, f64) {
            let start = Instant::now();
            let mut total_results = 0u32;

            for _ in 0..iterations {
                let where_clause = json!({
                    "category": category
                });
                if let Ok(results) = self.partition.query(&where_clause) {
                    total_results += results.len() as u32;
                }
            }

            let elapsed = start.elapsed().as_secs_f64();
            let qps = iterations as f64 / elapsed;
            (total_results, qps)
        }

        // 查询 2: 按 tag 精确匹配
        pub fn query_by_tag(&self, tag: &str, iterations: usize) -> (u32, f64) {
            let start = Instant::now();
            let mut total_results = 0u32;

            for _ in 0..iterations {
                let where_clause = json!({
                    "tags": tag
                });
                if let Ok(results) = self.partition.query(&where_clause) {
                    total_results += results.len() as u32;
                }
            }

            let elapsed = start.elapsed().as_secs_f64();
            let qps = iterations as f64 / elapsed;
            (total_results, qps)
        }

        // 查询 3: 按 score 范围查询
        pub fn query_by_score_range(&self, min: i64, max: i64, iterations: usize) -> (u32, f64) {
            let start = Instant::now();
            let mut total_results = 0u32;

            for _ in 0..iterations {
                let where_clause = json!({
                    "score": {
                        "$gte": min,
                        "$lte": max
                    }
                });
                if let Ok(results) = self.partition.query(&where_clause) {
                    total_results += results.len() as u32;
                }
            }

            let elapsed = start.elapsed().as_secs_f64();
            let qps = iterations as f64 / elapsed;
            (total_results, qps)
        }

        // 查询 4: 多条件 AND 查询
        pub fn query_multi_condition(
            &self,
            category: &str,
            tag: &str,
            iterations: usize,
        ) -> (u32, f64) {
            let start = Instant::now();
            let mut total_results = 0u32;

            for _ in 0..iterations {
                let where_clause = json!({
                    "category": category,
                    "tags": tag
                });
                if let Ok(results) = self.partition.query(&where_clause) {
                    total_results += results.len() as u32;
                }
            }

            let elapsed = start.elapsed().as_secs_f64();
            let qps = iterations as f64 / elapsed;
            (total_results, qps)
        }
    }
}

mod tantivy_test {
    use super::*;
    use tantivy::schema::{Schema, STORED, STRING, TEXT};
    use tantivy::Index;

    pub struct TantivyBenchmark {
        index: Index,
    }

    impl TantivyBenchmark {
        pub fn new(test_data_dir: &PathBuf, records: &[TestRecord]) -> Self {
            let index_dir = test_data_dir.join("tantivy_query_benchmark");
            fs::create_dir_all(&index_dir).ok();

            let mut schema_builder = Schema::builder();
            schema_builder.add_u64_field("id", STORED);
            schema_builder.add_text_field("title", TEXT | STORED);
            schema_builder.add_text_field("category", STRING | STORED);
            schema_builder.add_text_field("content", TEXT | STORED);
            schema_builder.add_text_field("tags", TEXT | STORED);
            schema_builder.add_i64_field("score", STORED);
            schema_builder.add_i64_field("timestamp", STORED);

            let schema = schema_builder.build();
            let index = Index::create_in_dir(&index_dir, schema).unwrap();

            if let Ok(mut index_writer) = index.writer(50_000_000) {
                let schema = index.schema();
                let id_field = schema.get_field("id").unwrap();
                let title_field = schema.get_field("title").unwrap();
                let category_field = schema.get_field("category").unwrap();
                let content_field = schema.get_field("content").unwrap();
                let tags_field = schema.get_field("tags").unwrap();
                let score_field = schema.get_field("score").unwrap();
                let timestamp_field = schema.get_field("timestamp").unwrap();

                for record in records {
                    use tantivy::doc;
                    let document = doc!(
                        id_field => record.id,
                        title_field => record.title.as_str(),
                        category_field => record.category.as_str(),
                        content_field => record.content.as_str(),
                        tags_field => record.tags.as_str(),
                        score_field => record.score,
                        timestamp_field => record.timestamp,
                    );
                    index_writer.add_document(document).ok();
                }
                index_writer.commit().ok();
            }

            TantivyBenchmark { index }
        }

        // 查询 1: 按 category 精确匹配
        pub fn query_by_category(&self, category: &str, iterations: usize) -> (u32, f64) {
            let start = Instant::now();
            let mut total_results = 0u32;

            let searcher = self.index.reader().unwrap().searcher();
            let schema = self.index.schema();
            let category_field = schema.get_field("category").unwrap();

            for _ in 0..iterations {
                let query =
                    tantivy::query::QueryParser::for_index(&self.index, vec![category_field])
                        .parse_query(category)
                        .ok();

                if let Some(query) = query {
                    if let Ok(top_docs) = searcher.search(&query, &tantivy::collector::Count) {
                        total_results += top_docs as u32;
                    }
                }
            }

            let elapsed = start.elapsed().as_secs_f64();
            let qps = iterations as f64 / elapsed;
            (total_results, qps)
        }

        // 查询 2: 按 tag 精确匹配
        pub fn query_by_tag(&self, tag: &str, iterations: usize) -> (u32, f64) {
            let start = Instant::now();
            let mut total_results = 0u32;

            let searcher = self.index.reader().unwrap().searcher();
            let schema = self.index.schema();
            let tags_field = schema.get_field("tags").unwrap();

            for _ in 0..iterations {
                let query = tantivy::query::QueryParser::for_index(&self.index, vec![tags_field])
                    .parse_query(tag)
                    .ok();

                if let Some(query) = query {
                    if let Ok(top_docs) = searcher.search(&query, &tantivy::collector::Count) {
                        total_results += top_docs as u32;
                    }
                }
            }

            let elapsed = start.elapsed().as_secs_f64();
            let qps = iterations as f64 / elapsed;
            (total_results, qps)
        }

        // 查询 3: 按 score 范围查询
        pub fn query_by_score_range(&self, min: i64, max: i64, iterations: usize) -> (u32, f64) {
            let start = Instant::now();
            let mut total_results = 0u32;

            let searcher = self.index.reader().unwrap().searcher();
            let schema = self.index.schema();
            let score_field = schema.get_field("score").unwrap();

            for _ in 0..iterations {
                let query = tantivy::query::RangeQuery::new_i64(score_field, min..=max);
                if let Ok(count) = searcher.search(&query, &tantivy::collector::Count) {
                    total_results += count as u32;
                }
            }

            let elapsed = start.elapsed().as_secs_f64();
            let qps = iterations as f64 / elapsed;
            (total_results, qps)
        }

        // 查询 4: 多条件 AND 查询
        pub fn query_multi_condition(
            &self,
            category: &str,
            tag: &str,
            iterations: usize,
        ) -> (u32, f64) {
            let start = Instant::now();
            let mut total_results = 0u32;

            let searcher = self.index.reader().unwrap().searcher();
            let schema = self.index.schema();
            let category_field = schema.get_field("category").unwrap();
            let tags_field = schema.get_field("tags").unwrap();

            for _ in 0..iterations {
                let cat_query =
                    tantivy::query::QueryParser::for_index(&self.index, vec![category_field])
                        .parse_query(category)
                        .ok();
                let tag_query =
                    tantivy::query::QueryParser::for_index(&self.index, vec![tags_field])
                        .parse_query(tag)
                        .ok();

                if let (Some(cat_q), Some(tag_q)) = (cat_query, tag_query) {
                    let combined = tantivy::query::BooleanQuery::new(vec![
                        (tantivy::query::Occur::Must, Box::new(cat_q)),
                        (tantivy::query::Occur::Must, Box::new(tag_q)),
                    ]);
                    if let Ok(count) = searcher.search(&combined, &tantivy::collector::Count) {
                        total_results += count as u32;
                    }
                }
            }

            let elapsed = start.elapsed().as_secs_f64();
            let qps = iterations as f64 / elapsed;
            (total_results, qps)
        }
    }
}

#[tokio::main]
async fn main() {
    println!("════════════════════════════════════════════════════════════════");
    println!("   查询性能对标：Calm vs Tantivy");
    println!("   数据规模：1,000,000 条记录（快速测试）");
    println!("════════════════════════════════════════════════════════════════\n");

    let test_data_dir = PathBuf::from("/tmp/calm_vs_tantivy_query_benchmark");
    fs::remove_dir_all(&test_data_dir).ok();
    fs::create_dir_all(&test_data_dir).ok();

    println!("📊 生成测试数据...");
    let test_data = generate_test_data(1_000_000);
    println!("✓ 生成 {} 条测试记录\n", test_data.len());

    println!("📝 Calm 初始化...");
    let calm_bench = calm_test::CalmBenchmark::new(&test_data_dir, &test_data);
    println!("✓ Calm 已加载\n");

    println!("📝 Tantivy 初始化...");
    let tantivy_bench = tantivy_test::TantivyBenchmark::new(&test_data_dir, &test_data);
    println!("✓ Tantivy 已加载\n");

    println!("════════════════════════════════════════════════════════════════");
    println!("🔍 查询性能测试");
    println!("════════════════════════════════════════════════════════════════\n");

    let iterations = 1000;

    // 查询 1: 按 category 精确匹配
    println!("📌 查询 1: 按 category 精确匹配 ({} 次迭代)", iterations);
    let (calm_results, calm_qps) = calm_bench.query_by_category("Technology", iterations);
    let (tantivy_results, tantivy_qps) = tantivy_bench.query_by_category("Technology", iterations);
    println!(
        "  Calm:    {:>6} results, {:.0} QPS",
        calm_results, calm_qps
    );
    println!(
        "  Tantivy: {:>6} results, {:.0} QPS",
        tantivy_results, tantivy_qps
    );
    let speedup = tantivy_qps / calm_qps;
    println!(
        "  ✅ {} 快 {:.1}x\n",
        if speedup > 1.0 { "Tantivy" } else { "Calm" },
        speedup.max(1.0 / speedup)
    );

    // 查询 2: 按 tag 精确匹配
    println!("📌 查询 2: 按 tag 精确匹配 ({} 次迭代)", iterations);
    let (calm_results, calm_qps) = calm_bench.query_by_tag("news", iterations);
    let (tantivy_results, tantivy_qps) = tantivy_bench.query_by_tag("news", iterations);
    println!(
        "  Calm:    {:>6} results, {:.0} QPS",
        calm_results, calm_qps
    );
    println!(
        "  Tantivy: {:>6} results, {:.0} QPS",
        tantivy_results, tantivy_qps
    );
    let speedup = tantivy_qps / calm_qps;
    println!(
        "  ✅ {} 快 {:.1}x\n",
        if speedup > 1.0 { "Tantivy" } else { "Calm" },
        speedup.max(1.0 / speedup)
    );

    // 查询 3: 按 score 范围查询
    println!("📌 查询 3: 按 score 范围查询 ({} 次迭代)", iterations);
    let (calm_results, calm_qps) = calm_bench.query_by_score_range(100, 900, iterations);
    let (tantivy_results, tantivy_qps) = tantivy_bench.query_by_score_range(100, 900, iterations);
    println!(
        "  Calm:    {:>6} results, {:.0} QPS",
        calm_results, calm_qps
    );
    println!(
        "  Tantivy: {:>6} results, {:.0} QPS",
        tantivy_results, tantivy_qps
    );
    let speedup = tantivy_qps / calm_qps;
    println!(
        "  ✅ {} 快 {:.1}x\n",
        if speedup > 1.0 { "Tantivy" } else { "Calm" },
        speedup.max(1.0 / speedup)
    );

    // 查询 4: 多条件 AND 查询
    println!("📌 查询 4: 多条件 AND 查询 ({} 次迭代)", iterations);
    let (calm_results, calm_qps) =
        calm_bench.query_multi_condition("Technology", "news", iterations);
    let (tantivy_results, tantivy_qps) =
        tantivy_bench.query_multi_condition("Technology", "news", iterations);
    println!(
        "  Calm:    {:>6} results, {:.0} QPS",
        calm_results, calm_qps
    );
    println!(
        "  Tantivy: {:>6} results, {:.0} QPS",
        tantivy_results, tantivy_qps
    );
    let speedup = tantivy_qps / calm_qps;
    println!(
        "  ✅ {} 快 {:.1}x\n",
        if speedup > 1.0 { "Tantivy" } else { "Calm" },
        speedup.max(1.0 / speedup)
    );

    println!("════════════════════════════════════════════════════════════════");
    println!("✅ 查询性能测试完成!\n");
}
