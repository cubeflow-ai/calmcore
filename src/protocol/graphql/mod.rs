use std::sync::Arc;

use async_graphql::{Context, EmptySubscription, Object, Result, Schema, SimpleObject};
use poem::{
    get, handler, listener::TcpListener, middleware::Cors, post, EndpointExt, Route, Server,
};
use serde_json::Value as JsonValue;

use crate::{
    catalog::{PartitionStrategy, PartitionValue, RangePartition},
    engine::Engine,
    schema::{field::FieldOption, PersistPolicy, Schema as CalmSchema},
    utils::arrow_utils,
};

/// GraphQL Schema for database operations
pub type CalmGraphQLSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

/// 创建 GraphQL Schema
pub fn create_schema(engine: Arc<Engine>) -> CalmGraphQLSchema {
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(engine)
        .finish()
}

// ===== 类型定义 =====

/// 字段类型枚举
#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub enum FieldTypeEnum {
    /// 关键字/文本类型
    Keyword,
    /// 文本类型 (keyword 的别名)
    Text,
    /// 8位有符号整数
    I8,
    /// 16位有符号整数
    I16,
    /// 32位有符号整数
    I32,
    /// 整数 (i32 的别名)
    Integer,
    /// 64位有符号整数
    I64,
    /// 长整数 (i64 的别名)
    Long,
    /// 8位无符号整数
    U8,
    /// 16位无符号整数
    U16,
    /// 32位无符号整数
    U32,
    /// 64位无符号整数
    U64,
    /// 32位浮点数
    F32,
    /// 浮点数 (f32 的别名)
    Float,
    /// 64位浮点数
    F64,
    /// 双精度浮点数 (f64 的别名)
    Double,
    /// 布尔类型
    Boolean,
    /// 布尔类型别名
    Bool,
    /// 时间戳类型 (毫秒级)
    Timestamp,
    /// 时间类型 (timestamp 的别名)
    Datetime,
}

#[derive(SimpleObject)]
pub struct Table {
    pub name: String,
    pub partition_count: u64,
    pub fields: Vec<Field>,
    pub primary_key: Option<String>,
}

#[derive(SimpleObject)]
pub struct Field {
    pub name: String,
    pub field_type: String,
    pub indexed: bool,
}

/// 分区策略类型枚举
#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub enum PartitionStrategyType {
    /// 哈希分区
    Hash,
    /// 范围分区
    Range,
    /// 自定义分区
    Custom,
    /// 无分区
    None,
}

/// 分区值枚举（用于 Range 分区）
#[derive(async_graphql::InputObject)]
pub struct PartitionValueInput {
    /// 整数值（可选）
    pub int_value: Option<i64>,
    /// 无符号整数值（可选）
    pub uint_value: Option<u64>,
    /// 字符串值（可选）
    pub string_value: Option<String>,
}

impl From<PartitionValueInput> for PartitionValue {
    fn from(val: PartitionValueInput) -> Self {
        if let Some(i) = val.int_value {
            PartitionValue::Int64(i)
        } else if let Some(u) = val.uint_value {
            PartitionValue::UInt64(u)
        } else if let Some(s) = val.string_value {
            PartitionValue::String(s)
        } else {
            PartitionValue::Int64(0) // 默认值
        }
    }
}

/// 范围分区定义
#[derive(async_graphql::InputObject)]
pub struct RangePartitionInput {
    /// 起始值
    pub start: PartitionValueInput,
    /// 结束值
    pub end: PartitionValueInput,
    /// 分区 ID
    pub partition_id: u64,
}

impl From<RangePartitionInput> for RangePartition {
    fn from(val: RangePartitionInput) -> Self {
        RangePartition {
            start: val.start.into(),
            end: val.end.into(),
        }
    }
}

/// 分区策略配置
#[derive(async_graphql::InputObject)]
pub struct PartitionStrategyInput {
    /// 分区策略类型
    pub strategy_type: PartitionStrategyType,
    /// 分区字段名（Hash、Range、List 策略需要）
    pub field: Option<String>,
    /// 分区数量（Hash 策略需要）
    pub num_partitions: Option<u64>,
    /// 范围定义（Range 策略需要）
    pub ranges: Option<Vec<RangePartitionInput>>,
    /// 值映射（List 策略需要）格式："value1:0,value2:1,value3:2"
    pub value_mapping: Option<String>,
}

/// 持久化策略配置
#[derive(async_graphql::InputObject)]
pub struct PersistPolicyInput {
    /// 文档数阈值（达到此数量触发持久化，默认 100000）
    pub max_docs_per_segment: Option<u32>,
    /// 时间阈值（segment 存活超过此秒数触发持久化，默认 300 秒）
    pub max_segment_age_secs: Option<u64>,
}

#[derive(async_graphql::InputObject)]
pub struct CreateTableInput {
    pub name: String,
    pub primary_key: Option<String>,
    /// 分区策略（可选，默认使用 Hash 策略）
    pub partition_strategy: Option<PartitionStrategyInput>,
    /// 分区数量（仅在未指定 partition_strategy 时使用，默认为 1）
    pub partition_count: Option<u64>,
    pub fields: Vec<FieldInput>,
    /// 是否存储原始 JSON 数据（默认 true）
    pub store_source: Option<bool>,
    /// 持久化策略配置（可选）
    pub persist_policy: Option<PersistPolicyInput>,
}

#[derive(async_graphql::InputObject)]
pub struct FieldInput {
    pub name: String,
    pub field_type: FieldTypeEnum,
    pub indexed: Option<bool>,
    // Keyword 特定配置
    pub case_sensitive: Option<bool>,
    // Timestamp 特定配置
    pub format: Option<String>,
}

#[derive(SimpleObject)]
pub struct InsertResult {
    pub success: bool,
    pub rows_inserted: usize,
    pub message: String,
}

#[derive(SimpleObject)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<JsonValue>,
    pub total_rows: usize,
}

/// Partition 信息
#[derive(SimpleObject)]
pub struct PartitionInfo {
    pub partition_id: String,
    pub segment_count: usize,
    pub segments: Vec<SegmentInfo>,
}

/// Segment 信息
#[derive(SimpleObject)]
pub struct SegmentInfo {
    pub segment_id: u64,
    pub doc_count: u32,
    pub deleted_count: u64,
    pub is_persisted: bool,
    pub base_path: Option<String>,
    pub is_external_reference: bool,
    pub external_data_path: Option<String>,
}

/// Table 详细信息（包含分区和段）
#[derive(SimpleObject)]
pub struct TableDetail {
    pub name: String,
    pub partition_count: usize,
    pub total_segments: usize,
    pub total_documents: u64,
    pub fields: Vec<Field>,
    pub primary_key: Option<String>,
    pub partitions: Vec<PartitionInfo>,
}

#[derive(async_graphql::InputObject)]
pub struct InsertDataInput {
    pub table: String,
    pub data: Vec<JsonValue>,
}

/// 文件处理类型枚举
#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub enum FileHandlerTypeEnum {
    /// 引用文件路径（不移动原文件）
    Reference,
    /// 移动文件
    Move,
    /// 拷贝文件
    Copy,
}

impl From<FileHandlerTypeEnum> for crate::segment_loader::FileHandlerType {
    fn from(val: FileHandlerTypeEnum) -> Self {
        match val {
            FileHandlerTypeEnum::Reference => crate::segment_loader::FileHandlerType::Reference,
            FileHandlerTypeEnum::Move => crate::segment_loader::FileHandlerType::Move,
            FileHandlerTypeEnum::Copy => crate::segment_loader::FileHandlerType::Copy,
        }
    }
}

#[derive(async_graphql::InputObject)]
pub struct LoadSegmentInput {
    /// 表名
    pub table: String,
    /// 分区名称（必须提供，且符合目录名称规范）
    pub partition_name: String,
    /// 文件路径（支持 .parquet 和 .jsonl 格式）
    pub file_path: String,
    /// 文件处理类型（Parquet 必需：REFERENCE/MOVE/COPY，JSONL 不需要）
    pub handler_type: Option<FileHandlerTypeEnum>,
}

#[derive(SimpleObject)]
pub struct LoadSegmentResult {
    pub success: bool,
    pub documents_loaded: usize,
    pub partition_name: String,
    pub message: String,
}

// ===== Query Root =====

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// 列出所有表
    async fn tables(&self, ctx: &Context<'_>) -> Result<Vec<String>> {
        let engine = ctx.data::<Arc<Engine>>()?;
        Ok(engine.list_tables())
    }

    /// 获取表信息
    async fn table(&self, ctx: &Context<'_>, name: String) -> Result<Option<Table>> {
        let engine = ctx.data::<Arc<Engine>>()?;

        let meta = match engine.get_table_meta(&name) {
            Ok(meta) => meta,
            Err(_) => return Ok(None),
        };

        let fields = meta
            .schema
            .fields
            .iter()
            .map(|f| {
                let field_type = match f {
                    FieldOption::Keyword { .. } => "keyword",
                    FieldOption::I64 { .. } => "i64",
                    FieldOption::F64 { .. } => "f64",
                    FieldOption::Boolean { .. } => "boolean",
                    FieldOption::I32 { .. } => "i32",
                    FieldOption::F32 { .. } => "f32",
                    FieldOption::Timestamp { .. } => "timestamp",
                    _ => "unknown",
                };

                Field {
                    name: f.name().to_string(),
                    field_type: field_type.to_string(),
                    indexed: f.is_index(),
                }
            })
            .collect();

        Ok(Some(Table {
            name: meta.schema.name.clone(),
            partition_count: meta.parallel_workers as u64,
            fields,
            primary_key: meta.schema.primary_key.clone(),
        }))
    }

    /// 获取表的所有分区信息
    async fn partitions(&self, ctx: &Context<'_>, table: String) -> Result<Vec<PartitionInfo>> {
        let engine = ctx.data::<Arc<Engine>>()?;

        // 获取表的所有分区 ID
        let partition_ids = engine.list_partitions(&table).await;

        let mut partition_infos = Vec::new();

        for partition_id in partition_ids {
            if let Some(partition) = engine.get_partition(&table, &partition_id).await {
                // 获取 frozen segments
                let frozen_segments = partition.get_frozen_segments();
                let mut segments = Vec::new();

                // 收集 frozen segments 信息
                for (seg_id, segment) in frozen_segments.iter() {
                    segments.push(SegmentInfo {
                        segment_id: *seg_id,
                        doc_count: segment.doc_count(),
                        deleted_count: segment.deleted_count(),
                        is_persisted: segment.is_persisted(),
                        base_path: segment.base_path(),
                        is_external_reference: segment.is_external_reference(),
                        external_data_path: segment.get_external_data_path(),
                    });
                }

                // 释放读锁
                drop(frozen_segments);

                // 获取当前 segment（ID = 0 表示当前活跃 segment）
                let current_segment = partition.get_current_segment();
                segments.push(SegmentInfo {
                    segment_id: 0,
                    doc_count: current_segment.doc_count(),
                    deleted_count: current_segment.deleted_count(),
                    is_persisted: current_segment.is_persisted(),
                    base_path: current_segment.base_path(),
                    is_external_reference: current_segment.is_external_reference(),
                    external_data_path: current_segment.get_external_data_path(),
                });

                partition_infos.push(PartitionInfo {
                    partition_id,
                    segment_count: segments.len(),
                    segments,
                });
            }
        }

        Ok(partition_infos)
    }

    /// 获取表的完整详情（包括分区和段）
    async fn table_detail(&self, ctx: &Context<'_>, name: String) -> Result<Option<TableDetail>> {
        let engine = ctx.data::<Arc<Engine>>()?;

        let meta = match engine.get_table_meta(&name) {
            Ok(meta) => meta,
            Err(_) => return Ok(None),
        };

        let fields = meta
            .schema
            .fields
            .iter()
            .map(|f| {
                let field_type = match f {
                    FieldOption::Keyword { .. } => "keyword",
                    FieldOption::I64 { .. } => "i64",
                    FieldOption::F64 { .. } => "f64",
                    FieldOption::Boolean { .. } => "boolean",
                    FieldOption::I32 { .. } => "i32",
                    FieldOption::F32 { .. } => "f32",
                    FieldOption::Timestamp { .. } => "timestamp",
                    _ => "unknown",
                };

                Field {
                    name: f.name().to_string(),
                    field_type: field_type.to_string(),
                    indexed: f.is_index(),
                }
            })
            .collect();

        // 获取所有分区信息
        let partition_ids = engine.list_partitions(&name).await;
        let mut partition_infos = Vec::new();
        let mut total_segments = 0;
        let mut total_documents = 0u64;

        for partition_id in partition_ids {
            if let Some(partition) = engine.get_partition(&name, &partition_id).await {
                eprintln!(
                    "🔍 [table_detail] Partition name: {}, address: {:p}",
                    partition.name(),
                    &*partition
                );

                // 获取 frozen segments
                let frozen_segments = partition.get_frozen_segments();
                eprintln!(
                    "🔍 [table_detail] frozen_segments count: {}",
                    frozen_segments.len()
                );
                let mut segments = Vec::new();

                // 收集 frozen segments 信息
                for (seg_id, segment) in frozen_segments.iter() {
                    let doc_count = segment.doc_count();
                    let deleted_count = segment.deleted_count();
                    total_documents += doc_count as u64 - deleted_count;

                    segments.push(SegmentInfo {
                        segment_id: *seg_id,
                        doc_count,
                        deleted_count,
                        is_persisted: segment.is_persisted(),
                        base_path: segment.base_path(),
                        is_external_reference: segment.is_external_reference(),
                        external_data_path: segment.get_external_data_path(),
                    });
                }

                // 释放读锁
                drop(frozen_segments);

                // 获取当前 segment
                let current_segment = partition.get_current_segment();
                let doc_count = current_segment.doc_count();
                let deleted_count = current_segment.deleted_count();
                total_documents += doc_count as u64 - deleted_count;

                segments.push(SegmentInfo {
                    segment_id: 0,
                    doc_count,
                    deleted_count,
                    is_persisted: current_segment.is_persisted(),
                    base_path: current_segment.base_path(),
                    is_external_reference: current_segment.is_external_reference(),
                    external_data_path: current_segment.get_external_data_path(),
                });

                total_segments += segments.len();

                partition_infos.push(PartitionInfo {
                    partition_id,
                    segment_count: segments.len(),
                    segments,
                });
            }
        }

        Ok(Some(TableDetail {
            name: meta.schema.name.clone(),
            partition_count: partition_infos.len(),
            total_segments,
            total_documents,
            fields,
            primary_key: meta.schema.primary_key.clone(),
            partitions: partition_infos,
        }))
    }

    /// 执行 SQL 查询
    async fn query(&self, ctx: &Context<'_>, sql: String) -> Result<QueryResult> {
        let engine = ctx.data::<Arc<Engine>>()?;

        // 使用 Engine 的 execute_sql 方法
        let result = engine
            .execute_sql(&sql)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Query failed: {}", e)))?;

        if result.batch.num_rows() == 0 {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                total_rows: 0,
            });
        }

        // 获取列名
        let columns: Vec<String> = result
            .batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        // 转换为 JSON
        let rows = arrow_utils::record_batch_to_json(&result.batch)
            .map_err(|e| async_graphql::Error::new(format!("Failed to convert to JSON: {}", e)))?;

        let total_rows = rows.len();

        Ok(QueryResult {
            columns,
            rows,
            total_rows,
        })
    }
}

// ===== Mutation Root =====

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// 创建表
    async fn create_table(&self, ctx: &Context<'_>, input: CreateTableInput) -> Result<Table> {
        let engine = ctx.data::<Arc<Engine>>()?;

        // 构建字段
        let mut fields = Vec::new();
        for field_input in input.fields {
            let indexed = field_input.indexed.unwrap_or(true);

            let field = match field_input.field_type {
                FieldTypeEnum::Keyword | FieldTypeEnum::Text => {
                    let case_sensitive = field_input.case_sensitive.unwrap_or(true);
                    FieldOption::Keyword {
                        name: field_input.name.clone(),
                        index: indexed,
                        is_array: false,
                        persist_option: None,
                        case_sensitive,
                    }
                }
                FieldTypeEnum::I64 | FieldTypeEnum::Long => FieldOption::I64 {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                FieldTypeEnum::F64 | FieldTypeEnum::Double => FieldOption::F64 {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                FieldTypeEnum::Boolean | FieldTypeEnum::Bool => FieldOption::Boolean {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                FieldTypeEnum::I32 | FieldTypeEnum::Integer => FieldOption::I32 {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                FieldTypeEnum::F32 | FieldTypeEnum::Float => FieldOption::F32 {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                FieldTypeEnum::I8 => FieldOption::I8 {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                FieldTypeEnum::I16 => FieldOption::I16 {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                FieldTypeEnum::U8 => FieldOption::U8 {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                FieldTypeEnum::U16 => FieldOption::U16 {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                FieldTypeEnum::U32 => FieldOption::U32 {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                FieldTypeEnum::U64 => FieldOption::U64 {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                FieldTypeEnum::Timestamp | FieldTypeEnum::Datetime => FieldOption::Timestamp {
                    name: field_input.name.clone(),
                    index: indexed,
                    format: field_input.format,
                },
            };

            fields.push(field);
        }

        // 构建持久化策略
        let persist_policy = if let Some(policy_input) = input.persist_policy {
            PersistPolicy {
                max_docs_per_segment: policy_input.max_docs_per_segment.unwrap_or(100_000),
                max_segment_age: std::time::Duration::from_secs(
                    policy_input.max_segment_age_secs.unwrap_or(300),
                ),
            }
        } else {
            PersistPolicy::default()
        };

        // 创建 Schema
        let schema = CalmSchema {
            name: input.name.clone(),
            primary_key: input.primary_key.clone(),
            store_source: input.store_source.unwrap_or(true),
            fields: fields.clone(),
            persist_policy,
        };

        // 构建分区策略
        let (partition_strategy, num_partitions) =
            if let Some(strategy_input) = input.partition_strategy {
                match strategy_input.strategy_type {
                    PartitionStrategyType::Hash => {
                        let field = strategy_input.field.ok_or_else(|| {
                            async_graphql::Error::new("Hash strategy requires 'field' parameter")
                        })?;
                        let num_partitions = strategy_input.num_partitions.ok_or_else(|| {
                            async_graphql::Error::new(
                                "Hash strategy requires 'num_partitions' parameter",
                            )
                        })? as usize;

                        (
                            PartitionStrategy::Hash {
                                field,
                                num_partitions,
                            },
                            num_partitions,
                        )
                    }
                    PartitionStrategyType::Range => {
                        let field = strategy_input.field.ok_or_else(|| {
                            async_graphql::Error::new("Range strategy requires 'field' parameter")
                        })?;
                        let ranges_input = strategy_input.ranges.ok_or_else(|| {
                            async_graphql::Error::new("Range strategy requires 'ranges' parameter")
                        })?;

                        let ranges: Vec<RangePartition> =
                            ranges_input.into_iter().map(|r| r.into()).collect();

                        let num_partitions = ranges.len();

                        (PartitionStrategy::Range { field, ranges }, num_partitions)
                    }
                    PartitionStrategyType::Custom => {
                        // Custom 分区不需要其他参数
                        (PartitionStrategy::Custom, 0)
                    }
                    PartitionStrategyType::None => {
                        // None 分区策略，所有数据在一个 partition
                        (PartitionStrategy::None, 1)
                    }
                }
            } else {
                // 如果未指定分区策略，使用默认的 Hash 策略
                let partition_field = input.primary_key.clone().unwrap_or_else(|| {
                    fields
                        .first()
                        .map(|f| f.name().to_string())
                        .unwrap_or_default()
                });

                let partition_count = input.partition_count.unwrap_or(1) as usize;

                (
                    PartitionStrategy::Hash {
                        field: partition_field,
                        num_partitions: partition_count,
                    },
                    partition_count,
                )
            };

        // 创建表
        engine
            .create_table(&input.name, schema, partition_strategy, num_partitions)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        // 返回创建的表信息
        let field_info: Vec<Field> = fields
            .iter()
            .map(|f| {
                let field_type = match f {
                    FieldOption::Keyword { .. } => "keyword",
                    FieldOption::I64 { .. } => "i64",
                    FieldOption::F64 { .. } => "f64",
                    FieldOption::Boolean { .. } => "boolean",
                    FieldOption::I32 { .. } => "i32",
                    FieldOption::F32 { .. } => "f32",
                    FieldOption::Timestamp { .. } => "timestamp",
                    _ => "unknown",
                };

                Field {
                    name: f.name().to_string(),
                    field_type: field_type.to_string(),
                    indexed: f.is_index(),
                }
            })
            .collect();

        Ok(Table {
            name: input.name,
            partition_count: num_partitions as u64,
            fields: field_info,
            primary_key: input.primary_key,
        })
    }

    /// 删除表
    async fn drop_table(&self, ctx: &Context<'_>, name: String) -> Result<bool> {
        let engine = ctx.data::<Arc<Engine>>()?;

        engine
            .drop_table(&name)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        Ok(true)
    }

    /// 持久化表（强制将表的所有数据写入磁盘）
    async fn flush_table(&self, ctx: &Context<'_>, name: String) -> Result<bool> {
        let engine = ctx.data::<Arc<Engine>>()?;

        engine
            .flush_table(&name)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Failed to flush table: {}", e)))?;

        Ok(true)
    }

    /// 持久化表（别名，与 flush_table 功能相同）
    async fn table_persist(&self, ctx: &Context<'_>, name: String) -> Result<bool> {
        self.flush_table(ctx, name).await
    }

    /// 插入数据
    async fn insert_data(&self, ctx: &Context<'_>, input: InsertDataInput) -> Result<InsertResult> {
        let engine = ctx.data::<Arc<Engine>>()?;

        // 获取表的元数据
        let meta = engine
            .get_table_meta(&input.table)
            .map_err(|e| async_graphql::Error::new(format!("Table not found: {}", e)))?;

        // 获取主键字段
        let pk_field = meta
            .schema
            .primary_key
            .as_ref()
            .ok_or_else(|| async_graphql::Error::new("Table has no primary key"))?;

        let mut total_inserted = 0;

        // 对每条数据进行路由和插入
        for doc in &input.data {
            // 1. 提取主键值
            let pk_value = doc.get(pk_field).and_then(|v| v.as_str()).ok_or_else(|| {
                async_graphql::Error::new(format!("Missing or invalid primary key: {}", pk_field))
            })?;

            // 2. 根据路由策略计算 partition_id
            let partition_id = engine
                .route_partition(&input.table, pk_value)
                .map_err(|e| async_graphql::Error::new(format!("Route failed: {}", e)))?;

            // 3. 获取 partition
            let partition = engine
                .get_partition(&input.table, &partition_id)
                .await
                .ok_or_else(|| {
                    async_graphql::Error::new(format!(
                        "Partition {} not found for table {}",
                        partition_id, input.table
                    ))
                })?;

            // 4. 插入单条数据
            partition
                .upsert_json(&[doc.clone()])
                .map_err(|e| async_graphql::Error::new(format!("Insert failed: {}", e)))?;

            total_inserted += 1;
        }

        Ok(InsertResult {
            success: true,
            rows_inserted: total_inserted,
            message: format!("Successfully inserted {} rows", total_inserted),
        })
    }

    /// 加载外部文件到 segment（用于 Custom 分区）
    async fn load_segment(
        &self,
        ctx: &Context<'_>,
        input: LoadSegmentInput,
    ) -> Result<LoadSegmentResult> {
        let engine = ctx.data::<Arc<Engine>>()?;

        // 验证文件路径
        let file_path = std::path::PathBuf::from(&input.file_path);
        if !file_path.exists() {
            return Err(async_graphql::Error::new(format!(
                "File does not exist: {}",
                input.file_path
            )));
        }

        // 转换 handler_type（可选）
        let handler_type = input.handler_type.map(|ht| ht.into());

        // 调用 engine 的 load_segment 方法
        let doc_count = engine
            .load_segment(
                &input.table,
                input.partition_name.clone(),
                file_path,
                handler_type,
            )
            .await
            .map_err(|e| async_graphql::Error::new(format!("Load segment failed: {}", e)))?;

        Ok(LoadSegmentResult {
            success: true,
            documents_loaded: doc_count,
            partition_name: input.partition_name,
            message: format!(
                "Successfully loaded {} documents from {}",
                doc_count, input.file_path
            ),
        })
    }
}

// ===== GraphQL Server =====

/// GraphQL 服务器
pub struct GraphQLServer {
    engine: Arc<Engine>,
}

impl GraphQLServer {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    /// 启动 GraphQL 服务器
    pub async fn start(self, addr: &str) -> Result<(), std::io::Error> {
        // 创建 GraphQL Schema
        let graphql_schema = create_schema(self.engine.clone());

        // GraphQL endpoint
        let graphql_endpoint = async_graphql_poem::GraphQL::new(graphql_schema);

        // 构建路由
        let app = Route::new()
            .at("/", get(root))
            .at("/health", get(health))
            .at("/graphql", post(graphql_endpoint))
            .at(
                "/playground",
                get(poem::endpoint::make_sync(move |_| {
                    poem::web::Html(
                        r#"
                        <!DOCTYPE html>
                        <html>
                        <head>
                            <title>GraphQL Playground</title>
                            <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/graphql-playground-react/build/static/css/index.css" />
                            <script src="https://cdn.jsdelivr.net/npm/graphql-playground-react/build/static/js/middleware.js"></script>
                        </head>
                        <body>
                            <div id="root"></div>
                            <script>
                                window.addEventListener('load', function() {
                                    GraphQLPlayground.init(document.getElementById('root'), {
                                        endpoint: '/graphql'
                                    })
                                })
                            </script>
                        </body>
                        </html>
                        "#.to_string()
                    )
                })),
            )
            .with(Cors::new());

        Server::new(TcpListener::bind(addr)).run(app).await
    }
}

/// 健康检查
#[handler]
async fn health() -> &'static str {
    "OK"
}

/// 根路径
#[handler]
async fn root() -> poem::web::Json<serde_json::Value> {
    poem::web::Json(serde_json::json!({
        "name": "Calm Database - GraphQL Server",
        "version": "0.1.0",
        "graphql_endpoint": "/graphql",
        "playground": "/playground"
    }))
}
