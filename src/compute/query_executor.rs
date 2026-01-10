// use std::sync::Arc;

// use crate::catalog::Catalog;
// use crate::cluster::ClusterManager;
// use crate::compute::NormalizedSql;
// use crate::engine::Engine;
// use crate::utils::error::CoreResult;

// pub struct CalmQueryExecutor {
//     catalog: Arc<Catalog>,
//     engine: Arc<Engine>,
//     cluster_manager: Arc<ClusterManager>,
// }

// impl CalmQueryExecutor {
//     pub fn new(
//         catalog: Arc<Catalog>,
//         engine: Arc<Engine>,
//         cluster_manager: Arc<ClusterManager>,
//     ) -> Self {
//         Self {
//             catalog,
//             engine,
//             cluster_manager,
//         }
//     }

//     pub async fn execute_with_partitions(
//         &self,
//         normalized: &NormalizedSql,
//         partition_hint: Option<&[String]>,
//         local_only: bool,
//     ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
//         self.execute_internal(normalized, partition_hint, local_only)
//             .await
//     }

//     async fn execute_internal(
//         &self,
//         normalized: &NormalizedSql,
//         partition_hint: Option<&[String]>,
//         local_only: bool,
//     ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
//         let sql = &normalized.rewritten_sql;
//         log::debug!(
//             "[FederatedQueryExecutor] Executing SQL (local_only={}, partitions_hint={:?}): {}",
//             local_only,
//             partition_hint,
//             sql
//         );

//         let table_names = self.parse_table_names(sql)?;
//         if table_names.is_empty() {
//             return Err(CoreError::Internal("No tables found in SQL".to_string()));
//         }

//         let my_node_id = self.cluster_manager.node_id();
//         let partition_hint_set: Option<HashSet<String>> =
//             partition_hint.map(|names| names.iter().cloned().collect::<HashSet<String>>());

//         // 预先计算每个表需要访问的分区分布
//         let mut table_partition_nodes: HashMap<String, HashMap<String, Vec<String>>> =
//             HashMap::new();
//         let mut table_schemas: HashMap<String, datafusion::arrow::datatypes::SchemaRef> =
//             HashMap::new();

//         for table_name in &table_names {
//             let table_info = self.catalog.get_or_load_table(table_name).await?;
//             let partitions = table_info.partitions.read().await;
//             let all_partition_names: Vec<String> = partitions.keys().cloned().collect();

//             let partition_filter_set = if normalized.partition_filters.scan_all() {
//                 None
//             } else {
//                 Some(
//                     normalized
//                         .partition_filters
//                         .resolve_partitions(&all_partition_names)
//                         .into_iter()
//                         .collect::<HashSet<_>>(),
//                 )
//             };

//             let mut nodes: HashMap<String, Vec<String>> = HashMap::new();

//             for (partition_name, partition_meta) in partitions.iter() {
//                 if !Self::partition_selected(
//                     partition_name,
//                     partition_filter_set.as_ref(),
//                     partition_hint_set.as_ref(),
//                 ) {
//                     continue;
//                 }

//                 nodes
//                     .entry(partition_meta.owner.clone())
//                     .or_default()
//                     .push(partition_name.clone());
//             }

//             table_schemas.insert(
//                 table_name.clone(),
//                 table_info.table.schema.to_arrow_schema(),
//             );
//             log::debug!(
//                 "📊 [FederatedQueryExecutor] Table '{}' partition distribution: {:?}",
//                 table_name,
//                 nodes
//                     .iter()
//                     .map(|(node, parts)| (node.clone(), parts.len()))
//                     .collect::<Vec<_>>()
//             );
//             table_partition_nodes.insert(table_name.clone(), nodes);
//         }

//         // 🚀 使用 DataFusion 默认的 SessionState，而不是 datafusion-federation 的
//         // 这样避免使用 FederatedQueryPlanner，直接使用我们的 RemoteTableProvider
//         let ctx = SessionContext::new();
//         let fulltext_context = register_fulltext_udfs(&ctx);
//         if normalized.needs_score_column {
//             fulltext_context.clear_scores();
//         }

//         for table_name in &table_names {
//             let schema = table_schemas
//                 .get(table_name)
//                 .cloned()
//                 .ok_or_else(|| CoreError::Internal("Missing table schema".to_string()))?;
//             let nodes = table_partition_nodes.get(table_name).unwrap();

//             if local_only {
//                 let local_partitions = nodes.get(my_node_id).cloned().unwrap_or_default();
//                 if local_partitions.is_empty() {
//                     self.register_empty_table(&ctx, table_name, schema.clone())?;
//                 } else {
//                     // 统一使用 MixedTableProvider（remote_nodes 为空）
//                     self.register_unified_table(
//                         &ctx,
//                         table_name,
//                         nodes,
//                         normalized.needs_score_column,
//                         normalized.is_count_only,
//                         normalized.limit,
//                     )
//                     .await?;
//                 }
//                 continue;
//             }

//             if nodes.is_empty() {
//                 log::info!("⚠️  [FederatedQueryExecutor] Table '{}' has no partitions, registering empty table", table_name);
//                 self.register_empty_table(&ctx, table_name, schema.clone())?;
//                 continue;
//             }

//             log::debug!(
//                 "🔍 [FederatedQueryExecutor] Table '{}' decision: nodes.len()={}, contains_my_node={}, my_node_id={}",
//                 table_name,
//                 nodes.len(),
//                 nodes.contains_key(my_node_id),
//                 my_node_id
//             );

//             if nodes.len() == 1 && nodes.contains_key(my_node_id) {
//                 let local_partitions = nodes.get(my_node_id).cloned().unwrap_or_default();
//                 if local_partitions.is_empty() {
//                     log::debug!("🏠 [FederatedQueryExecutor] Table '{}': All local but no partitions, using empty table", table_name);
//                     self.register_empty_table(&ctx, table_name, schema.clone())?;
//                 } else {
//                     log::debug!("🏠 [FederatedQueryExecutor] Table '{}': All {} partition(s) on local node, using unified table", table_name, local_partitions.len());
//                     // 统一使用 MixedTableProvider（remote_nodes 为空）
//                     self.register_unified_table(
//                         &ctx,
//                         table_name,
//                         nodes,
//                         normalized.needs_score_column,
//                         normalized.is_count_only,
//                         normalized.limit,
//                     )
//                     .await?;
//                 }
//             } else if !nodes.contains_key(my_node_id) {
//                 log::debug!(
//                     "🌐 [FederatedQueryExecutor] Table '{}': All partitions on remote nodes, using unified table",
//                     table_name
//                 );
//                 // 统一使用 MixedTableProvider（local_partitions 为空）
//                 self.register_unified_table(
//                     &ctx,
//                     table_name,
//                     nodes,
//                     normalized.needs_score_column,
//                     normalized.is_count_only,
//                     normalized.limit,
//                 )
//                 .await?;
//             } else {
//                 log::debug!("🔀 [FederatedQueryExecutor] Table '{}': Partitions distributed across {} nodes, using unified table", table_name, nodes.len());
//                 // 统一使用 MixedTableProvider
//                 self.register_unified_table(
//                     &ctx,
//                     table_name,
//                     nodes,
//                     normalized.needs_score_column,
//                     normalized.is_count_only,
//                     normalized.limit,
//                 )
//                 .await?;
//             }
//         }

//         log::debug!("[FederatedQueryExecutor] Parsing SQL: {}", sql);
//         let df = ctx
//             .sql(sql)
//             .await
//             .map_err(|e| CoreError::Internal(format!("Failed to parse SQL: {}", e)))?;

//         let stream = df
//             .execute_stream()
//             .await
//             .map_err(|e| CoreError::Internal(format!("Failed to execute query: {}", e)))?;

//         let stream = if normalized.needs_score_column {
//             build_score_stream(stream, fulltext_context.clone(), &normalized.score)
//         } else {
//             stream
//         };

//         Ok(stream)
//     }
// }
