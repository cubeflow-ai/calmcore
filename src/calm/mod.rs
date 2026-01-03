use std::{net::SocketAddr, sync::Arc, time::Duration};

use arrow_flight::{encode::FlightDataEncoderBuilder, FlightClient, FlightDescriptor, PutResult};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::empty::EmptyTable;
use futures::{stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use tarpc::server::Channel;
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_serde::formats::Bincode;
use tonic::transport::Channel as TonicChannel;

use crate::{
    catalog::{Catalog, TableMeta},
    cluster::{keys, ClusterManager},
    compute::{
        udf::fulltext_udf::{register_fulltext_udfs, wrap_stream_with_scores},
        NormalizedSql, SqlNormalizer, UnionTableProvider,
    },
    engine::Engine,
    utils::error::{CoreError, CoreResult},
};

// pub(crate) mod data;
mod job;
// pub(crate) mod meta;
mod flight_actions;
mod flight_service;
mod service;
mod servie_ext;

// 公开导出 trait 和 client
pub use service::{CalmRpcService, CalmRpcServiceClient};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDetail {
    pub table: TableMeta,
    pub partitions: std::collections::HashMap<String, PartitionDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionDetail {
    pub partition_name: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub owner: String,
    pub segments: Vec<SegmentDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentDetail {
    pub segment_id: u64,
    pub doc_count: u32,
    pub deleted_count: u64,
    pub is_persisted: bool,
    pub base_path: Option<String>,
    pub is_external_reference: bool,
    pub external_data_path: Option<String>,
}

/// 节点状态信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// 节点ID
    pub node_id: String,
    /// Partition 数量
    pub partition_count: usize,
    /// CPU 使用率 (0-100)
    pub cpu_usage: f32,
    /// 内存使用率 (0-100)
    pub memory_usage: f32,
    /// 总内存 (bytes)
    pub total_memory: u64,
    /// 已使用内存 (bytes)
    pub used_memory: u64,
    /// 系统负载 (1分钟平均负载)
    pub load_avg_1min: f32,
    /// 所有分区列表 (table_name, partition_name)
    pub all_partitions: Vec<(String, String)>,
}

pub struct Locker {
    partition_lock: Mutex<()>,
    coord_partition_lock: Mutex<()>,
}

impl Default for Locker {
    fn default() -> Self {
        Self::new()
    }
}

impl Locker {
    pub fn new() -> Self {
        Self {
            partition_lock: Mutex::new(()),
            coord_partition_lock: Mutex::new(()),
        }
    }
}

#[derive(Clone)]
pub struct CalmService {
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) cluster_manager: ClusterManagerRef,
    pub(crate) engine: Arc<Engine>,
    locker: Arc<Locker>,
}

impl CalmService {
    pub async fn new(conf: crate::config::Config) -> CoreResult<Arc<Self>> {
        // 初始化集群 (从配置文件读取 gRPC 端口)
        let cluster_manager = ClusterManagerRef(if conf.cluster.is_some() {
            Some(Arc::new(ClusterManager::new(&conf).await?))
        } else {
            None
        });

        // 创建 Catalog
        let catalog = Arc::new(Catalog::new(conf.engine.data_dir.clone()).await?);

        // 创建 Engine
        let engine = Engine::new(&conf)?;

        let calm_service = Arc::new(Self {
            catalog,
            cluster_manager,
            engine,
            locker: Arc::new(Locker::new()),
        });

        calm_service.clone().init(&conf).await?;

        Ok(calm_service)
    }

    /// 获取 Engine 引用
    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    /// 获取 Catalog 引用
    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    /// 获取 ClusterManager 引用
    pub async fn cluster_manager(&self) -> CoreResult<Arc<ClusterManager>> {
        self.cluster_manager
            .0
            .clone()
            .ok_or_else(|| CoreError::Internal("No cluster manager in standalone mode".to_string()))
    }

    /// 初始化 DDL Service
    pub async fn init(self: Arc<Self>, conf: &crate::config::Config) -> CoreResult<()> {
        if self.cluster_manager.is_standalone() {
            log::info!("Running in standalone mode");

            for table_name in self.catalog.list_tables().await {
                let table_info = self.catalog.get_or_load_table(&table_name).await?;
                for (partition_name, _partition_meta) in table_info.partitions.read().await.iter() {
                    let partition_dir = self.catalog.partition_dir(&table_name, partition_name);
                    self.engine
                        .load_partition(
                            partition_name,
                            &table_name,
                            partition_dir,
                            table_info.table.schema.clone(),
                        )
                        .await?;
                }
            }

            return Ok(());
        }

        let cluster_manager = self.cluster_manager.as_ref().unwrap();

        log::info!("Running in cluster mode");

        // 从配置文件获取端口
        let cluster_config = conf
            .cluster
            .as_ref()
            .ok_or_else(|| CoreError::ConfigError("Cluster config missing".to_string()))?;

        let tarpc_port = cluster_config.internal_port;
        let flight_port = cluster_config.distributed.grpc_port.ok_or_else(|| {
            CoreError::ConfigError(
                "grpc_port must be configured in cluster.distributed".to_string(),
            )
        })?;

        // 启动集群服务

        // 1. 启动 tarpc RPC 服务器
        let rpc_addr = format!("0.0.0.0:{}", tarpc_port);
        {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let calm_service = self.clone();
            let addr_str = rpc_addr.clone();
            let _handler = tokio::spawn(async move {
                match calm_service.start_rpc_server(addr_str, tx).await {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("❌ tarpc RPC server error: {}", e);
                    }
                }
            });

            rx.await
                .map_err(|_| CoreError::Internal("RPC server failed to start".to_string()))?;
            log::info!("✅ tarpc RPC server started on {}", rpc_addr);
        }

        // 2. 启动 Arrow Flight 服务器
        let custom_flight_addr = format!("0.0.0.0:{}", flight_port);
        let custom_addr = custom_flight_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|e| CoreError::ConfigError(format!("Invalid Flight address: {}", e)))?;

        {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let calm_service = self.clone();
            let _handler = tokio::spawn(async move {
                if let Err(e) =
                    flight_service::start_flight_server(calm_service, custom_addr, tx).await
                {
                    log::error!("❌ Custom Flight server error: {}", e);
                }
            });

            rx.await.map_err(|_| {
                CoreError::Internal("Custom Flight server failed to start".to_string())
            })?;
            log::info!(
                "✅ Custom Arrow Flight server (do_put + SQL) started on {}",
                custom_flight_addr
            );
        }

        cluster_manager.set_my_status_ready().await;

        // 1. 选举并尝试连接中央节点，如果最小节点就是自己，则自己变为中央节点
        loop {
            log::info!("🔍 [Cluster] Finding coordinator node...");
            match cluster_manager.find_coord_node().await {
                Ok(coord) => {
                    log::info!("✅ [Cluster] Found coordinator candidate: {}", coord);

                    if let Err(e) = cluster_manager.set_coord_node(&coord).await {
                        log::warn!(
                            "⚠️  [Cluster] Failed to set coordinator node: {}. Retrying...",
                            e
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }

                    // 检查是否是自己
                    if cluster_manager.am_i_coord_node() {
                        log::info!("🎯 [Cluster] ✅ I AM THE COORDINATOR NODE!");
                    } else {
                        log::info!("📡 [Cluster] I am a worker node, coordinator is: {}", coord);
                    }

                    break;
                }
                Err(e) => {
                    log::warn!(
                        "⚠️  [Cluster] Failed to find coordinator node: {}. Retrying...",
                        e
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        // try to find all partitions router
        let routers = cluster_manager.find_partition_routes().await;
        for table_name in self.catalog.list_tables().await {
            let table_info = self.catalog.get_or_load_table(&table_name).await?;
            {
                let mut partitions = table_info.partitions.write().await;
                for (partition_name, partition) in partitions.iter_mut() {
                    if let Some((n, v)) = routers.get(&(table_name.clone(), partition_name.clone()))
                    {
                        if partition.updated_at < *v {
                            log::info!(
                                "Update partition route: table={}, partition={}, node_id={}, version={}",
                                table_name,
                                partition_name,
                                n,
                                v
                            );
                            partition.owner = n.clone();
                            partition.updated_at = *v;
                        }
                    }
                }
            } // write 锁在这里释放
        }

        // 启动集群后台任务
        let calm_service = self.clone();
        let _handle = tokio::task::spawn(async move {
            if let Err(e) = job::start_cluster_job(calm_service).await {
                log::error!("❌ Cluster job failed: {}", e);
            }
        });

        Ok(())
    }

    /// 启动 tarpc RPC 服务器
    ///
    /// 监听 rpc_addr，处理集群内部的 RPC 请求
    pub async fn start_rpc_server(
        self: Arc<Self>,
        addr_str: String,
        startup_tx: tokio::sync::oneshot::Sender<()>,
    ) -> CoreResult<JoinHandle<()>> {
        let listen_addr = addr_str
            .parse::<SocketAddr>()
            .map_err(|e| CoreError::ConfigError(format!("Invalid RPC listen address: {}", e)))?;

        log::info!("🚀 Starting CalmService RPC server on {}", listen_addr);

        let service_clone = self.clone();

        let handle = tokio::spawn(async move {
            let mut listener =
                match tarpc::serde_transport::tcp::listen(listen_addr, Bincode::default).await {
                    Ok(l) => l,
                    Err(e) => {
                        log::error!("❌ Failed to bind RPC server: {}", e);
                        let _ = startup_tx.send(());
                        return;
                    }
                };

            // 通知服务已启动
            let _ = startup_tx.send(());

            log::info!("✅ CalmService RPC server listening on {}", listen_addr);

            listener.config_mut().max_frame_length(usize::MAX);

            // Accept connections
            loop {
                match listener.next().await {
                    Some(Ok(transport)) => {
                        let service = Arc::as_ref(&service_clone).clone();
                        tokio::spawn(async move {
                            let server = tarpc::server::BaseChannel::with_defaults(transport);
                            let requests = server.execute(service.serve());
                            futures::pin_mut!(requests);
                            while let Some(request) = requests.next().await {
                                tokio::spawn(request);
                            }
                        });
                    }
                    Some(Err(e)) => {
                        log::error!("❌ RPC transport error: {}", e);
                    }
                    None => {
                        log::warn!("⚠️ RPC listener closed");
                        break;
                    }
                }
            }
        });

        Ok(handle)
    }

    /// 创建 Arrow Flight 客户端连接到远程节点
    async fn create_flight_client(&self, node_id: &str) -> CoreResult<FlightClient> {
        // 从 node_id 直接解析出 Flight 地址
        use crate::cluster::keys;
        let addr = keys::parse_flight_address_from_node_id(node_id)
            .ok_or_else(|| CoreError::Internal(format!("Invalid node_id format: {}", node_id)))?;

        let endpoint = format!("http://{}", addr);
        log::info!(
            "🔌 [Flight] Connecting to node '{}' at {}",
            node_id,
            &endpoint
        );

        let channel = TonicChannel::from_shared(endpoint.clone())
            .map_err(|e| {
                log::error!("❌ [Flight] Invalid endpoint '{}': {}", &endpoint, e);
                CoreError::Internal(format!("Invalid endpoint: {}", e))
            })?
            .connect()
            .await
            .map_err(|e| {
                log::error!("❌ [Flight] Failed to connect to '{}': {}", &endpoint, e);
                CoreError::Network(format!("Failed to connect: {}", e))
            })?;

        log::info!("✅ [Flight] Connected to node '{}'", node_id);
        Ok(FlightClient::new(channel))
    }

    /// 通过 Arrow Flight do_put 向远程分区插入数据
    async fn flight_do_put(
        &self,
        node_id: &str,
        table_name: &str,
        partition_name: &str,
        batch: RecordBatch,
    ) -> CoreResult<()> {
        log::info!(
            "📡 [Flight] Starting do_put to node '{}' for partition '{}'",
            node_id,
            partition_name
        );

        let mut client = self.create_flight_client(node_id).await?;

        // 创建 FlightDescriptor，包含表名和分区名
        let descriptor =
            FlightDescriptor::new_path(vec![table_name.to_string(), partition_name.to_string()]);

        log::info!(
            "📡 [Flight] Sending {} rows to {}/{}",
            batch.num_rows(),
            table_name,
            partition_name
        );

        // 使用 FlightDataEncoder 将 RecordBatch 编码为 FlightData 流
        let schema = batch.schema();
        let encoder = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(descriptor))
            .with_schema(schema)
            .build(stream::iter(vec![Ok(batch)]));

        log::info!("📡 [Flight] Calling do_put...");

        // 调用 do_put 发送数据
        let response = client.do_put(encoder).await.map_err(|e| {
            log::error!("❌ [Flight] do_put failed: {}", e);
            CoreError::Network(format!("Flight do_put failed: {}", e))
        })?;

        log::info!("📡 [Flight] do_put returned, reading response...");

        // 读取响应以确认完成
        let _results: Vec<PutResult> = response.try_collect().await.map_err(|e| {
            log::error!("❌ [Flight] Failed to read do_put response: {}", e);
            CoreError::Network(format!("Failed to read do_put response: {}", e))
        })?;

        log::info!(
            "✅ [Flight] Successfully sent batch to remote partition '{}' on node '{}'",
            partition_name,
            node_id
        );

        Ok(())
    }

    /// 执行 SQL 查询并返回流
    pub async fn execute_query_stream(
        &self,
        sql: &str,
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        self.execute_query_stream_with_options(sql, None, false)
            .await
    }

    /// 执行本地 SQL 查询并返回流（不进行 Federation）
    pub async fn execute_local_query_stream(
        &self,
        sql: &str,
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        self.execute_query_stream_with_options(sql, None, true)
            .await
    }

    /// 仅用于 Arrow Flight 内部调用，强制指定分区
    pub async fn execute_local_query_stream_with_partitions(
        &self,
        sql: &str,
        partition_names: &[String],
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        self.execute_query_stream_with_options(sql, Some(partition_names), true)
            .await
    }

    async fn execute_query_stream_with_options(
        &self,
        sql: &str,
        partition_hint: Option<&[String]>,
        local_only: bool,
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        log::info!(
            "🚀 [CalmService] Executing{} query: {}",
            if local_only { " LOCAL" } else { "" },
            sql
        );

        let normalized = SqlNormalizer::normalize(sql)?;
        self.execute_normalized_query(&normalized, partition_hint, local_only)
            .await
    }

    async fn execute_normalized_query(
        &self,
        normalized: &NormalizedSql,
        partition_hint: Option<&[String]>,
        local_only: bool,
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        use datafusion::prelude::*;

        if let Some(cm) = self.cluster_manager.as_ref() {
            use crate::compute::federation::FederatedQueryExecutor;

            let executor =
                FederatedQueryExecutor::new(self.catalog.clone(), self.engine.clone(), cm.clone());

            return executor
                .execute_with_partitions(normalized, partition_hint, local_only)
                .await;
        }

        // 单机模式下直接使用 DataFusion 执行改写后的 SQL
        let ctx = SessionContext::new();
        let fulltext_context = register_fulltext_udfs(&ctx);
        if normalized.needs_score_column {
            fulltext_context.clear_scores();
        }
        self.register_local_tables_for_session(
            &ctx,
            &normalized.rewritten_sql,
            normalized.needs_score_column,
        )
        .await?;
        let df = ctx
            .sql(&normalized.rewritten_sql)
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to parse SQL: {}", e)))?;

        let stream = df
            .execute_stream()
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to execute query: {}", e)))?;
        let stream = if normalized.needs_score_column {
            wrap_stream_with_scores(stream, fulltext_context.clone())
        } else {
            stream
        };

        Ok(stream)
    }

    async fn register_local_tables_for_session(
        &self,
        ctx: &datafusion::prelude::SessionContext,
        sql: &str,
        needs_internal_id: bool,
    ) -> CoreResult<()> {
        let table_names = Self::extract_table_names_from_sql(sql)?;

        for table_name in table_names {
            self.register_single_local_table(ctx, &table_name, needs_internal_id)
                .await?;
        }

        Ok(())
    }

    async fn register_single_local_table(
        &self,
        ctx: &datafusion::prelude::SessionContext,
        table_name: &str,
        needs_internal_id: bool,
    ) -> CoreResult<()> {
        let table_info = self.catalog.get_or_load_table(table_name).await?;
        let schema = table_info.table.schema.to_arrow_schema();

        let partition_names = self.engine.list_partitions(table_name).await;
        let mut partitions = Vec::new();

        for partition_name in partition_names {
            match self.engine.get_partition(table_name, &partition_name).await {
                Some(partition) => partitions.push(partition),
                None => log::warn!(
                    "[CalmService] Partition '{}' for table '{}' not found locally",
                    partition_name,
                    table_name
                ),
            }
        }

        if partitions.is_empty() {
            log::debug!(
                "[CalmService] No local partitions found for table '{}', registering empty table",
                table_name
            );
            let empty_table = EmptyTable::new(schema);
            ctx.register_table(table_name, Arc::new(empty_table))
                .map_err(|e| {
                    CoreError::Internal(format!(
                        "Failed to register empty table '{}': {}",
                        table_name, e
                    ))
                })?;
            return Ok(());
        }

        let provider = UnionTableProvider::new(
            partitions,
            table_name.to_string(),
            self.engine.clone(),
            schema,
            needs_internal_id,
        )?;

        ctx.register_table(table_name, Arc::new(provider))
            .map_err(|e| {
                CoreError::Internal(format!("Failed to register table '{}': {}", table_name, e))
            })?;

        Ok(())
    }

    fn extract_table_names_from_sql(sql: &str) -> CoreResult<Vec<String>> {
        use sqlparser::ast::{SetExpr, Statement, TableFactor};
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| CoreError::Internal(format!("Failed to parse SQL: {}", e)))?;

        let mut table_names = Vec::new();

        for statement in statements {
            if let Statement::Query(query) = statement {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    for table_with_joins in &select.from {
                        if let TableFactor::Table { name, .. } = &table_with_joins.relation {
                            let table_name = name.to_string();
                            if !table_names.contains(&table_name) {
                                table_names.push(table_name);
                            }
                        }

                        for join in &table_with_joins.joins {
                            if let TableFactor::Table { name, .. } = &join.relation {
                                let table_name = name.to_string();
                                if !table_names.contains(&table_name) {
                                    table_names.push(table_name);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(table_names)
    }

    /// 插入数据到表
    ///
    /// 内部自动路由到各个分区，本地直接插入，远程通过 Flight do_put
    pub async fn insert_data(
        &self,
        table_name: &str,
        batch: datafusion::arrow::record_batch::RecordBatch,
    ) -> CoreResult<usize> {
        use crate::storage::router::Router;

        log::info!(
            "📝 [CalmService] Inserting {} rows to table '{}'",
            batch.num_rows(),
            table_name
        );

        let table_info = self.catalog.get_or_load_table(table_name).await?;
        let meta = &table_info.table;

        // 路由到各个分区
        let table_meta_arc = Arc::new(meta.clone());
        let routed_batches = Router::route_batch(batch, &table_meta_arc)?;

        if routed_batches.is_empty() {
            log::warn!(
                "⚠️  [CalmService] Router returned empty result for table '{}'",
                table_name
            );
            return Err(CoreError::Internal(
                "Router returned no partitions. Check partition strategy.".to_string(),
            ));
        }

        log::debug!(
            "📊 [CalmService] Routed to {} partition(s)",
            routed_batches.len()
        );

        let table = self.catalog.get_or_load_table(table_name).await?;
        let my_node_id = self.cluster_manager.node_id_without_none().to_string();

        let self_for_tasks = self.clone();
        let catalog_for_loop = self.catalog.clone();

        let mut tasks = Vec::new();

        for (partition_name, batch) in routed_batches.into_iter() {
            let node_id = table
                .partitions
                .read()
                .await
                .get(&partition_name)
                .map(|p| p.owner.clone());

            let owner_node = match node_id {
                Some(node_id) => node_id,
                None => {
                    log::warn!(
                        "⚠️  [CalmService] Partition '{}' does not exist in table '{}'",
                        partition_name,
                        table_name
                    );
                    if !table.table.partition_strategy.should_precreate_partitions() {
                        let partition_meta = self_for_tasks
                            .clone()
                            .create_partition(
                                tarpc::context::current(),
                                table_name.to_string(),
                                partition_name.to_string(),
                            )
                            .await?;

                        let partition_owner = partition_meta.owner.clone();

                        catalog_for_loop
                            .update_partition_meta(table_name, partition_meta)
                            .await?;

                        partition_owner
                    } else {
                        let table_detail = self_for_tasks
                            .clone()
                            .get_table_detail(tarpc::context::current(), table_name.to_string())
                            .await?;

                        let partition_owner = table_detail
                            .partitions
                            .get(&partition_name)
                            .map(|p| p.owner.clone())
                            .ok_or_else(|| {
                                CoreError::NotExisted(format!(
                                    "Partition '{}/{}' does not exist and should be pre-created",
                                    table_name, partition_name
                                ))
                            })?;

                        catalog_for_loop
                            .force_update_table_detail(table_detail)
                            .await?;

                        partition_owner
                    }
                }
            };

            let self_clone = self_for_tasks.clone();
            let table_name = table_name.to_string();
            let partition_name = partition_name.to_string();
            let is_remote = !owner_node.eq(&my_node_id);

            let handle = tokio::spawn(async move {
                let rows = batch.num_rows();

                if is_remote {
                    self_clone
                        .flight_do_put(&owner_node, &table_name, &partition_name, batch)
                        .await?;
                } else {
                    self_clone
                        .engine
                        .insert_batch(&table_name, &partition_name, batch)
                        .await?;
                }

                Ok::<usize, CoreError>(rows)
            });
            tasks.push(handle);
        }

        let mut total_inserted = 0;

        for task in tasks {
            match task.await {
                Ok(Ok(rows)) => {
                    total_inserted += rows;
                }
                Ok(Err(e)) => {
                    return Err(e);
                }
                Err(e) => {
                    return Err(CoreError::Internal(format!("Task panicked: {}", e)));
                }
            }
        }

        log::info!(
            "✅ [CalmService] Successfully inserted {} rows to table '{}'",
            total_inserted,
            table_name
        );

        Ok(total_inserted)
    }

    pub async fn stop(&self) -> CoreResult<()> {
        self.engine.stop().await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct ClusterManagerRef(Option<Arc<ClusterManager>>);

impl ClusterManagerRef {
    pub fn is_standalone(&self) -> bool {
        self.0.is_none()
    }

    pub fn as_ref(&self) -> Option<&Arc<ClusterManager>> {
        self.0.as_ref()
    }

    pub fn node_id(&self) -> Option<&str> {
        match &self.0 {
            Some(cm) => Some(cm.node_id()),
            None => None,
        }
    }

    pub fn node_id_without_none(&self) -> &str {
        match &self.0 {
            Some(cm) => cm.node_id(),
            None => "standalone",
        }
    }

    pub async fn idle_nodes(&self) -> CoreResult<Vec<String>> {
        match &self.0 {
            Some(cm) => Ok(cm.idle_nodes().await),
            None => Ok(vec![keys::SINGLE_NODE_CLUSTER_ID.to_string()]),
        }
    }

    pub fn am_i_coord_node(&self) -> bool {
        match &self.0 {
            Some(cm) => cm.am_i_coord_node(),
            None => true,
        }
    }

    pub async fn publish_partition(&self, table_name: &str, partition_name: &str) {
        if let Some(cm) = &self.0 {
            log::info!(
                "📡 [DataNode] Publishing partition '{}/{}' to gossip ",
                table_name,
                partition_name,
            );

            cm.put_partition(table_name, partition_name).await;
        }
    }

    pub async fn remove_partition(&self, table_name: &str, partition_name: &str) {
        if let Some(cm) = &self.0 {
            log::info!(
                "🗑️  [DataNode] Removing partition '{}/{}' from gossip",
                table_name,
                partition_name
            );

            cm.remove_partition(table_name, partition_name).await;
        }
    }

    async fn coord_id(&self) -> CoreResult<String> {
        let cm = self
            .0
            .as_ref()
            .ok_or_else(|| CoreError::ClusterState("Not in cluster mode".to_string()))?;
        let coord_id = cm
            .get_coord()
            .await
            .ok_or_else(|| CoreError::ClusterState("Coordinator not set".to_string()))?;
        Ok(coord_id)
    }
}

pub(crate) async fn new_data_client(node_id: &str) -> CoreResult<CalmRpcServiceClient> {
    // 从 node_id 解析 tarpc 地址
    let tarpc_addr = crate::cluster::keys::parse_tarpc_address_from_node_id(node_id)
        .ok_or_else(|| CoreError::Internal(format!("Invalid node_id format: {}", node_id)))?;

    log::info!(
        "📤 [Node] Connecting to calm_service {} at {}",
        node_id,
        tarpc_addr
    );

    let transport = tarpc::serde_transport::tcp::connect(tarpc_addr, Bincode::default)
        .await
        .map_err(|e| {
            log::error!("❌ Failed to connect to node {}: {}", node_id, e);
            CoreError::Network(format!("Failed to connect to node: {}", e))
        })?;

    // 配置 RPC client: 增加超时时间和并发数
    let mut client_config = tarpc::client::Config::default();
    client_config.max_in_flight_requests = 1000;
    client_config.pending_request_buffer = 1000;

    log::info!("✅ Connected to node {}", node_id);

    Ok(CalmRpcServiceClient::new(client_config, transport).spawn())
}
