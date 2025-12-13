use std::{net::SocketAddr, sync::Arc};

use futures::{lock::Mutex, StreamExt};
use tarpc::{client::Config, server::Channel};
use tokio::task::JoinHandle;
use tokio_serde::formats::Bincode;

use crate::{
    catalog::Catalog,
    cluster::{self, ClusterManager},
    engine::Engine,
    service::{
        ddl::{DDLService, DDLServiceImpl},
        dml::DMLService,
        job::start_cluster_job,
    },
    utils::error::{CoreError, CoreResult},
};

pub(crate) mod ddl;
pub(crate) mod dml;
mod job;

/// Calm 服务 - 业务协调层
///
/// **架构定位**：
/// - **Engine**: 本地数据操作（加载分区、查询、写入）
/// - **Catalog**: 元数据管理、缓存
/// - **ClusterManager**: Gossip 协议、节点发现
/// - **CalmService**: 协调各种能力，实现复杂业务逻辑
/// - **协议层**: 直接调用 CalmService
///
/// **服务包含**：
/// - DDL Service: 表的创建/删除等元数据操作（协调 Engine + Catalog + ClusterManager）
/// - DML Service: 数据的插入/查询等（协调 Engine + 路由）
pub struct CalmService {
    /// DDL 服务实现
    ddl: DDLServiceImpl,

    /// DML 服务实现
    dml: DMLService,

    catalog: Arc<Catalog>,
    cluster_manager: Option<Arc<ClusterManager>>,
    engine: Arc<Engine>,

    handle: Mutex<Option<JoinHandle<()>>>,
}

impl CalmService {
    pub async fn new(conf: crate::config::Config) -> CoreResult<Arc<Self>> {
        // 初始化集群
        let cluster_manager = if conf.cluster.is_some() {
            Some(Arc::new(ClusterManager::new(&conf).await?))
        } else {
            None
        };

        // 创建 Catalog
        let catalog = Arc::new(Catalog::new(conf.engine.data_dir.clone()).await?);

        // 创建 Engine
        let engine = Engine::new(&conf)?;

        let calm_service = Arc::new(Self {
            ddl: DDLServiceImpl::new(engine.clone(), catalog.clone(), cluster_manager.clone()),
            dml: DMLService::new(),
            catalog,
            cluster_manager,
            engine,
        });

        calm_service.init().await?;

        Ok(calm_service)
    }

    /// 初始化 DDL Service
    pub async fn init(self: Arc<Self>) -> CoreResult<()> {
        if self.cluster_manager.is_none() {
            log::info!("Running in standalone mode");

            for table_name in self.catalog.list_tables() {
                let table_info = self.catalog.get_or_load_table(&table_name).await?;
                for partition in table_info.partitions.read().unwrap().iter() {
                    let partition_name = &partition.partition.partition_name;
                    let partition_dir = self.catalog.partition_dir(&table_name, &partition_name);
                    self.engine
                        .load_partition(
                            &partition.partition.partition_name,
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

        let calm_service = self.clone();
        let handler = tokio::spawn(async move {
            if let Err(e) = calm_service.start_rpc_server().await {
                panic!("❌ Internal RPC server error: {}", e);
            }
        });
        self.handle.lock().await.replace(handler);

        // try to find all partitions router

        // 1. 选举并尝试连接中央节点，如果最小节点就是自己，则自己变为中央节点

        for table_name in self.catalog.list_tables() {
            let table_info = self.catalog.get_or_load_table(&table_name).await?;

            let mut partitions = table_info.partitions.write().unwrap();

            for partition in partitions.iter_mut() {
                let partition_name = &partition.partition.partition_name;
                // 加载所有表的路由信息，
                // 先用初始partition中的onwer信息 ，从cluster 获取， onwer 和 addr 填写回 PartitionInfo
                // 如果获取不到，则置为 None，等待后续更新
                cluster_manager.partition_manager
                    .find_partition_route(&table_name, partition_name)
                    .await?;
            }
        }

        tokio::spawn(start_cluster_job(self.clone()));

        Ok(())
    }

    /// 启动 tarpc RPC 服务器
    ///
    /// 监听 rpc_addr，处理集群内部的 RPC 请求
    pub async fn start_rpc_server(self: Arc<Self>) -> Result<JoinHandle<()>, std::io::Error> {
        if let Some(cm) = self.cluster_manager.as_ref() {
            let config = cm.config();
            let realip = config.real_ip().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Real IP must be set in cluster mode",
                )
            })?;

            let addr_str = format!("{}:{}", realip, config.internal_port);

            let internal_listen_addr = addr_str.parse::<SocketAddr>().map_err(|e| {
                CoreError::ConfigError(format!("Invalid RPC listen address: {}", e))
            })?;

            let listener =
                tarpc::serde_transport::tcp::listen(internal_listen_addr, Bincode::default).await?;

            log::info!(
                "🚀 CalmService RPC server listening on {}",
                internal_listen_addr
            );

            let handle = tokio::spawn(async move {
                listener
                    .filter_map(|r| async move {
                        match r {
                            Ok(transport) => Some(transport),
                            Err(e) => {
                                log::warn!("❌ Failed to accept connection: {}", e);
                                None
                            }
                        }
                    })
                    // 为每个连接创建一个 channel
                    .for_each_concurrent(None, move |transport| {
                        let ddl_service = self.ddl.clone();
                        async move {
                            let server = tarpc::server::BaseChannel::with_defaults(transport);
                            server
                                .execute(ddl_service.serve())
                                .for_each(|response| async move {
                                    tokio::spawn(response);
                                })
                                .await;
                        }
                    })
                    .await;
            });
            cm.node_manager.set_internal_addr(&addr_str).await;
            Ok(handle)
        } else {
            Ok(tokio::spawn(async {}))
        }
    }

    /// 获取 DDL Service（用于本地调用）
    pub fn ddl_service(&self) -> &DDLServiceImpl {
        &self.ddl
    }

    /// 获取 DML Service（用于本地调用）
    pub fn dml_service(&self) -> &DMLService {
        &self.dml
    }

    pub async fn stop(&self) -> CoreResult<()> {
        // stop internal rpc server
        if let Some(handle) = self.handle.lock().await.take() {
            handle.abort();
        }

        self.engine.stop().await?;
        //TODO: ANSJ
        Ok(())
    }
}
