use std::{net::SocketAddr, sync::Arc};

use futures::StreamExt;
use tarpc::server::Channel;
use tokio::task::JoinHandle;
use tokio_serde::formats::Bincode;

use crate::{
    catalog::Catalog,
    cluster::ClusterManager,
    engine::Engine,
    service::{
        ddl::{DDLService, DDLServiceImpl},
        dml::DMLService,
    },
    utils::error::{CoreError, CoreResult},
};

pub(crate) mod ddl;
pub(crate) mod dml;

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
}

impl CalmService {
    /// 创建 CalmService
    ///
    /// 各个能力独立传入：
    /// - engine: 本地数据操作
    /// - catalog: 元数据管理
    /// - cluster_manager: 集群管理
    pub fn new(
        engine: Arc<Engine>,
        catalog: Arc<Catalog>,
        cluster_manager: Option<Arc<ClusterManager>>,
    ) -> Self {
        Self {
            ddl: DDLServiceImpl::new(engine.clone(), catalog.clone(), cluster_manager.clone()),
            dml: DMLService::new(),
            catalog,
            cluster_manager,
            engine,
        }
    }

    /// 初始化 DDL Service
    pub async fn init(self: Arc<Self>) -> Result<JoinHandle<()>, CoreError> {
        //启动内部结点服务
        let calm_service = self.clone();
        let handler = tokio::spawn(async move {
            if let Err(e) = calm_service.start_rpc_server().await {
                panic!("❌ Internal RPC server error: {}", e);
            }
        });

        // 1. 获取中央结点。
        if let Some(cm) = &self.cluster_manager {
            let coor = cm.node_manager.run_election().await?;
            // 1.如果中央结点是自己，
            // 2.加载所有tables
            // 3.加载全部路由
            // 4.将当前节点状态设置为 Ready
            cm.set_my_status_ready().await;
        } else {
            // 2.加载所有tables
        }

        Ok(handler)
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
        self.engine.stop().await?;
        //TODO: ANSJ
        Ok(())
    }
}
