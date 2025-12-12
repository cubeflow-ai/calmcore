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

    /// tarpc server 监听地址
    rpc_addr: SocketAddr,
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
        cluster_manager: Arc<ClusterManager>,
        rpc_addr: SocketAddr,
    ) -> Self {
        Self {
            ddl: DDLServiceImpl::new(engine.clone(), catalog.clone(), cluster_manager.clone()),
            dml: DMLService::new(),
            rpc_addr,
        }
    }

    /// 启动 tarpc RPC 服务器
    ///
    /// 监听 rpc_addr，处理集群内部的 RPC 请求
    pub async fn start_rpc_server(self: Arc<Self>) -> Result<JoinHandle<()>, std::io::Error> {
        let listener =
            tarpc::serde_transport::tcp::listen(&self.rpc_addr, Bincode::default).await?;

        log::info!("🚀 CalmService RPC server listening on {}", self.rpc_addr);

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

        Ok(handle)
    }

    /// 获取 DDL Service（用于本地调用）
    pub fn ddl_service(&self) -> &DDLServiceImpl {
        &self.ddl
    }

    /// 获取 DML Service（用于本地调用）
    pub fn dml_service(&self) -> &DMLService {
        &self.dml
    }
}
