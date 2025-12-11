//! 内部 GraphQL API - 用于集群节点间通信
//!
//! 提供分区级别的操作接口，仅供集群内部节点调用，需要 cluster_id 认证。

use std::sync::Arc;

use async_graphql::{Context, EmptySubscription, Object, Result, Schema};
use poem::{handler, http::StatusCode, web::Data, Endpoint, Middleware, Request};

use crate::engine::Engine;

/// 空查询（内部 API 不需要查询）
pub struct EmptyQuery;

#[Object]
impl EmptyQuery {
    /// Ping 查询，用于健康检查
    async fn ping(&self) -> bool {
        true
    }
}

/// 内部 GraphQL Schema
pub type InternalGraphQLSchema = Schema<EmptyQuery, InternalMutationRoot, EmptySubscription>;

/// 创建内部 GraphQL Schema
pub fn create_internal_schema(engine: Arc<Engine>) -> InternalGraphQLSchema {
    Schema::build(EmptyQuery, InternalMutationRoot, EmptySubscription)
        .data(engine)
        .finish()
}

/// 内部 Mutation Root
pub struct InternalMutationRoot;

#[Object]
impl InternalMutationRoot {
    /// 在本地创建分区（由协调节点调用）
    ///
    /// 此方法只在本地加载分区，不会再次广播到其他节点。
    async fn create_partition(
        &self,
        ctx: &Context<'_>,
        table_name: String,
        partition_name: String,
    ) -> Result<bool> {
        let engine = ctx.data::<Arc<Engine>>()?;

        // 加载分区到内存
        engine
            .local_load_partition(&table_name, &partition_name)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        log::info!(
            "✅ Internal: partition created {}:{}",
            table_name,
            partition_name
        );
        Ok(true)
    }

    /// 在本地删除分区（由协调节点调用）
    ///
    /// 此方法只在本地删除分区，不会再次广播到其他节点。
    async fn drop_partition(
        &self,
        ctx: &Context<'_>,
        table_name: String,
        partition_name: String,
    ) -> Result<bool> {
        let engine = ctx.data::<Arc<Engine>>()?;

        engine
            .local_drop_partition(&table_name, &partition_name)
            .await;

        log::info!(
            "✅ Internal: partition dropped {}:{}",
            table_name,
            partition_name
        );
        Ok(true)
    }
}

/// 验证 cluster_id 中间件
pub struct VerifyClusterId {
    expected_cluster_id: String,
}

impl VerifyClusterId {
    pub fn new(expected_cluster_id: String) -> Self {
        Self {
            expected_cluster_id,
        }
    }
}

impl<E: Endpoint> Middleware<E> for VerifyClusterId {
    type Output = VerifyClusterIdEndpoint<E>;

    fn transform(&self, ep: E) -> Self::Output {
        VerifyClusterIdEndpoint {
            inner: ep,
            expected_cluster_id: self.expected_cluster_id.clone(),
        }
    }
}

pub struct VerifyClusterIdEndpoint<E> {
    inner: E,
    expected_cluster_id: String,
}

impl<E: Endpoint> Endpoint for VerifyClusterIdEndpoint<E> {
    type Output = E::Output;

    async fn call(&self, req: Request) -> poem::Result<Self::Output> {
        let provided = req
            .headers()
            .get("X-Cluster-Id")
            .and_then(|v| v.to_str().ok());

        if provided != Some(&self.expected_cluster_id) {
            log::warn!(
                "🔒 Internal API: Unauthorized (expected: {}, provided: {:?})",
                self.expected_cluster_id,
                provided
            );
            return Err(poem::Error::from_status(StatusCode::FORBIDDEN));
        }

        self.inner.call(req).await
    }
}

/// 内部 GraphQL 处理器
#[handler]
pub async fn internal_graphql_handler(
    req: poem::web::Json<async_graphql::Request>,
    schema: Data<&InternalGraphQLSchema>,
) -> poem::Result<poem::web::Json<async_graphql::Response>> {
    let response = schema.execute(req.0).await;
    Ok(poem::web::Json(response))
}
