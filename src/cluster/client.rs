//! 集群 RPC 客户端
//!
//! 提供节点间通信的高层接口，包括重试逻辑和错误处理

use std::{sync::Arc, time::Duration};

use base64::engine;
use chitchat::{Chitchat, ChitchatId};
use serde_json::json;

use crate::{cluster::ClusterManager, utils::error::CoreResult};

/// 重试配置
const MAX_RETRIES: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_millis(500);

impl ClusterManager {
    /// 通知所有活跃节点创建分区
    ///
    /// 此方法会向集群中所有活跃节点发送创建分区的请求。
    /// 如果某个节点请求失败，会自动重试最多 3 次。
    pub async fn call_create_partition(
        &self,
        table_name: &str,
        partition_name: &str,
    ) -> CoreResult<()> {
        let cluster_id = &self.config().cluster_id;

        let nodes = self.node_manager.idle_nodes().await;

        if nodes.is_empty() {
            return Err(crate::utils::error::CoreError::Internal(
                "No available node to create partition".to_string(),
            ));
        }

        for target_node in nodes {
            let query = format!(
                r#"mutation {{ createPartition(tableName: "{}", partitionName: "{}") }}"#,
                table_name, partition_name,
            );

            let addr = self.node_manager.node_internal_addr(&target_node).await?;

            self.send_internal_request_with_retry(&addr, &query, cluster_id)
                .await?;
            log::info!(
                "✅ Successfully broadcast create_partition: {}:{}",
                table_name,
                partition_name
            );
            return Ok(());
        }

        Err(crate::utils::error::CoreError::Internal(
            "Failed to create partition on any node".to_string(),
        ))
    }

    /// 通知指定节点删除分区
    pub async fn call_drop_partition(
        &self,
        node_id: &str,
        table_name: &str,
        partition_name: &str,
    ) -> CoreResult<()> {
        let cluster_id = &self.config().cluster_id;

        // 查找目标节点
        let target_node = self.node_manager.get_node(node_id).await.ok_or_else(|| {
            crate::utils::error::CoreError::Internal(format!("Node {} not found", node_id))
        })?;

        log::info!(
            "📡 Sending drop_partition to {}: {}:{}",
            node_id,
            table_name,
            partition_name
        );

        let query = format!(
            r#"mutation {{ dropPartition(tableName: "{}", partitionName: "{}") }}"#,
            table_name, partition_name,
        );

        let addr = self.node_manager.node_internal_addr(&target_node).await?;

        self.send_internal_request_with_retry(&addr, &query, cluster_id)
            .await?;

        log::info!(
            "✅ Successfully sent drop_partition to {}: {}:{}",
            node_id,
            table_name,
            partition_name
        );
        Ok(())
    }

    /// 通知所有活跃节点删除表
    pub async fn call_drop_table(&self, table_name: &str) -> CoreResult<()> {
        let cluster_id = &self.config().cluster_id;
        let nodes = self.node_manager.live_nodes().await;

        log::info!(
            "📡 Broadcasting drop_table to {} nodes: {}",
            nodes.len(),
            table_name
        );

        for node in nodes {
            // 跳过自己（本地已经删除了）
            if node.node_id == *self.node_id() {
                continue;
            }

            let query = format!(r#"mutation {{ dropTable(tableName: "{}") }}"#, table_name);

            match self.node_manager.node_internal_addr(&node).await {
                Ok(addr) => {
                    self.send_internal_request_with_retry(&addr, &query, cluster_id)
                        .await?
                }
                Err(_) => continue,
            };
        }

        log::info!("✅ Successfully broadcast drop_table: {}", table_name);
        Ok(())
    }

    /// 检查节点是否存活
    ///
    /// 通过发送 ping 查询来检测节点的健康状态
    pub async fn call_check_alive(&self, node: &ChitchatId) -> bool {
        let cluster_id = &self.config().cluster_id;
        let query = r#"query { ping }"#;

        // 构造节点地址
        match self.node_manager.node_internal_addr(&node).await {
            Ok(addr) =>
            // 使用单次请求，不重试（快速失败）
            {
                match self.send_internal_request(&addr, query, cluster_id).await {
                    Ok(_) => {
                        log::debug!("✅ Node {} is alive", node.node_id);
                        true
                    }
                    Err(e) => {
                        log::debug!("❌ Node {} is not responding: {}", node.node_id, e);
                        false
                    }
                }
            }
            Err(_) => {
                log::warn!("❌ Cannot determine address for node {}", node.node_id);
                false
            }
        }
    }

    /// 发送内部 GraphQL 请求（带重试逻辑）
    ///
    /// 此方法会在请求失败时自动重试最多 MAX_RETRIES 次，
    /// 每次重试之间会等待 RETRY_DELAY。
    async fn send_internal_request_with_retry(
        &self,
        node_addr: &str,
        query: &str,
        cluster_id: &str,
    ) -> CoreResult<()> {
        let mut last_error = None;

        for attempt in 1..=MAX_RETRIES {
            match self
                .send_internal_request(node_addr, query, cluster_id)
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    log::warn!(
                        "⚠️  Internal request to {} failed (attempt {}/{}): {}",
                        node_addr,
                        attempt,
                        MAX_RETRIES,
                        e
                    );
                    last_error = Some(e);

                    // 如果还有重试机会，等待一段时间
                    if attempt < MAX_RETRIES {
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            crate::utils::error::CoreError::Internal("Unknown error".to_string())
        }))
    }

    /// 发送内部 GraphQL 请求（单次，不重试）
    ///
    /// 携带 X-Cluster-Id 头部进行认证
    async fn send_internal_request(
        &self,
        node_addr: &str,
        query: &str,
        cluster_id: &str,
    ) -> CoreResult<()> {
        let url = format!("http://{}/_internal/graphql", node_addr);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| {
                crate::utils::error::CoreError::Internal(format!(
                    "Failed to create HTTP client: {}",
                    e
                ))
            })?;

        let response = client
            .post(&url)
            .header("X-Cluster-Id", cluster_id)
            .header("Content-Type", "application/json")
            .json(&json!({ "query": query }))
            .send()
            .await
            .map_err(|e| {
                crate::utils::error::CoreError::Network(format!(
                    "Failed to send request to {}: {}",
                    node_addr, e
                ))
            })?;

        let status = response.status();

        if !status.is_success() {
            let error_body = response.text().await.unwrap_or_default();
            return Err(crate::utils::error::CoreError::Internal(format!(
                "Internal request failed: status={}, node={}, body={}",
                status, node_addr, error_body
            )));
        }

        // 解析响应检查是否有 GraphQL 错误
        let body: serde_json::Value = response.json().await.map_err(|e| {
            crate::utils::error::CoreError::Internal(format!("Failed to parse response: {}", e))
        })?;

        if let Some(errors) = body.get("errors") {
            return Err(crate::utils::error::CoreError::Internal(format!(
                "GraphQL errors: {}",
                errors
            )));
        }

        Ok(())
    }
}
