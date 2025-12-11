//! 查询协调器
//!
//! 统一的查询入口，自动判断使用单机还是分布式执行。
//!
//! ## 核心职责
//!
//! 1. 判断当前是单机还是集群模式
//! 2. 单机模式：直接调用 DataFusionExecutor（现有代码，零修改）
//! 3. 集群模式：根据 Partition 分布决定执行策略
//!
//! ## 设计原则
//!
//! - **零侵入**：单机模式完全不经过分布式代码
//! - **复用**：分布式模式复用现有的 SegmentScanner 等组件

use std::sync::Arc;

use datafusion::physical_plan::SendableRecordBatchStream;

use super::config::DistributedConfig;
use crate::cluster::ClusterManager;
use crate::engine::Engine;
use crate::utils::error::CoreResult;

/// 查询协调器
///
/// 统一的查询入口，根据集群状态自动选择执行路径：
/// - 单机模式：直接使用 DataFusionExecutor
/// - 分布式模式：使用 DistributedExecutor
pub struct QueryCoordinator {
    /// 引擎实例
    engine: Arc<Engine>,

    /// 集群管理器（可选，单机模式为 None）
    cluster_manager: Option<Arc<ClusterManager>>,

    /// 分布式配置
    config: DistributedConfig,
}

impl QueryCoordinator {
    /// 创建单机模式的 QueryCoordinator
    ///
    /// 单机模式下，所有查询直接走 DataFusionExecutor
    pub fn new(engine: Arc<Engine>, cluster_manager: Option<Arc<ClusterManager>>) -> Self {
        Self {
            engine,
            cluster_manager,
            config: DistributedConfig::default(),
        }
    }

    /// 检查是否为单机模式
    pub fn is_standalone(&self) -> bool {
        self.cluster_manager.is_none()
    }

    /// 执行 SQL 查询
    ///
    /// 根据当前模式自动选择执行路径：
    /// - 单机模式：直接调用 DataFusionExecutor
    /// - 分布式模式：使用 DistributedExecutor
    pub async fn execute_sql(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
        // 🔑 关键：单机模式直接走现有代码路径，零侵入
        if self.is_standalone() {
            log::debug!("[QueryCoordinator] Standalone mode, using DataFusionExecutor");
            return self.execute_standalone(sql).await;
        }

        // 分布式模式
        log::debug!("[QueryCoordinator] Distributed mode, checking partition distribution");
        self.execute_distributed(sql).await
    }

    /// 单机模式执行
    ///
    /// 直接使用现有的 Executor，完全不经过分布式代码
    async fn execute_standalone(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
        // 复用现有的 Executor
        let executor = crate::compute::Executor::new(self.engine.clone());
        executor.execute_sql_stream(sql).await
    }

    /// 分布式模式执行
    ///
    /// 根据 Partition 分布决定执行策略：
    /// - 所有 Partition 在本地：使用本地执行
    /// - 存在远程 Partition：使用分布式执行
    async fn execute_distributed(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
        todo!()
        // let cluster_manager = self.cluster_manager.as_ref().unwrap();
        // let partition_manager = self.partition_manager.as_ref().unwrap();

        // // 获取本节点 ID
        // let my_node_id = cluster_manager.node_id();

        // // 提取表名
        // let table_name = self.extract_table_name(sql)?;

        // // 获取表的 Partition 拓扑
        // let topology = match partition_manager.get_table_topology(&table_name).await {
        //     Some(t) => t,
        //     None => {
        //         // 表不存在于集群拓扑中，使用本地执行
        //         log::debug!(
        //             "[QueryCoordinator] Table '{}' not in cluster topology, using standalone",
        //             table_name
        //         );
        //         return self.execute_standalone(sql).await;
        //     }
        // };

        // // 分类本地和远程 Partition
        // let (local_partitions, remote_partitions) =
        //     self.classify_partitions(&topology, &my_node_id);

        // log::info!(
        //     "[QueryCoordinator] Table '{}': {} local partitions, {} remote partitions",
        //     table_name,
        //     local_partitions.len(),
        //     remote_partitions.len()
        // );

        // // 如果所有 Partition 都在本地，使用本地执行
        // if remote_partitions.is_empty() {
        //     log::debug!("[QueryCoordinator] All partitions local, using standalone execution");
        //     return self.execute_standalone(sql).await;
        // }

        // // 使用 DistributedExecutor 执行分布式查询
        // let distributed_executor = super::DistributedExecutor::new(
        //     self.engine.clone(),
        //     cluster_manager.clone(),
        //     partition_manager.clone(),
        //     self.config.clone(),
        // );

        // distributed_executor.execute_sql(sql).await
    }

    /// 从 SQL 中提取表名
    fn extract_table_name(&self, sql: &str) -> CoreResult<String> {
        let sql_upper = sql.to_uppercase();

        if let Some(from_pos) = sql_upper.find(" FROM ") {
            let after_from = &sql[from_pos + 6..].trim();
            let table_name = after_from
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches(|c: char| !c.is_alphanumeric() && c != '_' && c != '.');

            if !table_name.is_empty() {
                return Ok(table_name.to_string());
            }
        }

        Err(crate::utils::error::CoreError::InvalidParam(format!(
            "Could not extract table name from SQL: {}",
            sql
        )))
    }

    /// 分类 Partition 为本地和远程
    // fn classify_partitions(
    //     &self,
    //     topology: &crate::cluster::TableTopology,
    //     my_node_id: &str,
    // ) -> (Vec<String>, Vec<(String, String)>) {
    //     let mut local = Vec::new();
    //     let mut remote = Vec::new();

    //     for (partition_name, partition_info) in &topology.partitions {
    //         if partition_info.write_node == my_node_id {
    //             local.push(partition_name.clone());
    //         } else {
    //             remote.push((partition_name.clone(), partition_info.write_node.clone()));
    //         }
    //     }

    //     (local, remote)
    // }

    /// 获取配置
    pub fn config(&self) -> &DistributedConfig {
        &self.config
    }

    /// 获取引擎
    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试表名提取逻辑（不需要 Engine）
    #[test]
    fn test_extract_table_name_logic() {
        // 直接测试 SQL 解析逻辑
        let test_cases = vec![
            ("SELECT * FROM users", "users"),
            ("SELECT COUNT(*) FROM taxi_trips WHERE x > 10", "taxi_trips"),
            ("SELECT a, b FROM my_table GROUP BY a", "my_table"),
            ("select * from Events where id = 1", "Events"),
        ];

        for (sql, expected) in test_cases {
            let sql_upper = sql.to_uppercase();
            if let Some(from_pos) = sql_upper.find(" FROM ") {
                let after_from = &sql[from_pos + 6..].trim();
                let table_name = after_from
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .trim_matches(|c: char| !c.is_alphanumeric() && c != '_' && c != '.');
                assert_eq!(table_name, expected, "Failed for SQL: {}", sql);
            } else {
                panic!("Could not find FROM in SQL: {}", sql);
            }
        }
    }

    /// 测试配置默认值
    #[test]
    fn test_config_defaults() {
        let config = DistributedConfig::default();
        assert_eq!(config.query_timeout_ms, 30_000);
    }
}
