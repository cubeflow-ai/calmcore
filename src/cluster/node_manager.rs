use std::sync::Arc;

use chitchat::{Chitchat, ChitchatId};
use itertools::Itertools;
use rand::seq::SliceRandom;
use tokio::sync::Mutex;

use crate::utils::error::CoreResult;

pub const INTERNAL_ADDR_KEY: &str = "internal_addr";
pub const CENTER_NODE_KEY: &str = "coord_node";
pub const NODE_START_TIME_KEY: &str = "node_start_time"; // 节点启动时间戳
pub const CLUSTER_NODE_COUNT: &str = "cluster_node_count"; // 集群版本（看到的节点数量）

pub struct NodeManager {
    /// Chitchat instance for cluster membership
    chitchat: Arc<Mutex<Chitchat>>,
    /// 本地缓存的中央节点
    coord_node: Arc<Mutex<Option<ChitchatId>>>,
    /// 本节点启动时间
    start_time: u64,
}

impl NodeManager {
    pub fn new(chitchat: Arc<Mutex<Chitchat>>) -> Self {
        let start_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            chitchat,
            coord_node: Arc::new(Mutex::new(None)),
            start_time,
        }
    }

    /// 启动中央节点选举任务
    ///
    /// 这个任务会：
    /// 1. 定期检查集群中所有节点的启动时间
    /// 2. 选出启动时间最早的节点作为候选
    /// 3. 如果超过半数节点认同，则确认为中央节点
    /// 4. 将自己的投票写入 CENTER_NODE_KEY
    pub async fn start_center_node_election(self: Arc<Self>) {
        // 首先注册自己的启动时间
        {
            let mut chitchat = self.chitchat.lock().await;
            chitchat
                .self_node_state()
                .set(NODE_START_TIME_KEY, &self.start_time.to_string());
        }

        let interval = std::time::Duration::from_secs(5); // 每5秒检查一次

        let center_node = self.center_node.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;

                let mut guard = self.chitchat.lock().await;
                let count = guard.live_nodes().count();
                guard
                    .self_node_state()
                    .set(CLUSTER_NODE_COUNT, count.to_string());
                drop(guard); // 释放锁以避免死锁
                if center_node.lock().await.is_none() {
                    if let Err(e) = self.run_election().await {
                        log::error!("Center node election error: {}", e);
                    }
                }
            }
        });
    }

    /// 执行一轮选举
    async fn run_election(&self) -> CoreResult<()> {
        let chitchat_guard = self.chitchat.lock().await;

        // 1. 收集所有活跃节点的启动时间
        let mut node_start_times: Vec<(ChitchatId, u64)> = Vec::new();

        for (node_id, node_state) in chitchat_guard.node_states() {
            if let Some(start_time_str) = node_state.get(NODE_START_TIME_KEY) {
                if let Ok(start_time) = start_time_str.parse::<u64>() {
                    node_start_times.push((node_id.clone(), start_time));
                }
            }
        }

        if node_start_times.is_empty() {
            return Ok(());
        }

        let total_nodes = node_start_times.len();

        // 更新自己的集群版本（看到的节点数量）
        let my_version = chitchat_guard.self_node_state().get(CLUSTER_VERSION_KEY);
        if my_version.as_deref() != Some(&total_nodes.to_string()) {
            drop(chitchat_guard);
            let mut chitchat_mut = self.chitchat.lock().await;
            chitchat_mut
                .self_node_state()
                .set(CLUSTER_VERSION_KEY, &total_nodes.to_string());

            log::debug!("📊 Updated cluster version: {}", total_nodes);
            drop(chitchat_mut);

            // 重新获取锁继续选举
            let chitchat_guard = self.chitchat.lock().await;
            return self
                .continue_election(chitchat_guard, node_start_times, total_nodes)
                .await;
        }

        self.continue_election(chitchat_guard, node_start_times, total_nodes)
            .await
    }

    /// 继续执行选举流程（分离出来避免多次锁）
    async fn continue_election(
        &self,
        chitchat_guard: tokio::sync::MutexGuard<'_, Chitchat>,
        mut node_start_times: Vec<(ChitchatId, u64)>,
        total_nodes: usize,
    ) -> CoreResult<()> {
        // 2. 找出启动时间最早的节点（存活时间最久）
        node_start_times.sort_by_key(|(_, time)| *time);
        let oldest_node = &node_start_times[0].0;

        // 3. 统计有多少节点投票给这个最老的节点
        let mut votes = 0;

        for (_node_id, node_state) in chitchat_guard.node_states() {
            if let Some(voted_center) = node_state.get(CENTER_NODE_KEY) {
                if voted_center == oldest_node.node_id {
                    votes += 1;
                }
            }
        }

        // 4. 检查是否达到多数（超过半数）
        let quorum = total_nodes / 2 + 1;
        let is_elected = votes >= quorum;

        // 5. 更新自己的投票
        let my_current_vote = chitchat_guard.self_node_state().get(CENTER_NODE_KEY);

        if my_current_vote.as_deref() != Some(&oldest_node.node_id) {
            // 投票给最老的节点
            drop(chitchat_guard); // 释放锁以避免死锁
            let mut chitchat_mut = self.chitchat.lock().await;
            chitchat_mut
                .self_node_state()
                .set(CENTER_NODE_KEY, &oldest_node.node_id);

            log::info!(
                "🗳️  Voting for center node: {} (votes: {}/{}, quorum: {})",
                oldest_node.node_id,
                votes + 1, // 加上自己的投票
                total_nodes,
                quorum
            );
        }

        // 6. 如果达到多数，更新本地缓存
        if is_elected {
            let mut center = self.center_node.lock().await;
            if center.as_ref().map(|n| &n.node_id) != Some(&oldest_node.node_id) {
                log::info!(
                    "✅ Center node elected: {} (votes: {}/{}, start_time: {})",
                    oldest_node.node_id,
                    votes,
                    total_nodes,
                    node_start_times[0].1
                );
                *center = Some(oldest_node.clone());
            }
        }

        Ok(())
    }

    /// 获取当前的中央节点
    ///
    /// 返回已选举出的中央节点，如果还未选举出则返回 None
    pub async fn get_center_node(&self) -> Option<ChitchatId> {
        self.center_node.lock().await.clone()
    }

    /// 判断本节点是否是中央节点
    pub async fn is_self_center_node(&self) -> bool {
        let chitchat = self.chitchat.lock().await;
        let my_node_id = &chitchat.self_chitchat_id().node_id;

        match self.center_node.lock().await.as_ref() {
            Some(center) => &center.node_id == my_node_id,
            None => false,
        }
    }

    /// 检查本节点看到的集群视图是否完整
    ///
    /// 通过对比自己看到的节点数量和其他节点报告的数量来判断
    /// 返回 (is_complete, my_version, max_version)
    pub async fn check_cluster_view_completeness(&self) -> (bool, usize, usize) {
        let chitchat = self.chitchat.lock().await;

        // 获取自己看到的节点数量
        let my_version = chitchat.node_states().count();

        // 找出所有节点报告的最大版本号
        let mut max_version = my_version;

        for (_node_id, node_state) in chitchat.node_states() {
            if let Some(version_str) = node_state.get(CLUSTER_VERSION_KEY) {
                if let Ok(version) = version_str.parse::<usize>() {
                    max_version = max_version.max(version);
                }
            }
        }

        let is_complete = my_version >= max_version;

        if !is_complete {
            log::warn!(
                "⚠️  Incomplete cluster view: my_version={}, max_version={}",
                my_version,
                max_version
            );
        }

        (is_complete, my_version, max_version)
    }

    /// 获取本节点的集群版本（看到的节点数量）
    pub async fn get_my_cluster_version(&self) -> usize {
        self.chitchat.lock().await.node_states().count()
    }

    /// 获取集群中报告的最大版本号
    pub async fn get_max_cluster_version(&self) -> usize {
        let chitchat = self.chitchat.lock().await;
        let mut max_version = 0;

        for (_node_id, node_state) in chitchat.node_states() {
            if let Some(version_str) = node_state.get(CLUSTER_VERSION_KEY) {
                if let Ok(version) = version_str.parse::<usize>() {
                    max_version = max_version.max(version);
                }
            }
        }

        max_version
    }

    /// Get all live nodes
    ///
    /// # Requirements
    /// - Requirements 2.3: Return current metrics for all known nodes
    pub async fn live_nodes(&self) -> Vec<ChitchatId> {
        self.chitchat.lock().await.live_nodes().cloned().collect()
    }

    /// Get all live nodes
    pub async fn idle_nodes(&self) -> Vec<ChitchatId> {
        //TODO: implement idle node selection logic
        let mut nodes = self.live_nodes().await;
        nodes.shuffle(&mut rand::rng());
        nodes
    }

    pub async fn get_node(&self, node_id: &str) -> Option<ChitchatId> {
        self.chitchat
            .lock()
            .await
            .live_nodes()
            .find_or_first(|n| n.node_id == node_id)
            .cloned()
    }

    pub async fn node_internal_addr(&self, node: &ChitchatId) -> CoreResult<String> {
        self.chitchat
            .lock()
            .await
            .node_state(&node)
            .and_then(|n| n.get(INTERNAL_ADDR_KEY).map(ToString::to_string))
            .ok_or_else(|| {
                crate::utils::error::CoreError::Internal(format!(
                    "Internal address not found for node {}",
                    node.node_id
                ))
            })
    }

    /// Get node count (for quorum calculation)
    pub async fn node_count(&self) -> usize {
        self.chitchat.lock().await.live_nodes().count()
    }
}
