use std::{
    collections::HashMap,
    hash::Hash,
    sync::{atomic::AtomicBool, Arc},
};

use chitchat::{Chitchat, ChitchatId};
use itertools::Itertools;
use rand::seq::SliceRandom;
use tokio::sync::{broadcast, Mutex, RwLock};

use crate::{
    cluster::{self, ClusterEvent},
    utils::error::{CoreError, CoreResult},
};

pub const INTERNAL_ADDR_KEY: &str = "internal_addr";
pub const CENTER_NODE_KEY: &str = "coord_node";

pub struct NodeManager {
    node_id: String,
    /// Chitchat instance for cluster membership
    chitchat: Arc<Mutex<Chitchat>>,
    /// 本地缓存的中央节点
    coord_node: Arc<RwLock<Option<NodeInfo>>>,
    /// node 到 ChitchatId 的缓存
    node_chache: RwLock<HashMap<String, NodeInfo>>,

    is_coord: AtomicBool,
}

#[derive(Clone)]
pub struct NodeInfo {
    pub internal_addr: String,
    pub chitchat_id: ChitchatId,
}

impl NodeManager {
    pub async fn new(chitchat: Arc<Mutex<Chitchat>>) -> Self {
        let node_id = chitchat.lock().await.self_chitchat_id().node_id.to_string();
        Self {
            node_id,
            chitchat,
            coord_node: Arc::new(RwLock::new(None)),
            node_chache: RwLock::new(HashMap::new()),
            is_coord: AtomicBool::new(false),
        }
    }

    pub fn is_coord(&self) -> bool {
        self.is_coord.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn reset_coord_node(&self) -> CoreResult<()> {
        let mut coord_node = self.coord_node.write().await;
        *coord_node = None;
        Ok(())
    }

    pub async fn set_coord_node(&self, node_id: &str) -> CoreResult<()> {
        let mut coord_node = self.coord_node.write().await;

        log::info!(
            "Setting coord node from[{:?}] to [{}]",
            coord_node.as_ref().map(|n| n.internal_addr.clone()),
            node_id
        );

        let node = self.find_node(node_id).await.ok_or_else(|| {
            CoreError::Internal(format!(
                "set_coord_node failed: node [{}] not found",
                node_id
            ))
        })?;

        self.chitchat
            .lock()
            .await
            .self_node_state()
            .set(CENTER_NODE_KEY, node_id);

        *coord_node = Some(node);
        Ok(())
    }

    /// 执行一轮选举
    pub async fn run_election(&self) -> Result<String, Vec<String>> {
        // 1. 找到 node_id 最小的结点，同时统计投票
        let mut min_node: Option<ChitchatId> = None;
        let mut coord_votes: HashMap<String, usize> = HashMap::new();

        for (node_id, node_state) in self.chitchat.lock().await.node_states() {
            // 找最小的 node_id
            if min_node.is_none() || node_id.node_id < min_node.as_ref().unwrap().node_id {
                min_node = Some(node_id.clone());
            }

            // 统计每个节点投票给谁
            if let Some(voted_coord) = node_state.get(CENTER_NODE_KEY) {
                *coord_votes.entry(voted_coord.to_string()).or_insert(0) += 1;
            }
        }

        // 2. 如果没有找到任何节点，返回错误
        if min_node.is_none() {
            return Err(vec![]);
        }

        let min_node = min_node.unwrap();
        let min_node_id = min_node.node_id.clone();

        // 3. 检查投票一致性：如果超过一半投给了同一个节点，则选举成功

        if coord_votes.len() == 1 {
            let cluster_coord = coord_votes.keys().next().unwrap().as_str();
            if cluster_coord == min_node_id {
                return Ok(cluster_coord.to_string());
            }
            return Ok(min_node_id);
        }

        // 4. 投票不一致，返回所有候选节点

        // 投票给自己认为最小的节点
        self.chitchat
            .lock()
            .await
            .self_node_state()
            .set(CENTER_NODE_KEY, &min_node_id);

        let mut candidates: Vec<String> = coord_votes.keys().cloned().collect();
        if !candidates.contains(&min_node_id) {
            candidates.push(min_node_id);
        }
        candidates.sort();

        log::warn!(
            "⚠️ Election split: candidates={:?}, votes={:?}",
            candidates,
            coord_votes
        );

        Err(candidates)
    }

    /// 获取当前的中央节点
    ///
    /// 返回已选举出的中央节点，如果还未选举出则返回 None
    pub async fn get_coord_node_addr(&self) -> Option<String> {
        self.coord_node
            .read()
            .await
            .as_ref()
            .map(|n| n.internal_addr.to_string())
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

    async fn find_node(&self, node_id: &str) -> Option<NodeInfo> {
        let node = self.node_chache.read().await.get(node_id).cloned();
        if node.is_some() {
            return node;
        }

        let guard = self.chitchat.lock().await;

        let node = guard
            .live_nodes()
            .find_or_first(|n| n.node_id == node_id)
            .cloned()?;

        let internal_addr = guard
            .node_state(&node)
            .and_then(|n| n.get(INTERNAL_ADDR_KEY).map(ToString::to_string))?;

        let node_info = NodeInfo {
            internal_addr,
            chitchat_id: node.clone(),
        };

        self.node_chache
            .write()
            .await
            .insert(node_id.to_string(), node_info.clone());
        Some(node_info)
    }

    pub async fn get_node_id(&self, node_id: &str) -> Option<ChitchatId> {
        let node = self
            .node_chache
            .read()
            .await
            .get(node_id)
            .map(|n| n.chitchat_id.clone());

        if node.is_some() {
            return node;
        }
        let node = self.find_node(node_id).await?;
        Some(node.chitchat_id)
    }

    pub async fn get_node_internal_addr(&self, node_id: &str) -> Option<String> {
        let addr = self
            .node_chache
            .read()
            .await
            .get(node_id)
            .map(|n| n.internal_addr.to_string());

        if addr.is_some() {
            return addr;
        }
        let node = self.find_node(node_id).await?;
        Some(node.internal_addr)
    }

    /// Get node count (for quorum calculation)
    pub async fn node_count(&self) -> usize {
        self.chitchat.lock().await.live_nodes().count()
    }
}
