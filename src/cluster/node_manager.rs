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
use cluster::keys::*;

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
    pub async fn idle_node(&self) -> Vec<ChitchatId> {
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
