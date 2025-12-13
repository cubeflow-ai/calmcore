//! Gossip Protocol Management with Chitchat Integration
//!
//! This module implements the ClusterManager using the Chitchat library for
//! decentralized node discovery, failure detection, and state propagation.
//!
//! ## Key Features
//!
//! - **Node Discovery**: Automatic discovery via Gossip protocol
//! - **Failure Detection**: SWIM-like failure detection built into Chitchat
//! - **State Propagation**: Key-value state propagation across all nodes
//! - **Event Broadcasting**: Cluster events via tokio broadcast channels

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chitchat::transport::UdpTransport;
use chitchat::{
    spawn_chitchat, Chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig,
};
use tokio::sync::Mutex;

use super::ClusterConfig;
use crate::cluster::node_manager::NodeManager;
use crate::cluster::{self, gossip, PartitionManager};
use crate::config::Config;
use crate::utils::error::{CoreError, CoreResult};

use crate::cluster::keys::*;

enum NodeStatus {
    Healthy,
    Suspect,
    Dead,
}

/// Cluster Manager with Chitchat integration
///
/// The central component managing cluster membership and node information.
/// Uses Chitchat for Gossip-based communication and failure detection.
pub struct ClusterManager {
    /// Chitchat handle for Gossip communication
    chitchat_handle: Option<ChitchatHandle>,

    /// Chitchat instance (cloneable Arc for background tasks)
    pub chitchat: Arc<Mutex<Chitchat>>,

    pub partition_manager: Arc<PartitionManager>,

    pub node_manager: Arc<NodeManager>,

    /// Local node ID (UUID, immutable for node lifetime)
    node_id: String,
}

impl ClusterManager {
    /// Create and start cluster manager
    ///
    /// # Requirements
    /// - Requirements 1.2: Join cluster via seed nodes when configured
    pub async fn new(config: &Config) -> CoreResult<Self> {
        let cluster_config = config
            .cluster
            .as_ref()
            .ok_or_else(|| CoreError::ConfigError("Cluster config missing".to_string()))?;

        let host = config.host.clone().unwrap();

        let gossip_listen_addr = format!("{}:{}", host, cluster_config.gossip_port)
            .parse::<SocketAddr>()
            .map_err(|e| CoreError::ConfigError(format!("Invalid gossip listen address: {}", e)))?;

        // nodeid format is millsecods timstamp yyyyMMddHHssmmMMM  since unix epoch + "_"+ host
        let node_id = format!("{}_{}", chrono::Utc::now().format("%Y%m%d%H%M%S%3f"), host);

        log::info!("🚀 [Cluster] Starting node {}", node_id);
        log::info!("📡 [Cluster] gossip Listen address: {}", gossip_listen_addr);
        log::info!(
            "🌐 [Cluster] Running in CLUSTER mode with {} seed nodes",
            cluster_config.seed_nodes.len()
        );

        // Initialize Chitchat
        let chitchat_handle =
            Self::init_chitchat(node_id.clone(), config, gossip_listen_addr).await?;
        let chitchat = chitchat_handle.chitchat().clone();

        let manager = Self {
            chitchat_handle: Some(chitchat_handle),
            chitchat: chitchat.clone(),
            node_id: node_id.clone(),
            node_manager: Arc::new(NodeManager::new(chitchat.clone()).await),
            partition_manager: Arc::new(PartitionManager::new(node_id.clone(), chitchat.clone())),
        };

        manager.set_my_status_preparing().await;

        log::info!("✅ [Cluster] ClusterManager started successfully");

        Ok(manager)
    }

    /// Initialize Chitchat for cluster communication
    ///
    /// # Requirements
    /// - Requirements 1.2: Join cluster via seed nodes
    /// - Requirements 1.3: Receive current cluster membership and topology
    /// - Requirements 1.4: Fail if cannot contact any seed node
    async fn init_chitchat(
        node_id: String,
        config: &ClusterConfig,
        listen_addr: SocketAddr,
    ) -> CoreResult<ChitchatHandle> {
        // Create Chitchat ID
        let chitchat_id = ChitchatId::new(
            node_id,
            0, // generation (increments on restart)
            listen_addr,
        );

        // Configure failure detector
        let failure_detector_config = FailureDetectorConfig {
            phi_threshold: 8.0,
            initial_interval: config.gossip_interval,
            ..Default::default()
        };

        // Create Chitchat config - seed_nodes are strings for DNS resolution
        let chitchat_config = ChitchatConfig {
            chitchat_id,
            cluster_id: config.cluster_id.clone(),
            gossip_interval: config.gossip_interval,
            listen_addr,
            seed_nodes: config.seed_nodes.clone(),
            failure_detector_config,
            marked_for_deletion_grace_period: Duration::from_secs(3600),
            catchup_callback: None,
            extra_liveness_predicate: None,
        };

        log::info!("🔧 [Cluster] Initializing Chitchat with config:");
        log::info!("   - Cluster ID: {}", config.cluster_id);
        log::info!("   - Listen: {}", listen_addr);
        log::info!("   - Seeds: {:?}", config.seed_nodes);

        // Create UDP transport
        let transport = UdpTransport;

        // Spawn Chitchat
        let chitchat_handle = spawn_chitchat(chitchat_config, vec![], &transport)
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to start Chitchat: {}", e)))?;

        // If we have seed nodes, decide whether to wait or start immediately
        let is_seed = config.is_seed_node(listen_addr);

        if is_seed {
            // Seed nodes start immediately without waiting
            log::info!("🌱 [Cluster] Starting as SEED node (will not wait for other seeds)");
            log::info!("🔗 [Cluster] Will accept connections from other nodes...");

            tokio::time::sleep(Duration::from_secs(15)).await; // Give time to start
        } else {
            // Regular nodes must wait for seed nodes
            log::info!("🔗 [Cluster] Starting as WORKER node, connecting to seed nodes...");

            let chitchat = chitchat_handle.chitchat();
            let mut attempts = 0;

            loop {
                attempts += 1;

                // Check if we've discovered any peers
                let guard = chitchat.lock().await;
                let live_nodes = guard.live_nodes().count();
                drop(guard);

                if live_nodes > 1 {
                    log::info!(
                        "✅ [Cluster] Successfully joined cluster! Discovered {} live nodes",
                        live_nodes
                    );
                    break;
                } else {
                    if attempts == 1 {
                        log::warn!("⏳ [Cluster] Waiting for seed nodes to become available...");
                    }
                    log::info!(
                        "🔄 [Cluster] Attempting to connect to seed nodes... (attempt #{})",
                        attempts
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        Ok(chitchat_handle)
    }

    fn partition_key(table_name: &str, partition_name: &str) -> String {
        format!("partition:{}:{}", table_name, partition_name)
    }

    pub async fn put_partition(&self, table_name: &str, partition_name: &str) {
        let key = Self::partition_key(table_name, partition_name);
        let mut guard = self.chitchat.lock().await;
        guard
            .self_node_state()
            .set(key.clone(), self.node_id.clone());
    }

    pub async fn del_partition(&self, table_name: &str, partition_name: &str) {
        let key = Self::partition_key(table_name, partition_name);
        let mut guard = self.chitchat.lock().await;
        guard.self_node_state().delete(&key);
    }

    pub async fn get_partition(
        &self,
        table_name: &str,
        partition_name: &str,
    ) -> Option<(String, String)> {
        let key = Self::partition_key(table_name, partition_name);

        let guard = self.chitchat.lock().await;

        for (_, state) in guard.node_states() {
            if let Some(value) = state.get(&key) {
                return Some(value.to_string());
            }
        }
        None
    }

    /// Get local node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub async fn set_my_status_preparing(&self) {
        self.gossip_set(KEY_NODE_STATUS, VALUE_NODE_STATUS_PREPARING)
            .await;
        log::info!(
            "🔄 [Cluster] Updated node status to '{}'",
            VALUE_NODE_STATUS_PREPARING
        );
    }

    pub async fn set_my_status_ready(&self) {
        self.gossip_set(KEY_NODE_STATUS, VALUE_NODE_STATUS_READY)
            .await;
        log::info!(
            "✅ [Cluster] Updated node status to '{}'",
            VALUE_NODE_STATUS_READY
        );
    }

    pub async fn set_my_status_decommissioning(&self) {
        self.gossip_set(KEY_NODE_STATUS, VALUE_NODE_STATUS_DECOMMISSIONING)
            .await;
        log::info!(
            "⚠️  [Cluster] Updated node status to '{}'",
            VALUE_NODE_STATUS_DECOMMISSIONING
        );
    }

    pub async fn set_internal_addr(&self, addr: &str) {
        self.chitchat
            .lock()
            .await
            .self_node_state()
            .set(INTERNAL_ADDR_KEY, addr);
    }

    // ========================================================================
    // Gossip KV Operations (Requirements 4.3)
    // ========================================================================

    /// Set key-value in Gossip state
    ///
    /// # Requirements
    /// - Requirements 4.3: Propagate topology changes via Gossip
    pub async fn gossip_set(&self, key: &str, value: &str) {
        let mut guard = self.chitchat.lock().await;
        guard.self_node_state().set(key, value);
        log::debug!("📝 [Gossip] Set key: {} = {}", key, value);
    }

    /// Get key-value from Gossip state
    pub async fn gossip_get(&self, key: &str) -> Option<String> {
        let mut guard = self.chitchat.lock().await;

        // First check our own state
        if let Some(value) = guard.self_node_state().get(key) {
            return Some(value.to_string());
        }

        // Then check other nodes' states
        for (_chitchat_id, node_state) in guard.node_states() {
            if let Some(value) = node_state.get(key) {
                return Some(value.to_string());
            }
        }
        None
    }

    /// Set key-value (legacy API)
    pub async fn set_key_value(&self, key: String, value: String) -> CoreResult<()> {
        self.gossip_set(&key, &value).await;
        Ok(())
    }

    /// Get key-value (legacy API)
    pub async fn get_key_value(&self, key: &str) -> Option<String> {
        self.gossip_get(key).await
    }

    /// Get all keys from Gossip state
    pub async fn get_all_keys(&self) -> HashMap<String, String> {
        let mut result = HashMap::new();
        let mut guard = self.chitchat.lock().await;

        // Collect from all node states
        for (_chitchat_id, node_state) in guard.node_states() {
            for (key, value) in node_state.key_values() {
                result.insert(key.to_string(), value.to_string());
            }
        }

        // Also include our own state
        for (key, value) in guard.self_node_state().key_values() {
            result.insert(key.to_string(), value.to_string());
        }

        result
    }
    /// Delete key from Gossip state
    pub async fn delete_key(&self, key: &str) -> CoreResult<()> {
        let mut guard = self.chitchat.lock().await;
        guard.self_node_state().delete(key);
        log::debug!("🗑️  [Gossip] Deleted key: {}", key);
        Ok(())
    }

    // ========================================================================
    // Node Metrics Broadcasting (Requirements 2.1, 2.4)
    // ========================================================================

    /// Update my node metrics and broadcast via Gossip
    pub async fn update_my_metrics(
        &self,
        partition_count: usize,
        load: f64,
        memory_usage_bytes: u64,
    ) {
        // Broadcast via Gossip
        self.gossip_set(KEY_PARTITION_COUNT, &partition_count.to_string())
            .await;
        self.gossip_set(KEY_LOAD, &load.to_string()).await;
        self.gossip_set(KEY_MEMORY, &memory_usage_bytes.to_string())
            .await;

        log::debug!(
            "📊 [Cluster] Updated metrics: partitions={}, load={:.2}, memory={}",
            partition_count,
            load,
            memory_usage_bytes
        );
    }
}
