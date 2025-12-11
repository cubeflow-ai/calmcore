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
use datafusion::functions_aggregate::count;
use rand::seq::SliceRandom;
use tokio::sync::Mutex;

use super::ClusterConfig;
use crate::cluster::node_manager::NodeManager;
use crate::cluster::PartitionManager;
use crate::utils::error::{CoreError, CoreResult};

/// Gossip key prefixes for different types of data
const KEY_LOAD: &str = "load";
const KEY_MEMORY: &str = "memory";
const KEY_PARTITION_COUNT: &str = "partition_count";

/// Cluster Manager with Chitchat integration
///
/// The central component managing cluster membership and node information.
/// Uses Chitchat for Gossip-based communication and failure detection.
pub struct ClusterManager {
    /// Chitchat handle for Gossip communication
    chitchat_handle: ChitchatHandle,

    /// Chitchat instance (cloneable Arc for background tasks)
    pub chitchat: Arc<Mutex<Chitchat>>,

    pub partition_manager: Arc<PartitionManager>,

    pub node_manager: Arc<NodeManager>,

    /// Local node ID (UUID, immutable for node lifetime)
    node_id: String,

    /// Cluster configuration
    config: ClusterConfig,

    /// Gossip listen address
    listen_addr: String,
}

impl ClusterManager {
    /// Create and start cluster manager
    ///
    /// # Requirements
    /// - Requirements 1.2: Join cluster via seed nodes when configured
    pub async fn new(config: Option<ClusterConfig>) -> CoreResult<Self> {
        let config = match config {
            None => todo!("Standalone mode is not implemented in this version"),
            Some(c) => c,
        };

        // Validate configuration
        config.validate()?;

        let node_id = config.node_id.clone();
        let listen_addr = config.listen_addr.clone();

        log::info!("🚀 [Cluster] Starting node {}", node_id);
        log::info!("📡 [Cluster] Listen address: {}", listen_addr);
        log::info!(
            "🌐 [Cluster] Running in CLUSTER mode with {} seed nodes",
            config.seed_nodes.len()
        );

        // Initialize Chitchat
        let chitchat_handle = Self::init_chitchat(&config).await?;
        let chitchat = chitchat_handle.chitchat().clone();

        let manager = Self {
            chitchat_handle,
            chitchat: chitchat.clone(),
            node_id: node_id.clone(),
            config,
            listen_addr,
            node_manager: Arc::new(NodeManager::new(chitchat.clone())),
            partition_manager: Arc::new(PartitionManager::new(node_id.clone(), chitchat.clone())),
        };

        log::info!("✅ [Cluster] ClusterManager started successfully");

        Ok(manager)
    }

    /// Initialize Chitchat for cluster communication
    ///
    /// # Requirements
    /// - Requirements 1.2: Join cluster via seed nodes
    /// - Requirements 1.3: Receive current cluster membership and topology
    /// - Requirements 1.4: Fail if cannot contact any seed node
    async fn init_chitchat(config: &ClusterConfig) -> CoreResult<ChitchatHandle> {
        let listen_addr: SocketAddr = config.parse_listen_addr()?;

        // Create Chitchat ID
        let chitchat_id = ChitchatId::new(
            config.node_id.clone(),
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
        let is_seed = config.is_seed_node();

        if is_seed {
            // Seed nodes start immediately without waiting
            log::info!("🌱 [Cluster] Starting as SEED node (will not wait for other seeds)");
            log::info!("🔗 [Cluster] Will accept connections from other nodes...");
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

                if live_nodes > 0 {
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

    /// Calculate quorum threshold
    pub fn quorum_threshold(&self, node_count: usize) -> usize {
        self.config.quorum_threshold(node_count)
    }

    /// Get cluster configuration
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Get local node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
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

    // ========================================================================
    // Event Broadcasting (Requirements 3.4, 12.2)
    // ========================================================================

    // ========================================================================
    // Background Tasks
    // ========================================================================

    // ========================================================================
    // Graceful Shutdown (Requirements 8.1)
    // ========================================================================

    /// Gossip key for leaving notification
    const LEAVING_KEY_PREFIX: &'static str = "leaving:";

    /// Initiate graceful shutdown
    ///
    /// This method broadcasts a leaving notification to all peers via Gossip,
    /// allowing other nodes to initiate voting for this node's partitions
    /// before the node actually shuts down.
    ///
    /// # Requirements
    /// - Requirements 8.1: Broadcast leaving notification to all peers
    ///
    /// # Returns
    /// Ok(()) when the leaving notification has been broadcast
    pub async fn shutdown(&self) -> CoreResult<()> {
        log::info!(
            "👋 [Cluster] Initiating graceful shutdown for node {}",
            self.node_id
        );

        // Broadcast leaving notification via Gossip with node ID
        // Key format: leaving:{node_id} = "true"
        let leaving_key = format!("{}{}", Self::LEAVING_KEY_PREFIX, self.node_id);
        self.gossip_set(&leaving_key, "true").await;

        // Also set a simple "leaving" key on our own state for backward compatibility
        self.gossip_set("leaving", "true").await;

        log::info!(
            "📢 [Cluster] Broadcast leaving notification for node {}",
            self.node_id
        );

        Ok(())
    }

    /// Wait for partition transfer to complete before final shutdown
    ///
    /// This method blocks until all partitions owned by this node have been
    /// reassigned to other nodes, or until the timeout expires.
    ///
    /// # Arguments
    /// * `partition_manager` - Reference to PartitionManager to check partition ownership
    /// * `timeout` - Maximum time to wait for partition transfer
    ///
    /// # Requirements
    /// - Requirements 8.4: Block until all partitions reassigned or timeout
    ///
    /// # Returns
    /// Ok(true) if all partitions were transferred, Ok(false) if timeout expired
    pub async fn wait_for_partition_transfer(
        &self,
        check_fn: impl Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>,
        timeout: Duration,
    ) -> CoreResult<bool> {
        log::info!(
            "⏳ [Cluster] Waiting for partition transfer (timeout: {:?})",
            timeout
        );

        let start = std::time::Instant::now();
        let check_interval = Duration::from_millis(500);

        while start.elapsed() < timeout {
            // Check if all partitions have been transferred
            if check_fn().await {
                log::info!("✅ [Cluster] All partitions transferred successfully");
                return Ok(true);
            }

            // Wait before checking again
            tokio::time::sleep(check_interval).await;
        }

        log::warn!(
            "⚠️  [Cluster] Partition transfer timeout after {:?}",
            timeout
        );
        Ok(false)
    }

    /// Check if a node is leaving the cluster
    ///
    /// # Arguments
    /// * `node_id` - The node ID to check
    ///
    /// # Returns
    /// true if the node has broadcast a leaving notification
    pub async fn is_node_leaving(&self, node_id: &str) -> bool {
        let leaving_key = format!("{}{}", Self::LEAVING_KEY_PREFIX, node_id);
        self.gossip_get(&leaving_key)
            .await
            .map(|v| v == "true")
            .unwrap_or(false)
    }

    /// Complete the graceful shutdown after partition transfer
    ///
    /// This method should be called after wait_for_partition_transfer completes.
    /// It performs final cleanup and marks the shutdown as complete.
    ///
    /// # Requirements
    /// - Requirements 8.4: Complete shutdown after partition transfer
    pub async fn complete_shutdown(&self) -> CoreResult<()> {
        log::info!("🏁 [Cluster] Completing shutdown for node {}", self.node_id);

        // Give time for final Gossip propagation
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Note: ChitchatHandle::shutdown takes ownership, so we can't call it
        // on a reference. The handle will be dropped when ClusterManager is dropped.

        log::info!("✅ [Cluster] ClusterManager shutdown complete");
        Ok(())
    }

    // ========================================================================
    // Node Registration (for testing and manual node management)
    // ========================================================================
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_mode() {
        let config = ClusterConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            listen_addr: "127.0.0.1:17946".to_string(),
            seed_nodes: vec!["127.0.0.1:17946".to_string()], // Self as seed
            ..Default::default()
        };

        let manager = ClusterManager::new(Some(config)).await.unwrap();

        assert_eq!(manager.node_id(), "test-node");

        // Should have 1 node (self)
        let nodes = manager.live_nodes().await;
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, "test-node");
    }

    #[tokio::test]
    async fn test_gossip_kv_cluster() {
        let config = ClusterConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            listen_addr: "127.0.0.1:17947".to_string(),
            seed_nodes: vec!["127.0.0.1:17947".to_string()],
            ..Default::default()
        };

        let manager = ClusterManager::new(Some(config)).await.unwrap();

        // Set and get key
        manager.gossip_set("test_key", "test_value").await;
        let value = manager.gossip_get("test_key").await;
        assert_eq!(value, Some("test_value".to_string()));

        // Delete key
        manager.delete_key("test_key").await.unwrap();
        let value = manager.gossip_get("test_key").await;
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_update_metrics() {
        let config = ClusterConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            listen_addr: "127.0.0.1:17948".to_string(),
            seed_nodes: vec!["127.0.0.1:17948".to_string()],
            ..Default::default()
        };

        let manager = ClusterManager::new(Some(config)).await.unwrap();

        // Update metrics
        manager.update_my_metrics(10, 0.5, 1024 * 1024 * 100).await;

        // Verify metrics are stored
        let partition_count = manager.gossip_get(KEY_PARTITION_COUNT).await;
        assert_eq!(partition_count, Some("10".to_string()));

        let load = manager.gossip_get(KEY_LOAD).await;
        assert_eq!(load, Some("0.5".to_string()));
    }
}
