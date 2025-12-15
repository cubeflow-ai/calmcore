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

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chitchat::transport::UdpTransport;
use chitchat::{
    spawn_chitchat, Chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig,
};
use tokio::sync::Mutex;

use crate::cluster::node_manager::NodeManager;
use crate::cluster::PartitionManager;
use crate::config::cluster::ClusterSettings;
use crate::config::Config;
use crate::utils::error::{CoreError, CoreResult};

use crate::cluster::keys::*;

pub struct PartitionRouter {
    pub table_name: String,
    pub partition_name: String,
    pub owner_node_id: String,
}

pub struct GossipManager {
    chitchat_handle: ChitchatHandle,
}

impl GossipManager {
    /// Initialize Chitchat for cluster communication
    ///
    /// # Requirements
    /// - Requirements 1.2: Join cluster via seed nodes
    /// - Requirements 1.3: Receive current cluster membership and topology
    /// - Requirements 1.4: Fail if cannot contact any seed node
    pub async fn new(
        node_id: String,
        config: &ClusterSettings,
        listen_addr: SocketAddr,
    ) -> CoreResult<Self> {
        // Create Chitchat ID
        let chitchat_id = ChitchatId::new(
            node_id,
            0, // generation (increments on restart)
            listen_addr,
        );

        // Configure failure detector
        let failure_detector_config = FailureDetectorConfig {
            phi_threshold: 8.0,
            initial_interval: Duration::from_millis(config.gossip_interval_ms),
            ..Default::default()
        };

        // Create Chitchat config - seed_nodes are strings for DNS resolution
        let chitchat_config = ChitchatConfig {
            chitchat_id,
            cluster_id: config.cluster_id.clone(),
            gossip_interval: Duration::from_millis(config.gossip_interval_ms),
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

        Ok(GossipManager { chitchat_handle })
    }

    pub fn chitchat(&self) -> Arc<Mutex<Chitchat>> {
        self.chitchat_handle.chitchat().clone()
    }

    /// Get key-value from Gossip state
    pub async fn find_one(&self, key: &str) -> Option<String> {
        let chitchat = self.chitchat();
        let guard = chitchat.lock().await;

        // Then check other nodes' states
        for (_chitchat_id, node_state) in guard.node_states() {
            if let Some(value) = node_state.get(key) {
                return Some(value.to_string());
            }
        }
        None
    }

    pub async fn find_all(&self, key: &str) -> Vec<String> {
        let chitchat = self.chitchat();
        let guard = chitchat.lock().await;
        let mut values = Vec::new();

        // Check all nodes' states
        for (_chitchat_id, node_state) in guard.node_states() {
            if let Some(value) = node_state.get(key) {
                values.push(value.to_string());
            }
        }
        values
    }

    /// Set key-value in Gossip state
    ///
    /// # Requirements
    /// - Requirements 4.3: Propagate topology changes via Gossip
    pub async fn set(&self, key: &str, value: &str) {
        let chitchat = self.chitchat();
        let mut guard = chitchat.lock().await;
        guard.self_node_state().set(key, value);
        log::debug!("📝 [Gossip] Set key: {} = {}", key, value);
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let chitchat = self.chitchat();
        let guard = chitchat.lock().await;

        // First check local state
        if let Some(value) = guard.self_node_state().get(key) {
            return Some(value.to_string());
        }
        None
    }

    /// Delete key from Gossip state
    pub async fn delete(&self, key: &str) -> CoreResult<()> {
        let chitchat = self.chitchat();
        let mut guard = chitchat.lock().await;
        guard.self_node_state().delete(key);
        log::debug!("🗑️  [Gossip] Deleted key: {}", key);
        Ok(())
    }
}
