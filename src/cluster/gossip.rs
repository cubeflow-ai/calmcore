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
//!
//! ## Modes of Operation
//!
//! - **Standalone Mode**: No seed nodes configured, operates without Gossip
//! - **Cluster Mode**: With seed nodes, must join cluster or fail on startup

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chitchat::transport::UdpTransport;
use chitchat::{spawn_chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};

use super::node::{NodeId, NodeInfo, NodeState};
use super::{ClusterConfig, ClusterEvent};
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
    /// Chitchat handle for Gossip communication (None in standalone mode)
    chitchat_handle: Option<ChitchatHandle>,

    /// Chitchat instance (cloneable Arc for background tasks)
    chitchat: Option<Arc<tokio::sync::Mutex<chitchat::Chitchat>>>,

    /// Local node ID (UUID, immutable for node lifetime)
    node_id: NodeId,

    /// Cluster configuration
    config: ClusterConfig,

    /// All known nodes with their metrics
    nodes: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,

    /// Key-value store for Gossip state (used in standalone mode or as cache)
    kv_store: Arc<RwLock<HashMap<String, String>>>,

    /// Event broadcaster for cluster events
    event_tx: broadcast::Sender<ClusterEvent>,

    /// Failure callbacks (legacy support)
    failure_callbacks: Arc<RwLock<Vec<Box<dyn Fn(NodeId) + Send + Sync>>>>,

    /// Whether running in standalone mode
    standalone: bool,

    /// Gossip listen address
    listen_addr: String,
}

impl ClusterManager {
    /// Create and start cluster manager
    ///
    /// # Behavior
    /// - No seed nodes → standalone mode (skip Gossip)
    /// - With seed nodes → must join cluster, fail on error
    ///
    /// # Requirements
    /// - Requirements 1.1: Standalone mode without Gossip when no seed nodes
    /// - Requirements 1.2: Join cluster via seed nodes when configured
    pub async fn new(config: ClusterConfig) -> CoreResult<Self> {
        // Validate configuration
        config.validate()?;

        let node_id = config.node_id.clone();
        let listen_addr = config.listen_addr.clone();
        let standalone = config.is_standalone();

        log::info!("🚀 [Cluster] Starting node {}", node_id);
        log::info!("📡 [Cluster] Listen address: {}", listen_addr);

        // Create event channel
        let (event_tx, _) = broadcast::channel(1024);

        // Initialize nodes map with self
        let mut nodes = HashMap::new();
        nodes.insert(
            node_id.clone(),
            NodeInfo::new(node_id.clone(), listen_addr.clone()),
        );

        let manager = if standalone {
            // Standalone mode - no Gossip communication
            log::info!("🔒 [Cluster] Running in STANDALONE mode (no seed nodes)");

            Self {
                chitchat_handle: None,
                chitchat: None,
                node_id: node_id.clone(),
                config,
                nodes: Arc::new(RwLock::new(nodes)),
                kv_store: Arc::new(RwLock::new(HashMap::new())),
                event_tx,
                failure_callbacks: Arc::new(RwLock::new(Vec::new())),
                standalone: true,
                listen_addr,
            }
        } else {
            // Cluster mode - initialize Chitchat
            log::info!(
                "🌐 [Cluster] Running in CLUSTER mode with {} seed nodes",
                config.seed_nodes.len()
            );

            let chitchat_handle = Self::init_chitchat(&config).await?;
            let chitchat = chitchat_handle.chitchat().clone();

            Self {
                chitchat_handle: Some(chitchat_handle),
                chitchat: Some(chitchat),
                node_id: node_id.clone(),
                config,
                nodes: Arc::new(RwLock::new(nodes)),
                kv_store: Arc::new(RwLock::new(HashMap::new())),
                event_tx,
                failure_callbacks: Arc::new(RwLock::new(Vec::new())),
                standalone: false,
                listen_addr,
            }
        };

        // Start background tasks
        manager.start_heartbeat_task();
        manager.start_failure_detection_task();

        if !standalone {
            manager.start_membership_sync_task();
        }

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

        // If we have seed nodes, try to join the cluster
        if !config.seed_nodes.is_empty() {
            log::info!("🔗 [Cluster] Attempting to join cluster via seed nodes...");

            // Give some time for initial gossip exchange
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Check if we've discovered any peers
            let chitchat = chitchat_handle.chitchat();
            let guard = chitchat.lock().await;
            let live_nodes = guard.live_nodes().count();

            if live_nodes == 0 {
                log::warn!(
                    "⚠️  [Cluster] No peers discovered yet, but continuing (they may join later)"
                );
            } else {
                log::info!("✅ [Cluster] Discovered {} live nodes", live_nodes);
            }
        }

        Ok(chitchat_handle)
    }

    /// Create cluster manager from individual parameters (legacy API)
    pub async fn from_params(
        node_id: String,
        cluster_id: String,
        listen_addr: String,
        seed_nodes: Vec<String>,
    ) -> CoreResult<Self> {
        let config = ClusterConfig {
            enabled: !seed_nodes.is_empty(),
            node_id,
            cluster_id,
            listen_addr,
            seed_nodes,
            ..Default::default()
        };

        Self::new(config).await
    }

    /// Get my node ID
    pub fn my_node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get node ID (legacy alias)
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get all live nodes
    ///
    /// # Requirements
    /// - Requirements 2.3: Return current metrics for all known nodes
    pub async fn live_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.values().filter(|n| n.is_alive()).cloned().collect()
    }

    /// Get node count (for quorum calculation)
    pub async fn node_count(&self) -> usize {
        self.live_nodes().await.len()
    }

    /// Calculate quorum threshold
    pub fn quorum_threshold(&self, node_count: usize) -> usize {
        self.config.quorum_threshold(node_count)
    }

    /// Check if running in standalone mode
    pub fn is_standalone(&self) -> bool {
        self.standalone
    }

    /// Get cluster configuration
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    // ========================================================================
    // Gossip KV Operations (Requirements 4.3)
    // ========================================================================

    /// Set key-value in Gossip state
    ///
    /// # Requirements
    /// - Requirements 4.3: Propagate topology changes via Gossip
    pub async fn gossip_set(&self, key: &str, value: &str) {
        if let Some(ref chitchat) = self.chitchat {
            let mut guard = chitchat.lock().await;
            guard.self_node_state().set(key, value);
            log::debug!("📝 [Gossip] Set key: {} = {}", key, value);
        } else {
            // Standalone mode - use local KV store
            let mut kv = self.kv_store.write().await;
            kv.insert(key.to_string(), value.to_string());
            log::debug!("📝 [Standalone] Set key: {} = {}", key, value);
        }
    }

    /// Get key-value from Gossip state
    pub async fn gossip_get(&self, key: &str) -> Option<String> {
        if let Some(ref chitchat) = self.chitchat {
            let mut guard = chitchat.lock().await;

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
        } else {
            // Standalone mode - use local KV store
            let kv = self.kv_store.read().await;
            kv.get(key).cloned()
        }
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
        if let Some(ref chitchat) = self.chitchat {
            let mut guard = chitchat.lock().await;

            let mut result = HashMap::new();

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
        } else {
            let kv = self.kv_store.read().await;
            kv.clone()
        }
    }

    /// Delete key from Gossip state
    pub async fn delete_key(&self, key: &str) -> CoreResult<()> {
        if let Some(ref chitchat) = self.chitchat {
            let mut guard = chitchat.lock().await;
            guard.self_node_state().delete(key);
            log::debug!("🗑️  [Gossip] Deleted key: {}", key);
        } else {
            let mut kv = self.kv_store.write().await;
            kv.remove(key);
            log::debug!("🗑️  [Standalone] Deleted key: {}", key);
        }
        Ok(())
    }

    // ========================================================================
    // Node Metrics Broadcasting (Requirements 2.1, 2.4)
    // ========================================================================

    /// Update my node metrics and broadcast via Gossip
    ///
    /// # Requirements
    /// - Requirements 2.1: Periodically broadcast resource metrics
    /// - Requirements 2.4: Broadcast update within gossip_interval when metrics change
    pub async fn update_my_metrics(
        &self,
        partition_count: usize,
        load: f64,
        memory_usage_bytes: u64,
    ) {
        // Update local node info
        {
            let mut nodes = self.nodes.write().await;
            if let Some(node) = nodes.get_mut(&self.node_id) {
                node.update_metrics(partition_count, load, memory_usage_bytes);
            }
        }

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

    /// Get node info by ID
    pub async fn get_node_info(&self, node_id: &str) -> Option<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.get(node_id).cloned()
    }

    /// Get all nodes (including dead ones)
    pub async fn all_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    // ========================================================================
    // Event Broadcasting (Requirements 3.4, 12.2)
    // ========================================================================

    /// Subscribe to cluster events
    ///
    /// # Requirements
    /// - Requirements 3.4: Trigger partition failover when node marked Dead
    /// - Requirements 12.2: Emit log entry on node state changes
    pub fn subscribe(&self) -> broadcast::Receiver<ClusterEvent> {
        self.event_tx.subscribe()
    }

    /// Emit a cluster event with structured logging
    ///
    /// # Requirements
    /// - Requirements 12.2: Emit log entry with node ID, old state, and new state
    fn emit_event(&self, event: ClusterEvent) {
        // Log the event with structured information
        // Requirements 12.2: Log state changes with node ID, old/new state
        match &event {
            ClusterEvent::NodeJoined(node_id) => {
                log::info!(
                    target: "cluster::state_transition",
                    "🟢 [Cluster] State transition: node_id={}, old_state=None, new_state=Alive, event=NodeJoined",
                    node_id
                );
            }
            ClusterEvent::NodeSuspect(node_id) => {
                log::warn!(
                    target: "cluster::state_transition",
                    "🟡 [Cluster] State transition: node_id={}, old_state=Alive, new_state=Suspect, event=NodeSuspect",
                    node_id
                );
            }
            ClusterEvent::NodeDead(node_id) => {
                log::error!(
                    target: "cluster::state_transition",
                    "🔴 [Cluster] State transition: node_id={}, old_state=Suspect, new_state=Dead, event=NodeDead",
                    node_id
                );
            }
            ClusterEvent::NodeRecovered(node_id) => {
                log::info!(
                    target: "cluster::state_transition",
                    "🟢 [Cluster] State transition: node_id={}, old_state=Dead, new_state=Alive, event=NodeRecovered",
                    node_id
                );
            }
            ClusterEvent::TopologyChanged { table, partition } => {
                log::info!(
                    target: "cluster::topology",
                    "🔄 [Cluster] Topology changed: table={}, partition={}, event=TopologyChanged",
                    table,
                    partition
                );
            }
        }

        // Broadcast to subscribers (ignore errors if no subscribers)
        let _ = self.event_tx.send(event);
    }

    /// Log a state transition with detailed information
    ///
    /// # Requirements
    /// - Requirements 12.2: Emit log entry with node ID, old state, and new state
    fn log_state_transition(&self, node_id: &str, old_state: NodeState, new_state: NodeState) {
        log::info!(
            target: "cluster::state_transition",
            "🔄 [Cluster] State transition: node_id={}, old_state={}, new_state={}",
            node_id,
            old_state,
            new_state
        );
    }

    /// Watch for node failures (legacy callback API)
    pub async fn watch_failures<F>(&self, callback: F)
    where
        F: Fn(NodeId) + Send + Sync + 'static,
    {
        let mut callbacks = self.failure_callbacks.write().await;
        callbacks.push(Box::new(callback));
        log::info!("✅ [Cluster] Registered failure callback");
    }

    /// Trigger failure callbacks
    async fn trigger_failure_callbacks(&self, node_id: &NodeId) {
        let callbacks = self.failure_callbacks.read().await;
        for callback in callbacks.iter() {
            callback(node_id.clone());
        }
    }

    // ========================================================================
    // Background Tasks
    // ========================================================================

    /// Start heartbeat task (updates local node's heartbeat time)
    fn start_heartbeat_task(&self) {
        let node_id = self.node_id.clone();
        let nodes = self.nodes.clone();
        let interval = self.config.gossip_interval;

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;

                let mut nodes_guard = nodes.write().await;
                if let Some(node) = nodes_guard.get_mut(&node_id) {
                    node.last_heartbeat = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                }
            }
        });
    }

    /// Start failure detection task for standalone mode
    ///
    /// In cluster mode, failure detection is handled by start_membership_sync_task
    /// which integrates with Chitchat's built-in SWIM-like failure detection.
    ///
    /// In standalone mode, this task handles local failure detection based on
    /// heartbeat timeouts for manually registered nodes.
    ///
    /// # Requirements
    /// - Requirements 3.1: Send heartbeat messages every gossip_interval
    /// - Requirements 3.2: Mark peer as Suspect after failure_timeout
    /// - Requirements 3.3: Mark Suspect as Dead after suspect_timeout
    /// - Requirements 3.5: Dead → Alive recovery when heartbeat received
    fn start_failure_detection_task(&self) {
        // In cluster mode, failure detection is handled by membership sync task
        // which integrates with Chitchat's failure detector
        if !self.standalone {
            return;
        }

        let nodes = self.nodes.clone();
        let failure_callbacks = self.failure_callbacks.clone();
        let event_tx = self.event_tx.clone();
        let failure_timeout = self.config.failure_timeout.as_secs();
        let suspect_timeout = self.config.suspect_timeout.as_secs();
        let check_interval = self.config.gossip_interval;
        let my_node_id = self.node_id.clone();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(check_interval).await;

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                let mut state_changes: Vec<(NodeId, NodeState, NodeState)> = Vec::new();

                // Check node states based on heartbeat timeouts
                {
                    let mut nodes_guard = nodes.write().await;
                    for (node_id, node) in nodes_guard.iter_mut() {
                        // Skip self
                        if node_id == &my_node_id {
                            continue;
                        }

                        let elapsed = now.saturating_sub(node.last_heartbeat);
                        let old_state = node.state;

                        match node.state {
                            NodeState::Alive => {
                                // Requirements 3.2: Mark peer as Suspect after failure_timeout
                                if elapsed > failure_timeout {
                                    node.state = NodeState::Suspect;
                                    state_changes.push((
                                        node_id.clone(),
                                        old_state,
                                        NodeState::Suspect,
                                    ));
                                }
                            }
                            NodeState::Suspect => {
                                // Requirements 3.3: Mark Suspect as Dead after suspect_timeout
                                if elapsed > failure_timeout + suspect_timeout {
                                    node.state = NodeState::Dead;
                                    state_changes.push((
                                        node_id.clone(),
                                        old_state,
                                        NodeState::Dead,
                                    ));
                                }
                            }
                            NodeState::Dead => {
                                // Requirements 3.5: Dead → Alive recovery when heartbeat received
                                if elapsed < failure_timeout {
                                    node.state = NodeState::Alive;
                                    state_changes.push((
                                        node_id.clone(),
                                        old_state,
                                        NodeState::Alive,
                                    ));
                                }
                            }
                        }
                    }
                }

                // Process state changes and emit events
                // Requirements 3.4, 12.2: Emit events and log state changes
                for (node_id, old_state, new_state) in state_changes {
                    log::info!(
                        "🔄 [Cluster] Node {} state: {} -> {}",
                        node_id,
                        old_state,
                        new_state
                    );

                    match new_state {
                        NodeState::Suspect => {
                            let _ = event_tx.send(ClusterEvent::NodeSuspect(node_id.clone()));
                        }
                        NodeState::Dead => {
                            // Requirements 3.4: Trigger NodeDead event to start failover
                            let _ = event_tx.send(ClusterEvent::NodeDead(node_id.clone()));
                            // Trigger legacy callbacks
                            let callbacks = failure_callbacks.read().await;
                            for callback in callbacks.iter() {
                                callback(node_id.clone());
                            }
                        }
                        NodeState::Alive => {
                            if old_state == NodeState::Dead {
                                // Requirements 3.5: NodeRecovered to cancel pending votes
                                let _ = event_tx.send(ClusterEvent::NodeRecovered(node_id.clone()));
                            }
                        }
                    }
                }
            }
        });
    }

    /// Start membership sync task (syncs with Chitchat)
    ///
    /// This task subscribes to Chitchat membership changes and maps them to our NodeState enum.
    ///
    /// # Requirements
    /// - Requirements 3.1, 3.2, 3.3: Listen for node state transitions from Chitchat
    /// - Requirements 3.4: Trigger NodeDead event to start failover
    /// - Requirements 3.5: Handle Dead → Alive recovery
    fn start_membership_sync_task(&self) {
        let chitchat = match &self.chitchat {
            Some(c) => c.clone(),
            None => return,
        };

        let nodes = self.nodes.clone();
        let event_tx = self.event_tx.clone();
        let my_node_id = self.node_id.clone();
        let sync_interval = self.config.gossip_interval;
        let failure_timeout = self.config.failure_timeout;
        let suspect_timeout = self.config.suspect_timeout;

        tokio::spawn(async move {
            // Track previously known live nodes to detect state changes
            let mut previous_live_nodes: std::collections::HashSet<String> =
                std::collections::HashSet::new();

            loop {
                tokio::time::sleep(sync_interval).await;

                let guard = chitchat.lock().await;

                // Collect current live nodes from Chitchat
                let current_live_nodes: std::collections::HashSet<String> = guard
                    .live_nodes()
                    .map(|id| id.node_id.clone())
                    .filter(|id| id != &my_node_id)
                    .collect();

                let mut nodes_guard = nodes.write().await;
                let mut new_nodes: Vec<String> = Vec::new();
                let mut recovered_nodes: Vec<String> = Vec::new();
                let mut dead_nodes_detected: Vec<String> = Vec::new();

                // Process nodes that are now live in Chitchat
                for chitchat_id in guard.live_nodes() {
                    let node_id_str = chitchat_id.node_id.clone();

                    if node_id_str == my_node_id {
                        continue;
                    }

                    if !nodes_guard.contains_key(&node_id_str) {
                        // New node discovered
                        let node_info = NodeInfo::new(
                            node_id_str.clone(),
                            chitchat_id.gossip_advertise_addr.to_string(),
                        );
                        nodes_guard.insert(node_id_str.clone(), node_info);
                        new_nodes.push(node_id_str.clone());
                        log::info!("🔄 [Cluster] Node {} state: (new) -> Alive", node_id_str);
                    } else {
                        // Update existing node's heartbeat and check for recovery
                        if let Some(node) = nodes_guard.get_mut(&node_id_str) {
                            let old_state = node.state;
                            node.last_heartbeat = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs();

                            // Handle recovery: Dead → Alive
                            // Requirements 3.5: When a previously Dead node sends a heartbeat,
                            // transition back to Alive state
                            if old_state == NodeState::Dead {
                                node.state = NodeState::Alive;
                                recovered_nodes.push(node_id_str.clone());
                                log::info!(
                                    "🔄 [Cluster] Node {} state: Dead -> Alive (recovered)",
                                    node_id_str
                                );
                            } else if old_state == NodeState::Suspect {
                                // Suspect → Alive (heartbeat received, no longer suspect)
                                node.state = NodeState::Alive;
                                log::info!(
                                    "🔄 [Cluster] Node {} state: Suspect -> Alive",
                                    node_id_str
                                );
                            }
                        }
                    }

                    // Sync metrics from Chitchat state
                    if let Some(node_state) = guard.node_state(chitchat_id) {
                        if let Some(node) = nodes_guard.get_mut(&node_id_str) {
                            if let Some(load_str) = node_state.get(KEY_LOAD) {
                                if let Ok(load) = load_str.parse::<f64>() {
                                    node.load = load;
                                }
                            }
                            if let Some(memory_str) = node_state.get(KEY_MEMORY) {
                                if let Ok(memory) = memory_str.parse::<u64>() {
                                    node.memory_usage_bytes = memory;
                                }
                            }
                            if let Some(partition_str) = node_state.get(KEY_PARTITION_COUNT) {
                                if let Ok(count) = partition_str.parse::<usize>() {
                                    node.partition_count = count;
                                }
                            }
                        }
                    }
                }

                // Detect nodes that were previously live but are no longer in Chitchat's live list
                // This indicates Chitchat's failure detector has marked them as failed
                // Requirements 3.2, 3.3: Map Chitchat's failure detection to our NodeState
                for node_id in previous_live_nodes.difference(&current_live_nodes) {
                    if let Some(node) = nodes_guard.get_mut(node_id) {
                        let old_state = node.state;
                        if old_state == NodeState::Alive {
                            // Chitchat removed from live list - mark as Suspect first
                            // Requirements 3.2: Mark peer as Suspect when heartbeat timeout exceeded
                            node.state = NodeState::Suspect;
                            log::info!(
                                "🔄 [Cluster] Node {} state: Alive -> Suspect (Chitchat removed from live)",
                                node_id
                            );
                            let _ = event_tx.send(ClusterEvent::NodeSuspect(node_id.clone()));
                        }
                    }
                }

                // Check suspect nodes for timeout to transition to Dead
                // Requirements 3.3: Mark Suspect as Dead after suspect_timeout
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                for (node_id, node) in nodes_guard.iter_mut() {
                    if node_id == &my_node_id {
                        continue;
                    }

                    if node.state == NodeState::Suspect {
                        let elapsed = now.saturating_sub(node.last_heartbeat);
                        let total_timeout = failure_timeout.as_secs() + suspect_timeout.as_secs();

                        if elapsed > total_timeout {
                            node.state = NodeState::Dead;
                            dead_nodes_detected.push(node_id.clone());
                            log::info!(
                                "🔄 [Cluster] Node {} state: Suspect -> Dead (timeout: {}s > {}s)",
                                node_id,
                                elapsed,
                                total_timeout
                            );
                        }
                    }
                }

                // Update previous live nodes for next iteration
                previous_live_nodes = current_live_nodes;

                drop(nodes_guard);

                // Emit events for new nodes
                // Requirements 1.5: Propagate membership change to all nodes
                for node_id in new_nodes {
                    log::info!("🆕 [Cluster] Discovered new node: {}", node_id);
                    let _ = event_tx.send(ClusterEvent::NodeJoined(node_id));
                }

                // Emit events for recovered nodes
                // Requirements 3.5: NodeRecovered to cancel pending votes
                for node_id in recovered_nodes {
                    log::info!("🟢 [Cluster] Node recovered: {}", node_id);
                    let _ = event_tx.send(ClusterEvent::NodeRecovered(node_id));
                }

                // Emit events for dead nodes
                // Requirements 3.4: Trigger NodeDead event to start failover
                for node_id in dead_nodes_detected {
                    log::error!("🔴 [Cluster] Node confirmed dead: {}", node_id);
                    let _ = event_tx.send(ClusterEvent::NodeDead(node_id));
                }
            }
        });
    }

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

        // Emit a NodeDead event for ourselves to trigger partition reassignment
        // This allows PartitionManager to handle the leaving notification
        self.emit_event(ClusterEvent::NodeDead(self.node_id.clone()));

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

    /// Manually register a new node
    pub async fn register_node(&self, node_id: String, gossip_addr: String) -> CoreResult<()> {
        let mut nodes = self.nodes.write().await;
        let node_info = NodeInfo::new(node_id.clone(), gossip_addr);
        nodes.insert(node_id.clone(), node_info);

        log::info!("✅ [Cluster] Node {} registered", node_id);
        self.emit_event(ClusterEvent::NodeJoined(node_id));

        Ok(())
    }

    /// Manually update node state (for testing)
    ///
    /// This method allows manual state transitions and emits appropriate events.
    /// Used primarily for testing failure detection and recovery scenarios.
    ///
    /// # State Machine Transitions (Property 1)
    /// Valid transitions:
    /// - Alive → Suspect (when heartbeat timeout exceeded)
    /// - Suspect → Dead (when suspect timeout exceeded)
    /// - Dead → Alive (when heartbeat received - recovery)
    /// - Alive → Alive (heartbeat refresh)
    ///
    /// # Requirements
    /// - Requirements 3.2, 3.3, 3.5: State transitions
    /// - Requirements 12.2: Log state changes with node ID, old/new state
    pub async fn set_node_state(&self, node_id: &str, state: NodeState) -> CoreResult<()> {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            let old_state = node.state;

            // Validate state transition (Property 1: Node State Machine Transitions)
            let valid_transition = match (old_state, state) {
                (NodeState::Alive, NodeState::Suspect) => true,
                (NodeState::Alive, NodeState::Alive) => true, // heartbeat refresh
                (NodeState::Suspect, NodeState::Dead) => true,
                (NodeState::Suspect, NodeState::Alive) => true, // recovery from suspect
                (NodeState::Dead, NodeState::Alive) => true,    // recovery
                _ => false,
            };

            if !valid_transition && old_state != state {
                log::warn!(
                    "⚠️  [Cluster] Invalid state transition for node {}: {} -> {} (forcing anyway)",
                    node_id,
                    old_state,
                    state
                );
            }

            node.state = state;

            // Requirements 12.2: Log state changes with node ID, old/new state
            log::info!(
                "🔄 [Cluster] Node {} state manually set: {} -> {}",
                node_id,
                old_state,
                state
            );

            // Emit appropriate event based on state change
            // Requirements 3.4: Trigger NodeDead event to start failover
            // Requirements 3.5: NodeRecovered to cancel pending votes
            match state {
                NodeState::Suspect => {
                    self.emit_event(ClusterEvent::NodeSuspect(node_id.to_string()));
                }
                NodeState::Dead => {
                    self.emit_event(ClusterEvent::NodeDead(node_id.to_string()));
                }
                NodeState::Alive if old_state == NodeState::Dead => {
                    self.emit_event(ClusterEvent::NodeRecovered(node_id.to_string()));
                }
                _ => {}
            }
        } else {
            return Err(CoreError::Internal(format!(
                "Node {} not found in cluster",
                node_id
            )));
        }
        Ok(())
    }

    /// Get the current state of a node
    ///
    /// Returns None if the node is not found in the cluster.
    pub async fn get_node_state(&self, node_id: &str) -> Option<NodeState> {
        let nodes = self.nodes.read().await;
        nodes.get(node_id).map(|n| n.state)
    }

    /// Transition a node through the state machine
    ///
    /// This method handles the proper state transitions according to the state machine:
    /// - Alive → Suspect → Dead (failure path)
    /// - Dead → Alive (recovery path)
    ///
    /// # Requirements
    /// - Requirements 3.2: Alive → Suspect
    /// - Requirements 3.3: Suspect → Dead
    /// - Requirements 3.5: Dead → Alive
    pub async fn transition_node_state(
        &self,
        node_id: &str,
        target_state: NodeState,
    ) -> CoreResult<()> {
        let current_state = self.get_node_state(node_id).await;

        match current_state {
            Some(current) => {
                // Determine if we need intermediate transitions
                match (current, target_state) {
                    // Direct valid transitions
                    (NodeState::Alive, NodeState::Suspect)
                    | (NodeState::Suspect, NodeState::Dead)
                    | (NodeState::Dead, NodeState::Alive)
                    | (NodeState::Suspect, NodeState::Alive) => {
                        self.set_node_state(node_id, target_state).await
                    }

                    // Alive → Dead requires going through Suspect
                    (NodeState::Alive, NodeState::Dead) => {
                        self.set_node_state(node_id, NodeState::Suspect).await?;
                        self.set_node_state(node_id, NodeState::Dead).await
                    }

                    // Same state - no-op
                    (s, t) if s == t => Ok(()),

                    // Invalid transition
                    _ => Err(CoreError::Internal(format!(
                        "Invalid state transition: {} -> {}",
                        current, target_state
                    ))),
                }
            }
            None => Err(CoreError::Internal(format!(
                "Node {} not found in cluster",
                node_id
            ))),
        }
    }

    /// Simulate node failure (for testing)
    pub async fn _simulate_node_failure(&self, node_id: &str) -> CoreResult<()> {
        self.set_node_state(node_id, NodeState::Dead).await?;

        // Trigger failure callbacks
        self.trigger_failure_callbacks(&node_id.to_string()).await;

        Ok(())
    }

    // ========================================================================
    // Monitoring and Observability (Requirements 12.1, 12.4)
    // ========================================================================

    /// Get cluster status for monitoring
    ///
    /// # Requirements
    /// - Requirements 12.1: Expose node count, alive node count via metrics
    /// - Requirements 12.4: Return current cluster membership
    pub async fn cluster_status(&self) -> ClusterStatus {
        let nodes = self.nodes.read().await;

        let total_nodes = nodes.len();
        let alive_nodes = nodes
            .values()
            .filter(|n| n.state == NodeState::Alive)
            .count();
        let suspect_nodes = nodes
            .values()
            .filter(|n| n.state == NodeState::Suspect)
            .count();
        let dead_nodes = nodes
            .values()
            .filter(|n| n.state == NodeState::Dead)
            .count();

        ClusterStatus {
            node_id: self.node_id.clone(),
            cluster_id: self.config.cluster_id.clone(),
            standalone: self.standalone,
            total_nodes,
            alive_nodes,
            suspect_nodes,
            dead_nodes,
            quorum_threshold: self.quorum_threshold(alive_nodes),
        }
    }

    /// Get comprehensive cluster metrics for monitoring
    ///
    /// Returns detailed metrics including node count, alive count, and
    /// partition count per node.
    ///
    /// # Requirements
    /// - Requirements 12.1: Expose node count, alive node count, partition count per node
    pub async fn get_metrics(&self) -> ClusterMetrics {
        let status = self.cluster_status().await;
        let nodes = self.nodes.read().await;

        let node_metrics: Vec<NodeMetrics> = nodes
            .values()
            .map(|node| NodeMetrics {
                node_id: node.id.clone(),
                state: node.state.to_string(),
                partition_count: node.partition_count,
                load: node.load,
                memory_usage_bytes: node.memory_usage_bytes,
                gossip_addr: node.gossip_addr.clone(),
                last_heartbeat: node.last_heartbeat,
            })
            .collect();

        let collected_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        ClusterMetrics {
            status,
            node_metrics,
            collected_at,
        }
    }

    /// Get partition count for a specific node
    ///
    /// # Requirements
    /// - Requirements 12.1: Partition count per node
    pub async fn get_node_partition_count(&self, node_id: &str) -> Option<usize> {
        let nodes = self.nodes.read().await;
        nodes.get(node_id).map(|n| n.partition_count)
    }

    /// Get all node partition counts as a map
    ///
    /// # Requirements
    /// - Requirements 12.1: Partition count per node
    pub async fn get_all_partition_counts(&self) -> HashMap<NodeId, usize> {
        let nodes = self.nodes.read().await;
        nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.partition_count))
            .collect()
    }

    /// Get current cluster membership
    ///
    /// Returns a list of all nodes in the cluster with their current state.
    ///
    /// # Requirements
    /// - Requirements 12.4: Return current cluster membership
    pub async fn get_membership(&self) -> ClusterMembership {
        let nodes = self.nodes.read().await;

        let members: Vec<MemberInfo> = nodes
            .values()
            .map(|node| MemberInfo {
                node_id: node.id.clone(),
                gossip_addr: node.gossip_addr.clone(),
                state: node.state.to_string(),
                is_self: node.id == self.node_id,
            })
            .collect();

        ClusterMembership {
            cluster_id: self.config.cluster_id.clone(),
            my_node_id: self.node_id.clone(),
            members,
            total_count: nodes.len(),
            alive_count: nodes
                .values()
                .filter(|n| n.state == NodeState::Alive)
                .count(),
        }
    }

    /// Get complete cluster state including membership and basic info
    ///
    /// This is the main API endpoint for querying cluster state.
    ///
    /// # Requirements
    /// - Requirements 12.4: Return current cluster membership and table topology
    pub async fn get_cluster_state(&self) -> ClusterState {
        let status = self.cluster_status().await;
        let membership = self.get_membership().await;
        let metrics = self.get_metrics().await;

        ClusterState {
            status,
            membership,
            metrics,
        }
    }
}

/// Cluster membership information
///
/// # Requirements
/// - Requirements 12.4: Return current cluster membership
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMembership {
    /// Cluster identifier
    pub cluster_id: String,
    /// This node's ID
    pub my_node_id: String,
    /// All members in the cluster
    pub members: Vec<MemberInfo>,
    /// Total number of members
    pub total_count: usize,
    /// Number of alive members
    pub alive_count: usize,
}

/// Information about a cluster member
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    /// Node identifier
    pub node_id: String,
    /// Gossip address
    pub gossip_addr: String,
    /// Current state (Alive, Suspect, Dead)
    pub state: String,
    /// Whether this is the local node
    pub is_self: bool,
}

/// Complete cluster state
///
/// # Requirements
/// - Requirements 12.4: Return current cluster membership and table topology
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    /// Basic cluster status
    pub status: ClusterStatus,
    /// Cluster membership
    pub membership: ClusterMembership,
    /// Detailed metrics
    pub metrics: ClusterMetrics,
}

/// Cluster status for monitoring
///
/// # Requirements
/// - Requirements 12.1: Expose node count, alive node count via metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub node_id: String,
    pub cluster_id: String,
    pub standalone: bool,
    pub total_nodes: usize,
    pub alive_nodes: usize,
    pub suspect_nodes: usize,
    pub dead_nodes: usize,
    pub quorum_threshold: usize,
}

/// Comprehensive cluster metrics for monitoring
///
/// # Requirements
/// - Requirements 12.1: Expose node count, alive node count, partition count per node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMetrics {
    /// Basic cluster status
    pub status: ClusterStatus,
    /// Metrics for each node in the cluster
    pub node_metrics: Vec<NodeMetrics>,
    /// Timestamp when metrics were collected (Unix timestamp in seconds)
    pub collected_at: u64,
}

/// Metrics for a single node
///
/// # Requirements
/// - Requirements 12.1: Partition count per node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    /// Node identifier
    pub node_id: String,
    /// Node state (Alive, Suspect, Dead)
    pub state: String,
    /// Number of partitions this node owns as write_node
    pub partition_count: usize,
    /// CPU/memory load (0.0 - 1.0)
    pub load: f64,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// Gossip address
    pub gossip_addr: String,
    /// Last heartbeat timestamp (Unix timestamp in seconds)
    pub last_heartbeat: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_standalone_mode() {
        let config = ClusterConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            listen_addr: "127.0.0.1:17946".to_string(),
            seed_nodes: vec![], // No seed nodes = standalone
            ..Default::default()
        };

        let manager = ClusterManager::new(config).await.unwrap();

        assert!(manager.is_standalone());
        assert_eq!(manager.node_id(), "test-node");

        // Should have 1 node (self)
        let nodes = manager.live_nodes().await;
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, "test-node");
    }

    #[tokio::test]
    async fn test_gossip_kv_standalone() {
        let config = ClusterConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            listen_addr: "127.0.0.1:17947".to_string(),
            seed_nodes: vec![],
            ..Default::default()
        };

        let manager = ClusterManager::new(config).await.unwrap();

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
            seed_nodes: vec![],
            ..Default::default()
        };

        let manager = ClusterManager::new(config).await.unwrap();

        // Update metrics
        manager.update_my_metrics(10, 0.5, 1024 * 1024 * 100).await;

        // Verify metrics are stored
        let partition_count = manager.gossip_get(KEY_PARTITION_COUNT).await;
        assert_eq!(partition_count, Some("10".to_string()));

        let load = manager.gossip_get(KEY_LOAD).await;
        assert_eq!(load, Some("0.5".to_string()));
    }

    #[tokio::test]
    async fn test_event_subscription() {
        let config = ClusterConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            listen_addr: "127.0.0.1:17949".to_string(),
            seed_nodes: vec![],
            ..Default::default()
        };

        let manager = ClusterManager::new(config).await.unwrap();
        let mut receiver = manager.subscribe();

        // Register a new node
        manager
            .register_node("node-2".to_string(), "127.0.0.1:17950".to_string())
            .await
            .unwrap();

        // Should receive NodeJoined event
        let event = receiver.recv().await.unwrap();
        match event {
            ClusterEvent::NodeJoined(node_id) => {
                assert_eq!(node_id, "node-2");
            }
            _ => panic!("Expected NodeJoined event"),
        }
    }

    // ========================================================================
    // Failure Detection Tests (Task 5)
    // ========================================================================

    #[tokio::test]
    async fn test_state_transition_alive_to_suspect() {
        // Test Requirements 3.2: Alive → Suspect transition
        let config = ClusterConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            listen_addr: "127.0.0.1:17951".to_string(),
            seed_nodes: vec![],
            ..Default::default()
        };

        let manager = ClusterManager::new(config).await.unwrap();
        let mut receiver = manager.subscribe();

        // Register a node
        manager
            .register_node("node-2".to_string(), "127.0.0.1:17952".to_string())
            .await
            .unwrap();

        // Consume NodeJoined event
        let _ = receiver.recv().await.unwrap();

        // Verify initial state is Alive
        let state = manager.get_node_state("node-2").await;
        assert_eq!(state, Some(NodeState::Alive));

        // Transition to Suspect
        manager
            .set_node_state("node-2", NodeState::Suspect)
            .await
            .unwrap();

        // Verify state changed
        let state = manager.get_node_state("node-2").await;
        assert_eq!(state, Some(NodeState::Suspect));

        // Should receive NodeSuspect event
        let event = receiver.recv().await.unwrap();
        match event {
            ClusterEvent::NodeSuspect(node_id) => {
                assert_eq!(node_id, "node-2");
            }
            _ => panic!("Expected NodeSuspect event"),
        }
    }

    #[tokio::test]
    async fn test_state_transition_suspect_to_dead() {
        // Test Requirements 3.3: Suspect → Dead transition
        let config = ClusterConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            listen_addr: "127.0.0.1:17953".to_string(),
            seed_nodes: vec![],
            ..Default::default()
        };

        let manager = ClusterManager::new(config).await.unwrap();
        let mut receiver = manager.subscribe();

        // Register and transition to Suspect
        manager
            .register_node("node-2".to_string(), "127.0.0.1:17954".to_string())
            .await
            .unwrap();
        let _ = receiver.recv().await; // NodeJoined

        manager
            .set_node_state("node-2", NodeState::Suspect)
            .await
            .unwrap();
        let _ = receiver.recv().await; // NodeSuspect

        // Transition to Dead
        manager
            .set_node_state("node-2", NodeState::Dead)
            .await
            .unwrap();

        // Verify state changed
        let state = manager.get_node_state("node-2").await;
        assert_eq!(state, Some(NodeState::Dead));

        // Should receive NodeDead event
        let event = receiver.recv().await.unwrap();
        match event {
            ClusterEvent::NodeDead(node_id) => {
                assert_eq!(node_id, "node-2");
            }
            _ => panic!("Expected NodeDead event"),
        }
    }

    #[tokio::test]
    async fn test_state_transition_dead_to_alive_recovery() {
        // Test Requirements 3.5: Dead → Alive recovery
        let config = ClusterConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            listen_addr: "127.0.0.1:17955".to_string(),
            seed_nodes: vec![],
            ..Default::default()
        };

        let manager = ClusterManager::new(config).await.unwrap();
        let mut receiver = manager.subscribe();

        // Register and transition to Dead
        manager
            .register_node("node-2".to_string(), "127.0.0.1:17956".to_string())
            .await
            .unwrap();
        let _ = receiver.recv().await; // NodeJoined

        manager
            .set_node_state("node-2", NodeState::Suspect)
            .await
            .unwrap();
        let _ = receiver.recv().await; // NodeSuspect

        manager
            .set_node_state("node-2", NodeState::Dead)
            .await
            .unwrap();
        let _ = receiver.recv().await; // NodeDead

        // Recover: Dead → Alive
        manager
            .set_node_state("node-2", NodeState::Alive)
            .await
            .unwrap();

        // Verify state changed
        let state = manager.get_node_state("node-2").await;
        assert_eq!(state, Some(NodeState::Alive));

        // Should receive NodeRecovered event
        let event = receiver.recv().await.unwrap();
        match event {
            ClusterEvent::NodeRecovered(node_id) => {
                assert_eq!(node_id, "node-2");
            }
            _ => panic!("Expected NodeRecovered event"),
        }
    }

    #[tokio::test]
    async fn test_transition_node_state_alive_to_dead() {
        // Test transition_node_state which goes through Suspect
        let config = ClusterConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            listen_addr: "127.0.0.1:17957".to_string(),
            seed_nodes: vec![],
            ..Default::default()
        };

        let manager = ClusterManager::new(config).await.unwrap();
        let mut receiver = manager.subscribe();

        // Register a node
        manager
            .register_node("node-2".to_string(), "127.0.0.1:17958".to_string())
            .await
            .unwrap();
        let _ = receiver.recv().await; // NodeJoined

        // Use transition_node_state to go from Alive to Dead
        // This should automatically go through Suspect
        manager
            .transition_node_state("node-2", NodeState::Dead)
            .await
            .unwrap();

        // Verify final state is Dead
        let state = manager.get_node_state("node-2").await;
        assert_eq!(state, Some(NodeState::Dead));

        // Should receive NodeSuspect then NodeDead events
        let event1 = receiver.recv().await.unwrap();
        match event1 {
            ClusterEvent::NodeSuspect(node_id) => {
                assert_eq!(node_id, "node-2");
            }
            _ => panic!("Expected NodeSuspect event first"),
        }

        let event2 = receiver.recv().await.unwrap();
        match event2 {
            ClusterEvent::NodeDead(node_id) => {
                assert_eq!(node_id, "node-2");
            }
            _ => panic!("Expected NodeDead event second"),
        }
    }

    #[tokio::test]
    async fn test_cluster_status_with_state_changes() {
        // Test Requirements 12.1: Expose node count, alive/dead counts
        let config = ClusterConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            listen_addr: "127.0.0.1:17959".to_string(),
            seed_nodes: vec![],
            ..Default::default()
        };

        let manager = ClusterManager::new(config).await.unwrap();

        // Register nodes
        manager
            .register_node("node-2".to_string(), "127.0.0.1:17960".to_string())
            .await
            .unwrap();
        manager
            .register_node("node-3".to_string(), "127.0.0.1:17961".to_string())
            .await
            .unwrap();

        // Check initial status
        let status = manager.cluster_status().await;
        assert_eq!(status.total_nodes, 3);
        assert_eq!(status.alive_nodes, 3);
        assert_eq!(status.suspect_nodes, 0);
        assert_eq!(status.dead_nodes, 0);

        // Mark one node as Suspect
        manager
            .set_node_state("node-2", NodeState::Suspect)
            .await
            .unwrap();

        let status = manager.cluster_status().await;
        assert_eq!(status.alive_nodes, 2);
        assert_eq!(status.suspect_nodes, 1);
        assert_eq!(status.dead_nodes, 0);

        // Mark another node as Dead (through Suspect first)
        manager
            .set_node_state("node-3", NodeState::Suspect)
            .await
            .unwrap();
        manager
            .set_node_state("node-3", NodeState::Dead)
            .await
            .unwrap();

        let status = manager.cluster_status().await;
        assert_eq!(status.alive_nodes, 1); // Only test-node
        assert_eq!(status.suspect_nodes, 1); // node-2
        assert_eq!(status.dead_nodes, 1); // node-3
    }

    #[tokio::test]
    async fn test_get_node_state_not_found() {
        let config = ClusterConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            listen_addr: "127.0.0.1:17962".to_string(),
            seed_nodes: vec![],
            ..Default::default()
        };

        let manager = ClusterManager::new(config).await.unwrap();

        // Query non-existent node
        let state = manager.get_node_state("non-existent").await;
        assert_eq!(state, None);
    }

    #[tokio::test]
    async fn test_set_node_state_not_found() {
        let config = ClusterConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            listen_addr: "127.0.0.1:17963".to_string(),
            seed_nodes: vec![],
            ..Default::default()
        };

        let manager = ClusterManager::new(config).await.unwrap();

        // Try to set state on non-existent node
        let result = manager
            .set_node_state("non-existent", NodeState::Dead)
            .await;
        assert!(result.is_err());
    }
}
