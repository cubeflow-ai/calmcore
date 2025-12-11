use std::{collections::HashMap, sync::Arc};

use chitchat::Chitchat;
use tokio::sync::{Mutex, RwLock};

pub mod key_dic {
    pub fn partition_key(table_name: &str, partition_name: &str) -> String {
        format!("partition:{}:{}", table_name, partition_name)
    }
}

pub struct PartitionManager {
    node_id: String,
    /// Chitchat handle for Gossip communication
    chitchat: Arc<Mutex<Chitchat>>,
    /// In-memory key-value store for distributed metadata
    cache: RwLock<HashMap<String, String>>,
}

impl PartitionManager {
    pub fn new(node_id: String, chitchat: Arc<Mutex<Chitchat>>) -> Self {
        Self {
            node_id,
            chitchat,
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub async fn put_partition(&self, table_name: &str, partition_name: &str) {
        let key = key_dic::partition_key(table_name, partition_name);
        let mut guard = self.chitchat.lock().await;
        guard
            .self_node_state()
            .set(key.clone(), self.node_id.clone());
        self.cache.write().await.insert(key, self.node_id.clone());
    }

    pub async fn del_partition(&self, table_name: &str, partition_name: &str) {
        let key = key_dic::partition_key(table_name, partition_name);
        let mut guard = self.chitchat.lock().await;
        guard.self_node_state().delete(&key);
        self.cache.write().await.remove(&key);
    }

    pub async fn get_partition_owner(
        &self,
        table_name: &str,
        partition_name: &str,
    ) -> Option<String> {
        let key = key_dic::partition_key(table_name, partition_name);

        // First check local cache
        if let Some(owner) = self.cache.read().await.get(&key) {
            return Some(owner.clone());
        }

        // Fallback to Gossip
        let guard = self.chitchat.lock().await;

        for (_, state) in guard.node_states() {
            if let Some(value) = state.get(&key) {
                self.cache
                    .write()
                    .await
                    .insert(key.clone(), value.to_string());
                return Some(value.to_string());
            }
        }
        None
    }
}
