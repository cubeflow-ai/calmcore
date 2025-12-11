//! Query Router for routing queries to appropriate nodes
//!
//! Routes write queries to the partition's write_node and read queries
//! to one of the partition's read_nodes with load balancing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::partition_manager::PartitionManager;
use crate::utils::error::{CoreError, CoreResult};

/// Query Router routes queries to appropriate nodes
pub struct QueryRouter {
    partition_manager: Arc<PartitionManager>,
    /// Round-robin counter for read load balancing
    read_counter: AtomicUsize,
}

type NodeId = String;

impl QueryRouter {
    /// Create a new QueryRouter
    pub fn new(partition_manager: Arc<PartitionManager>) -> Self {
        Self {
            partition_manager,
            read_counter: AtomicUsize::new(0),
        }
    }

    /// Route a write query to the partition's write_node
    pub async fn route_write(&self, table: &str, partition: &str) -> CoreResult<NodeId> {
        // self.partition_manager
        //     .get_write_node(table, partition)
        //     .await
        //     .ok_or_else(|| {
        //         CoreError::Internal(format!(
        //             "Write node unavailable for partition {}:{}",
        //             table, partition
        //         ))
        //     })
        todo!()
    }

    /// Route a read query to one of the partition's read_nodes (load balanced)
    pub async fn route_read(&self, table: &str, partition: &str) -> CoreResult<NodeId> {
        todo!()
        // let read_nodes = self
        //     .partition_manager
        //     .get_read_nodes(table, partition)
        //     .await
        //     .ok_or_else(|| {
        //         CoreError::Internal(format!(
        //             "No read nodes available for partition {}:{}",
        //             table, partition
        //         ))
        //     })?;

        // if read_nodes.is_empty() {
        //     return Err(CoreError::Internal(format!(
        //         "Read nodes list is empty for partition {}:{}",
        //         table, partition
        //     )));
        // }

        // // Round-robin selection for load balancing
        // let idx = self.read_counter.fetch_add(1, Ordering::Relaxed) % read_nodes.len();
        // Ok(read_nodes[idx].clone())
    }
}
