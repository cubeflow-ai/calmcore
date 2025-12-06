//! 分布式查询模块
//!
//! 核心设计原则：**零侵入**
//! - 单机模式的代码路径和性能完全不受影响
//! - 分布式代码在独立目录，不修改现有单机代码
//! - 复用现有组件（SegmentScanner、索引优化等）
//!
//! ## 模块结构
//!
//! - `config`: 分布式查询配置
//! - `query_coordinator`: 查询协调器，统一入口
//! - `distributed_executor`: 分布式执行器
//! - `distributed_table`: 分布式 TableProvider
//! - `shuffle_exec`: Shuffle 执行计划
//! - `shuffle_stream`: Shuffle 流处理
//! - `node_client`: 节点间通信客户端
//!
//! ## 执行流程
//!
//! ```text
//! Client SQL
//!     │
//!     ▼
//! QueryCoordinator
//!     │
//!     ├─ 单机模式 ──► DataFusionExecutor (现有代码)
//!     │
//!     └─ 分布式模式 ──► DistributedExecutor
//!                           │
//!                           ├─ 无 GROUP BY ──► Scatter-Gather
//!                           │
//!                           └─ 有 GROUP BY ──► Shuffle + 本地聚合
//! ```

pub mod config;
pub mod distributed_executor;
pub mod distributed_table;
pub mod error_handler;
pub mod node_client;
pub mod query_coordinator;
pub mod rpc_server;
pub mod shuffle_exec;
pub mod shuffle_stream;
pub mod shuffle_transport;

// Re-exports
pub use config::DistributedConfig;
pub use distributed_executor::{DistributedExecutor, RemotePartitionInfo};
pub use distributed_table::{DistributedTableProvider, RemoteScanExec, ScatterGatherExec};
pub use error_handler::{
    is_node_unreachable_error, node_unreachable_error, shuffle_failed_error, DistributedQueryError,
    ErrorAggregator, QueryCancellationManager, TimeoutExecutor,
};
pub use node_client::{
    deserialize_batches, serialize_batch, CancelRequest, CancelResponse, HealthRequest,
    HealthResponse, NodeClient, NodeClientManager, QueryRequest, QueryResponse, QueryResultChunk,
    ShuffleAck, ShuffleChunk,
};
pub use query_coordinator::QueryCoordinator;
pub use rpc_server::{create_distributed_routes, DistributedQueryServiceState};
pub use shuffle_exec::{
    DefaultRemoteReceiverFactory, DefaultRemoteSenderFactory, MultiSourceShuffleReceiverExec,
    NetworkRemoteReceiverFactory, NetworkRemoteSenderFactory, NetworkShuffleManager,
    RemoteReceiverFactory, RemoteSenderFactory, ShuffleDataInjector, ShuffleExec,
    ShuffleReceiverExec,
};
pub use shuffle_stream::{ShuffleConfig, ShuffleStream};
pub use shuffle_transport::{ShuffleSender, ShuffleTransportManager, ShuffleTransportTask};
