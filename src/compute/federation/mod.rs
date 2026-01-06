//! Federation 模块 - 支持跨节点查询
//!
//! 基于 datafusion-federation 实现分布式查询

mod flight_executor;
// mod flight_sql_executor;
mod mixed_provider;
mod query_executor;
mod remote_provider;
mod remote_scan_exec;

pub use flight_executor::FlightExecutor;
pub use mixed_provider::{MixedTableProvider, NodePartitions};
pub use query_executor::FederatedQueryExecutor;
pub use remote_provider::RemoteTableProvider;
pub use remote_scan_exec::RemoteScanExec;
