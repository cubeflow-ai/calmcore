//! Engine 模块 - 核心存储引擎实现
//!
//! ## 模块结构
//! - `config`: 配置和共享类型定义
//! - `metadata`: 表和分区的元数据管理
//! - `lifecycle`: 引擎的启动和关闭逻辑
//! - `persist`: 后台持久化任务
//! - `data_operations`: 数据插入、查询和加载操作

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};

use crate::catalog::Catalog;
use crate::partition::Partition;

// 子模块声明
pub mod config;
pub mod data_operations;
pub mod lifecycle;
pub mod metadata;
pub mod persist;

// 重新导出常用类型
pub use config::{EngineConfig, EngineStats, InsertStats};

// 内部使用的类型
use config::PersistRequest;

/// 核心存储引擎
///
/// Engine 负责：
/// - 管理多个表及其分区
/// - 协调数据的插入、查询和持久化
/// - 提供统一的数据访问接口
#[derive(Clone)]
pub struct Engine {
    /// 引擎配置
    pub(crate) config: EngineConfig,

    /// Catalog 负责表的元数据管理
    pub(crate) catalog: Arc<Catalog>,

    /// 分区存储：(table_name, partition_name) -> Partition
    ///
    /// 使用 RwLock<HashMap> 提供并发读写访问
    pub(crate) partitions: Arc<RwLock<HashMap<(String, String), Arc<Partition>>>>,

    /// 持久化请求通道（发送端）
    pub(crate) persist_tx: mpsc::UnboundedSender<PersistRequest>,

    /// 分区通知通道（发送端）
    ///
    /// 当分区有新数据时，通过此通道通知持久化任务
    /// (table_name, partition_name)
    pub(crate) partition_notify_tx: Arc<mpsc::UnboundedSender<(String, String)>>,

    /// 持久化任务句柄
    ///
    /// 用于在停止时等待后台持久化任务完成
    pub(crate) persist_task_handle: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}
