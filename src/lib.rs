pub mod catalog;
pub mod cluster;
// pub mod compute;
pub mod engine;
pub mod service;
#[macro_use]
pub mod utils;
// pub mod analyzer;  // Temporarily disabled due to missing dependencies
pub mod protocol;
// TODO: 暂时禁用,等 tarpc Bincode API 问题解决后再启用
// pub(crate) mod service;
pub(crate) mod storage;

// Re-export modules for backward compatibility
pub mod schema {
    pub use crate::catalog::schema::*;
}
pub mod partition {
    pub use crate::storage::partition::*;
}
pub use storage::router;
pub use storage::segment;
pub mod segment_loader {
    pub use crate::storage::segment::segment_loader::*;
}
