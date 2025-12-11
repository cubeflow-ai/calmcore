pub mod catalog;
pub mod cluster;
pub mod compute;
pub mod engine;
#[macro_use]
pub mod utils;
// pub mod analyzer;  // Temporarily disabled due to missing dependencies
pub mod protocol;
pub(crate) mod service;
pub(crate) mod storage;

// Re-export modules for backward compatibility
pub mod schema {
    pub use crate::catalog::schema::*;
}
pub mod partition {
    pub use crate::storage::partition::*;
}
pub use protocol::elasticsearch::query_rewriter;
pub use storage::router;
pub use storage::segment;
pub mod segment_loader {
    pub use crate::storage::segment::segment_loader::*;
}
