//! Engine extension for SessionConfig
//!
//! 允许将 Engine 注入到 SessionConfig 中，供远程执行使用

use std::sync::Arc;
use crate::engine::Engine;

// 🎯 直接使用 Arc<Engine>，不需要额外包装！
// DataFusion 的 extension 系统支持任何 Send + Sync + 'static 的类型
pub type EngineExtension = Arc<Engine>;

/// 扩展键（现在不需要了，直接用类型）
pub const ENGINE_EXTENSION_KEY: &str = "calm.engine";
