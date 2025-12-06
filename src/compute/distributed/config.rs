//! 分布式查询配置
//!
//! 从 calm.toml 的 [distributed] 段加载配置

use std::time::Duration;

use crate::utils::error::CoreResult;

/// 分布式查询配置
#[derive(Debug, Clone)]
pub struct DistributedConfig {
    /// 是否启用分布式查询
    pub enabled: bool,

    /// 查询超时时间（毫秒）
    pub query_timeout_ms: u64,

    /// Shuffle 缓冲区大小（行数）
    pub shuffle_buffer_size: usize,

    /// 最大并发查询数
    pub max_concurrent_queries: usize,

    /// 节点间通信端口（RPC）
    pub rpc_port: u16,

    /// 连接超时时间（毫秒）
    pub connect_timeout_ms: u64,

    /// 请求超时时间（毫秒）
    pub request_timeout_ms: u64,
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            query_timeout_ms: 30_000,
            shuffle_buffer_size: 10_000,
            max_concurrent_queries: 100,
            rpc_port: 7947,
            connect_timeout_ms: 5_000,
            request_timeout_ms: 30_000,
        }
    }
}

impl DistributedConfig {
    /// 从 TOML 配置加载
    pub fn from_toml(toml_content: &str) -> CoreResult<Self> {
        let mut config = Self::default();

        if let Ok(value) = toml_content.parse::<toml::Table>() {
            if let Some(distributed) = value.get("distributed").and_then(|v| v.as_table()) {
                if let Some(val) = distributed.get("enabled").and_then(|v| v.as_bool()) {
                    config.enabled = val;
                }

                if let Some(val) = distributed
                    .get("query_timeout_ms")
                    .and_then(|v| v.as_integer())
                {
                    config.query_timeout_ms = val as u64;
                }

                if let Some(val) = distributed
                    .get("shuffle_buffer_size")
                    .and_then(|v| v.as_integer())
                {
                    config.shuffle_buffer_size = val as usize;
                }

                if let Some(val) = distributed
                    .get("max_concurrent_queries")
                    .and_then(|v| v.as_integer())
                {
                    config.max_concurrent_queries = val as usize;
                }

                if let Some(val) = distributed.get("rpc_port").and_then(|v| v.as_integer()) {
                    config.rpc_port = val as u16;
                }

                if let Some(val) = distributed
                    .get("connect_timeout_ms")
                    .and_then(|v| v.as_integer())
                {
                    config.connect_timeout_ms = val as u64;
                }

                if let Some(val) = distributed
                    .get("request_timeout_ms")
                    .and_then(|v| v.as_integer())
                {
                    config.request_timeout_ms = val as u64;
                }
            }
        }

        Ok(config)
    }

    /// 从配置文件加载
    pub fn load(config_path: Option<&str>) -> CoreResult<Self> {
        let config = if let Some(path) = config_path {
            if let Ok(content) = std::fs::read_to_string(path) {
                Self::from_toml(&content)?
            } else {
                Self::default()
            }
        } else {
            // 尝试默认路径
            if let Ok(content) = std::fs::read_to_string("calm.toml") {
                Self::from_toml(&content)?
            } else {
                Self::default()
            }
        };

        Ok(config)
    }

    /// 获取查询超时 Duration
    pub fn query_timeout(&self) -> Duration {
        Duration::from_millis(self.query_timeout_ms)
    }

    /// 获取连接超时 Duration
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }

    /// 获取请求超时 Duration
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    /// 验证配置
    pub fn validate(&self) -> CoreResult<()> {
        if self.query_timeout_ms == 0 {
            return Err(crate::utils::error::CoreError::InvalidParam(
                "query_timeout_ms must be > 0".to_string(),
            ));
        }

        if self.shuffle_buffer_size == 0 {
            return Err(crate::utils::error::CoreError::InvalidParam(
                "shuffle_buffer_size must be > 0".to_string(),
            ));
        }

        if self.max_concurrent_queries == 0 {
            return Err(crate::utils::error::CoreError::InvalidParam(
                "max_concurrent_queries must be > 0".to_string(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DistributedConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.query_timeout_ms, 30_000);
        assert_eq!(config.shuffle_buffer_size, 10_000);
        assert_eq!(config.max_concurrent_queries, 100);
        assert_eq!(config.rpc_port, 7947);
    }

    #[test]
    fn test_from_toml() {
        let toml = r#"
[distributed]
enabled = true
query_timeout_ms = 60000
shuffle_buffer_size = 20000
max_concurrent_queries = 200
rpc_port = 8080
"#;
        let config = DistributedConfig::from_toml(toml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.query_timeout_ms, 60_000);
        assert_eq!(config.shuffle_buffer_size, 20_000);
        assert_eq!(config.max_concurrent_queries, 200);
        assert_eq!(config.rpc_port, 8080);
    }

    #[test]
    fn test_validate() {
        let mut config = DistributedConfig::default();
        assert!(config.validate().is_ok());

        config.query_timeout_ms = 0;
        assert!(config.validate().is_err());
    }
}
