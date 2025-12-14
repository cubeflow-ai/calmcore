use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum CoreError {
    #[error("ok")]
    Ok(u64),

    #[error("{0}")]
    Internal(String),

    #[error("{0} duplicated.")]
    Duplicated(String),

    #[error("{0} not existed.")]
    NotExisted(String),

    #[error("source store has err:'{0}'")]
    IOError(String),

    #[error("decode error:'{0}' data:{1:?}")]
    DecodeError(String, Vec<u8>),

    #[error("decode error:'{0}'")]
    EcodeError(String),

    #[error("no support:{0}")]
    Notsupport(String),

    #[error("invalid param err:{0}")]
    InvalidParam(String),

    #[error("{0} existed.")]
    Existed(String),

    #[error("network error: {0}")]
    Network(String),

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("{0}")]
    ConfigError(String),

    #[error("{0}")]
    ClusterState(String),
}

impl CoreError {
    pub fn code(&self) -> i32 {
        match self {
            CoreError::Ok(_) => 0,
            CoreError::Internal(_) => 1,
            CoreError::Duplicated(_) => 2,
            CoreError::NotExisted(_) => 3,
            CoreError::IOError(_) => 4,
            CoreError::DecodeError(_, _) => 5,
            CoreError::Notsupport(_) => 6,
            CoreError::InvalidParam(_) => 7,
            CoreError::Existed(_) => 8,
            CoreError::EcodeError(_) => 9,
            CoreError::Network(_) => 10,
            CoreError::Timeout(_) => 11,
            CoreError::ConfigError(_) => 12,
            CoreError::ClusterState(_) => 13,
        }
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, CoreError::Ok(_))
    }
}

// DataFusion 错误转换
impl From<datafusion::error::DataFusionError> for CoreError {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        CoreError::Internal(format!("DataFusion error: {}", err))
    }
}

impl From<tarpc::client::RpcError> for CoreError {
    fn from(err: tarpc::client::RpcError) -> Self {
        CoreError::Network(format!("RPC error: {}", err))
    }
}

impl From<serde_json::Error> for CoreError {
    fn from(err: serde_json::Error) -> Self {
        CoreError::DecodeError(format!("Serde JSON error: {}", err), vec![])
    }
}

impl From<std::io::Error> for CoreError {
    fn from(err: std::io::Error) -> Self {
        CoreError::IOError(format!("IO error: {}", err))
    }
}
