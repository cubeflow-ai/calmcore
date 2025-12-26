//! Arrow Flight Action 定义（控制面协议）

use serde::{Deserialize, Serialize};
use crate::{
    catalog::table_meta::PartitionStrategy,
    schema::Schema,
    utils::error::{CoreError, CoreResult},
};

/// Flight Action 类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlightAction {
    // ==================== 表管理 ====================
    CreateTable {
        schema: Schema,
        partition_strategy: PartitionStrategy,
    },
    DropTable {
        table_name: String,
    },
    ListTables,
    GetTableDetail {
        table_name: String,
    },
    FlushTable {
        table_name: String,
    },

    // ==================== 分区管理 ====================
    CreatePartition {
        table_name: String,
        partition_name: String,
    },
    DropPartition {
        table_name: String,
        partition_name: String,
    },
    LoadPartition {
        table_name: String,
        partition_name: String,
    },
    FlushPartition {
        table_name: String,
        partition_name: String,
    },
    ListPartitions {
        table_name: String,
    },
    ListAllPartitions,

    // ==================== 查询 ====================
    IsCoordinator,
}

/// Flight Action 响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlightActionResponse {
    Ok,
    TableList(Vec<String>),
    PartitionList(Vec<String>),
    AllPartitions(Vec<(String, String)>),
    TableDetail(crate::calm::TableDetail),
    Bool(bool),
    Error(String),
}

impl FlightAction {
    /// 将 Action 序列化为 Flight Action
    pub fn to_flight_action(&self) -> CoreResult<arrow_flight::Action> {
        let json = serde_json::to_string(self)
            .map_err(|e| CoreError::Internal(format!("Failed to serialize action: {}", e)))?;
        
        Ok(arrow_flight::Action {
            r#type: "calm_rpc".to_string(),
            body: json.into_bytes().into(),
        })
    }

    /// 从 Flight Action 反序列化
    pub fn from_flight_action(action: &arrow_flight::Action) -> CoreResult<Self> {
        let json = String::from_utf8(action.body.to_vec())
            .map_err(|e| CoreError::Internal(format!("Invalid UTF-8 in action body: {}", e)))?;
        
        serde_json::from_str(&json)
            .map_err(|e| CoreError::Internal(format!("Failed to deserialize action: {}", e)))
    }
}

impl FlightActionResponse {
    /// 序列化为 Flight Result
    pub fn to_flight_result(&self) -> CoreResult<arrow_flight::Result> {
        let json = serde_json::to_string(self)
            .map_err(|e| CoreError::Internal(format!("Failed to serialize response: {}", e)))?;
        
        Ok(arrow_flight::Result {
            body: json.into_bytes().into(),
        })
    }

    /// 从 Flight Result 反序列化
    pub fn from_flight_result(result: &arrow_flight::Result) -> CoreResult<Self> {
        let json = String::from_utf8(result.body.to_vec())
            .map_err(|e| CoreError::Internal(format!("Invalid UTF-8 in result body: {}", e)))?;
        
        serde_json::from_str(&json)
            .map_err(|e| CoreError::Internal(format!("Failed to deserialize response: {}", e)))
    }
}
