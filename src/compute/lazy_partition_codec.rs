//! PhysicalExtensionCodec for LazyPartitionExec
//!
//! 实现 LazyPartitionExec 的 protobuf 序列化/反序列化

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use prost::Message;

use super::lazy_partition_exec::LazyPartitionExec;

/// LazyPartitionExec 的 protobuf 表示
#[derive(Clone, PartialEq, Message)]
pub struct LazyPartitionExecProto {
    /// Table name
    #[prost(string, tag = "1")]
    pub table_name: String,

    /// Partition names
    #[prost(string, repeated, tag = "2")]
    pub partition_names: Vec<String>,

    /// Schema encoded as Arrow IPC
    #[prost(bytes = "vec", tag = "3")]
    pub schema: Vec<u8>,

    /// Projection columns (if any)
    #[prost(uint64, repeated, tag = "4")]
    pub projection: Vec<u64>,

    /// Limit (if any)
    #[prost(uint64, optional, tag = "5")]
    pub limit: Option<u64>,

    /// Filters encoded as protobuf (we'll skip this for now, or encode as JSON)
    #[prost(bytes = "vec", tag = "6")]
    pub filters: Vec<u8>,

    /// Partition owners: key=partition_name, value=owner_node_id
    #[prost(map = "string, string", tag = "7")]
    pub partition_owners: std::collections::HashMap<String, String>,
}

/// Codec for LazyPartitionExec
#[derive(Debug, Clone, Default)]
pub struct LazyPartitionCodec;

impl PhysicalExtensionCodec for LazyPartitionCodec {
    // 📌 datafusion 51.0.0 的签名：name 是 &[u8]，实际包含完整的 protobuf 数据
    fn try_decode(
        &self,
        name: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _context: &TaskContext,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // 🎯 关键：name 参数实际上包含了完整的 protobuf 编码数据！
        // 直接解码，不要检查名字
        log::debug!("🔍 [LazyPartitionCodec] Decoding {} bytes", name.len());

        let proto = LazyPartitionExecProto::decode(name).map_err(|e| {
            log::error!("❌ [LazyPartitionCodec] Failed to decode: {}", e);
            DataFusionError::Internal(format!("Failed to decode LazyPartitionExec: {}", e))
        })?;

        log::info!(
            "✅ [LazyPartitionCodec] Decoded: table={}, partitions={:?}",
            proto.table_name,
            proto.partition_names
        );

        // 解码 schema
        use datafusion::arrow::ipc::reader::StreamReader;
        let reader = StreamReader::try_new(std::io::Cursor::new(proto.schema), None)
            .map_err(|e| DataFusionError::Internal(format!("Failed to decode schema: {}", e)))?;
        let schema = reader.schema();

        log::info!(
            "🔍 [LazyPartitionCodec] Decoded base_schema with {} fields: {:?}, projection: {:?}",
            schema.fields().len(),
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
            proto.projection
        );

        // 解码 filters（如果有）
        let filters = if proto.filters.is_empty() {
            vec![]
        } else {
            // TODO: 实现 filter 的序列化/反序列化
            // 现在先用空的
            vec![]
        };

        // Projection: 如果为空则 None，否则转换为 Some(Vec<usize>)
        let projection = if proto.projection.is_empty() {
            None
        } else {
            Some(proto.projection.iter().map(|&i| i as usize).collect())
        };

        // 注意：这里我们无法获取 Engine，因为它不在 protobuf 中
        // 我们需要从执行上下文中获取
        // 现在先返回一个错误，等待实现 execute 时从 context 获取

        // 创建 LazyPartitionExec（没有 engine）
        let exec = LazyPartitionExec::new_without_engine(
            proto.table_name,
            proto.partition_names,
            proto.partition_owners,
            schema,
            filters,
            projection,
            proto.limit.map(|l| l as usize),
        );

        Ok(Arc::new(exec))
    }

    // 📌 datafusion 51.0.0 的签名：返回类型是 Result<(), DataFusionError>
    // name 通过 buf 返回
    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DataFusionResult<()> {
        let lazy_exec = node
            .as_any()
            .downcast_ref::<LazyPartitionExec>()
            .ok_or_else(|| DataFusionError::Internal("Not a LazyPartitionExec".to_string()))?;

        log::info!(
            "🔄 [LazyPartitionCodec] Encoding: table={}, partitions={:?}",
            lazy_exec.table_name(),
            lazy_exec.partition_names()
        );

        // 🔧 FIX: 编码 base_schema（完整的表 schema），而不是 output_schema
        // 这样远程节点可以正确地重新应用 projection
        use datafusion::arrow::ipc::writer::StreamWriter;
        let mut schema_buf = Vec::new();
        {
            // 使用 base_schema
            let base_schema = lazy_exec.base_schema();
            let mut writer = StreamWriter::try_new(&mut schema_buf, &base_schema).map_err(|e| {
                DataFusionError::Internal(format!("Failed to encode schema: {}", e))
            })?;
            writer.finish().map_err(|e| {
                DataFusionError::Internal(format!("Failed to finish schema encoding: {}", e))
            })?;

            log::info!(
                "🔍 [LazyPartitionCodec] Encoded base_schema with {} fields: {:?}",
                base_schema.fields().len(),
                base_schema
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            );
        }

        // 编码 filters（暂时跳过）
        let filters_buf = Vec::new();

        // Projection: 如果是 None 则空 vec，否则转换
        let projection_vec = match lazy_exec.projection() {
            None => vec![],
            Some(proj) => proj.iter().map(|&i| i as u64).collect(),
        };

        let proto = LazyPartitionExecProto {
            table_name: lazy_exec.table_name().to_string(),
            partition_names: lazy_exec.partition_names().to_vec(),
            partition_owners: lazy_exec.partition_owners().clone(),
            schema: schema_buf,
            projection: projection_vec,
            limit: lazy_exec.limit().map(|l| l as u64),
            filters: filters_buf,
        };

        proto.encode(buf).map_err(|e| {
            DataFusionError::Internal(format!("Failed to encode LazyPartitionExec: {}", e))
        })?;

        log::info!("✅ [LazyPartitionCodec] Encoded {} bytes", buf.len());

        // 📌 在 datafusion 51.0.0 中，需要将 name 写入 buf 的开头
        // 但 datafusion-distributed 会处理这个，我们只需要返回 Ok(())
        Ok(())
    }
}
