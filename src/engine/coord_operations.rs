use std::sync::Arc;

use datafusion::arrow::compute::kernels::partition;

use crate::catalog::{dir, PartitionStrategy, TableMeta};
use crate::partition::Partition;
use crate::schema::Schema;
use crate::utils::error::{CoreError, CoreResult};

use super::Engine;

impl Engine {
    /// 创建新表
    ///
    /// # 参数
    /// - `table_name`: 表名
    /// - `schema`: 表的 Schema
    /// - `partition_strategy`: 分区策略
    /// - `num_partitions`: 分区数量
    pub async fn create_table(
        &self,
        table_name: &str,
        schema: Schema,
        partition_strategy: PartitionStrategy,
        num_partitions: usize,
    ) -> CoreResult<()> {
        // 创建 TableMeta
        let meta = TableMeta::new(table_name.to_string(), schema.clone(), partition_strategy);

        // 在 Catalog 中创建表（会创建目录结构和元数据）
        let partitions = self.catalog.create_table(meta)?;

        // 加载所有 partition 到内存
        log::debug!(
            "🔍 [DEBUG create_table] Creating {} partitions for table '{}' partitions:'{:?}'",
            num_partitions,
            table_name,
            partitions
        );

        // 通知集群中的其他节点创建对应的 partition
        for partition_name in &partitions {
            self.cluster_manager
                .call_create_partition(table_name, partition_name)
                .await?;
        }

        log::debug!(
            "✅ Table '{}' created with {} partitions",
            table_name,
            num_partitions
        );
        Ok(())
    }

    /// 删除表
    pub async fn drop_table(&self, table_name: &str) -> CoreResult<()> {
        // 删除远端标
        self.cluster_manager.call_drop_table(table_name).await?;

        // 从 catalog 中删除
        self.catalog.drop_table(table_name)?;
        Ok(())
    }
}
