//! TaskEstimator implementation for CalmCore
//!
//! 基于分区元数据估算分布式查询任务数

use datafusion::common::config::ConfigOptions;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{TaskCountAnnotation, TaskEstimation, TaskEstimator};
use std::sync::Arc;

use crate::catalog::Catalog;

/// CalmCore 的 TaskEstimator 实现
///
/// 职责：
/// - 分析查询计划涉及的分区
/// - 根据分区数估算并行任务数
/// - 考虑分区过滤条件
pub struct PartitionAwareTaskEstimator {
    catalog: Arc<Catalog>,
    /// 默认任务数（当无法从计划中提取时）
    default_tasks: usize,
}

impl PartitionAwareTaskEstimator {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            catalog,
            default_tasks: 4, // 默认 4 个并行任务
        }
    }

    /// 从执行计划中提取表名（如果可能）
    fn extract_table_name(&self, _plan: &Arc<dyn ExecutionPlan>) -> Option<String> {
        // TODO: 实现从计划中提取表名的逻辑
        // 可以通过遍历 plan.children() 寻找 TableScan 节点
        None
    }

    /// 估算给定表的分区数
    fn estimate_partition_count(&self, table_name: &str) -> usize {
        // 尝试从 catalog 获取分区数
        // 注意：这里需要异步操作，但 TaskEstimator trait 是同步的
        // 在初期阶段，我们使用简单的启发式

        // TODO: 考虑使用缓存的分区元数据
        log::debug!(
            "📊 [TaskEstimator] Estimating tasks for table: {}",
            table_name
        );

        // 暂时返回默认值
        self.default_tasks
    }
}

impl TaskEstimator for PartitionAwareTaskEstimator {
    fn tasks_for_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        // 尝试提取表名
        if let Some(table_name) = self.extract_table_name(plan) {
            let task_count = self.estimate_partition_count(&table_name);
            log::info!(
                "📊 [TaskEstimator] Estimated {} tasks for table '{}'",
                task_count,
                table_name
            );
            Some(TaskEstimation {
                task_count: TaskCountAnnotation::Desired(task_count),
            })
        } else {
            // 无法提取表名，返回 None 让其他 estimator 处理
            // 或使用默认值
            log::debug!(
                "📊 [TaskEstimator] Using default task estimation: {}",
                self.default_tasks
            );
            Some(TaskEstimation {
                task_count: TaskCountAnnotation::Desired(self.default_tasks),
            })
        }
    }

    fn scale_up_leaf_node(
        &self,
        _plan: &Arc<dyn ExecutionPlan>,
        _task_count: usize,
        _cfg: &ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        // 暂不需要转换计划节点
        // 返回 None 表示不修改
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use std::sync::Arc;

    #[test]
    fn test_default_estimation() {
        let catalog = Arc::new(Catalog::new());
        let estimator = PartitionAwareTaskEstimator::new(catalog);

        assert_eq!(estimator.default_tasks, 4);
    }
}
