//! datafusion-distributed 集成测试

#[cfg(test)]
mod tests {
    use crate::catalog::Catalog;
    use crate::compute::{
        CalmChannelResolver, DistributedDataFusionExecutor, PartitionAwareTaskEstimator,
    };
    use crate::engine::Engine;
    use datafusion::prelude::*;
    use datafusion_distributed::DistributedPhysicalOptimizerRule;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_distributed_context_creation() {
        // 创建基础组件
        let engine = Arc::new(Engine::new("test_data".to_string()));
        let catalog = Arc::new(Catalog::new());

        // 创建单机模式的 ChannelResolver
        let resolver = CalmChannelResolver::new_local(vec![8001, 8002, 8003]);

        // 验证 URLs
        let urls = resolver.get_urls().unwrap();
        assert_eq!(urls.len(), 3);
        println!("✅ ChannelResolver URLs: {:?}", urls);

        // 创建 TaskEstimator
        let task_estimator = Arc::new(PartitionAwareTaskEstimator::new(catalog.clone()));

        // 创建分布式优化规则
        let distributed_rule = Arc::new(DistributedPhysicalOptimizerRule::new(task_estimator));

        // 构建 SessionState
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(distributed_rule)
            .build();

        // 创建 SessionContext
        let ctx = SessionContext::new_with_state(state);

        println!("✅ 分布式 SessionContext 创建成功");

        // 简单查询测试
        let sql = "SELECT 1 as test";
        let df = ctx.sql(sql).await.expect("SQL parse failed");
        let result = df.collect().await.expect("Query execution failed");

        assert_eq!(result.len(), 1);
        println!("✅ 基本查询执行成功: {} rows", result[0].num_rows());
    }

    #[test]
    fn test_channel_resolver_creation() {
        // 测试本地模式
        let local_resolver = CalmChannelResolver::new_local(vec![8001, 8002]);
        let urls = local_resolver.get_urls().unwrap();

        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0].as_str(), "http://localhost:8001/");
        assert_eq!(urls[1].as_str(), "http://localhost:8002/");

        println!("✅ Local ChannelResolver 测试通过");
    }

    #[test]
    fn test_task_estimator_creation() {
        let catalog = Arc::new(Catalog::new());
        let estimator = PartitionAwareTaskEstimator::new(catalog);

        // TaskEstimator 创建成功
        println!("✅ TaskEstimator 创建成功");
    }
}
