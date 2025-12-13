use std::sync::Arc;

use crate::utils::error::CoreResult;

pub async fn start_cluster_job(service: Arc<super::CalmService>) -> CoreResult<()> {
    // 如果所有节点都选举自己为中央节点，则自身变为中央节点

    // 同步partition路由

    // 如果过自身为中央节点，则需要额外验证所有的partition 都有节点托管

    // 2.加载所有tables

    if match clone() {
        Some(cm) => {
            let tables = service.catalog.list_tables().await?;
            for table in tables {
                cm.load_table_routes(&table).await?;
            }
        }
        None => {}
    }

    match service.cluster_manager.clone() {
        Some(cm) => {}
        None => {}
    }

    // 1. 获取中央结点。
    if let Some(cm) = service.cluster_manager {
        let coor = cm.node_manager.run_election().await?;
        // 1.如果中央结点是自己，
        // 3.加载全部路由
        // 4.去中央节点询问是否是中央节点，等中央节点确认后，再设置状态为 Ready
        // 4.将当前节点状态设置为 Ready
        cm.set_my_status_ready().await;
    } else {
        return Ok(());
    }
}
