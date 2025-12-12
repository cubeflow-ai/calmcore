use std::sync::Arc;

use datafusion::arrow::compute::kernels::partition;

use crate::catalog::{dir, PartitionStrategy, TableMeta};
use crate::partition::Partition;
use crate::schema::Schema;
use crate::utils::error::{CoreError, CoreResult};

use super::Engine;

// ⚠️ 这个文件中的方法已经移到 CalmService/DDLService
// Engine 不再负责协调操作，只负责本地数据操作
// 保留这个文件是为了记录历史，后续可以删除

impl Engine {
    // ❌ 删除：create_table - 这是协调操作，应该在 DDLService 中
    // ❌ 删除：drop_table - 这是协调操作，应该在 DDLService 中
}
