pub(crate) mod dir;
/// Catalog 模块 - 管理表的元数据和目录结构
pub mod schema;
pub mod table_meta;

pub use table_meta::{
    PartitionMeta, PartitionStrategy, SegmentInfo, SegmentStatus, TableMeta, TimeGranularity,
};

use crate::utils::error::{CoreError, CoreResult};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// Catalog - 管理所有表的元数据
pub struct Catalog {
    /// 工作目录
    work_dir: PathBuf,

    /// 表元数据缓存 (table_name -> TableMeta)
    tables: RwLock<HashMap<String, Arc<TableInfo>>>,
}

pub struct PartitionInfo {
    pub partition: PartitionMeta,
    pub addr: Option<String>,
}

pub struct TableInfo {
    pub table: TableMeta,
    pub partitions: RwLock<Vec<PartitionInfo>>,
}

impl Catalog {
    /// 创建新的 Catalog
    pub async fn new(work_dir: PathBuf) -> CoreResult<Self> {
        // 确保工作目录存在
        let tables_dir = work_dir.join("tables");
        fs::create_dir_all(&tables_dir)
            .map_err(|e| CoreError::IOError(format!("Failed to create tables directory: {}", e)))?;

        let catalog = Self {
            work_dir,
            tables: RwLock::new(HashMap::new()),
        };

        catalog.init().await?;

        Ok(catalog)
    }

    /// =========================================== table operations ===========================================

    async fn init(&self) -> CoreResult<()> {
        let tables_dir = self.work_dir.join("tables");

        if !tables_dir.exists() {
            return Ok(());
        }

        let entries = fs::read_dir(&tables_dir)
            .map_err(|e| CoreError::IOError(format!("Failed to read tables directory: {}", e)))?;

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    log::error!("⚠️  Failed to read directory entry: {}", e);
                    continue;
                }
            };
            let path = entry.path();
            if path.is_dir() {
                let table_name = match path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
                    CoreError::IOError(format!("Invalid table directory name: {:?}", path))
                }) {
                    Ok(name) => name,
                    Err(e) => {
                        log::error!("⚠️ load table name by path:{:?} error: {}", path, e);
                        continue;
                    }
                };

                match self.load_table(table_name).await {
                    Ok(table_info) => {
                        let table_name = table_info.table.table_name.clone();
                        let mut tables = self.tables.write().unwrap();
                        tables.insert(table_name.clone(), Arc::new(table_info));
                        log::info!("✅ Loaded table '{}'", table_name);
                    }
                    Err(e) => {
                        log::error!("⚠️  Failed to load table from {:?}: {}", path, e);
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn load_table(&self, table_name: &str) -> CoreResult<TableInfo> {
        let path = self.work_dir.join("tables").join(table_name);
        let table = crate::utils::json::load_json_from_file::<TableMeta>(&path.join("meta.json"))
            .map_err(|e| CoreError::IOError(format!("Failed to load table meta: {}", e)))?;

        let partitions = self.load_partitions(table_name).await?;

        log::info!(
            "✅ Loaded table '{}' with {} partitions",
            table.table_name,
            partitions.len()
        );

        Ok(TableInfo { table, partitions })
    }

    pub fn partition_dir(&self, table_name: &str, partition_name: &str) -> PathBuf {
        self.work_dir
            .join("tables")
            .join(table_name)
            .join("partitions")
            .join(partition_name)
    }

    pub async fn load_partitions(&self, table_name: &str) -> CoreResult<Vec<PartitionInfo>> {
        let table_dir = self.work_dir.join("tables").join(table_name);
        let partitions_dir = table_dir.join("partitions");

        // 统一扫描 partitions 子目录，不依赖分区策略
        log::info!(
            "🔍 Scanning partition directories for path: {:?}...",
            partitions_dir
        );

        let mut partitions = Vec::new();

        if !partitions_dir.exists() {
            return Ok(partitions);
        }

        let entries = std::fs::read_dir(&partitions_dir).map_err(|e| {
            CoreError::IOError(format!("Failed to read partitions directory: {}", e))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                CoreError::IOError(format!("Failed to read partition entry: {}", e))
            })?;

            let path = entry.path();

            if !path.is_dir() {
                continue;
            }

            let partition_meta =
                crate::utils::json::load_json_from_file::<PartitionMeta>(&path.join("meta.json"))?;
            partitions.push(PartitionInfo {
                partition: partition_meta,
                addr: None,
            });
        }

        Ok(partitions)
    }

    /// 创建新表
    pub fn create_table(&self, meta: TableMeta) -> CoreResult<Vec<String>> {
        let table_name = meta.table_name.clone();

        // 检查表是否已存在
        {
            let tables = self.tables.read().unwrap();
            if tables.contains_key(&table_name) {
                return Err(CoreError::InvalidParam(format!(
                    "Table '{}' already exists",
                    table_name
                )));
            }
        }

        fs::create_dir_all(self.work_dir.join("tables").join(&table_name)).map_err(|e| {
            CoreError::IOError(format!(
                "Failed to create table directory '{}': {}",
                table_name, e
            ))
        })?;

        // 保存表元数据
        self.save_table_meta(&meta)?;

        let partitions = meta
            .partition_strategy
            .generate_partitions()
            .unwrap_or_else(Vec::new);

        for partition_name in &partitions {
            // 创建 partition 目录和元数据
            self.create_partition(&table_name, partition_name)?;
        }

        // 添加到缓存
        {
            let mut tables = self.tables.write().unwrap();
            tables.insert(table_name.clone(), Arc::new(meta));
        }

        println!("✅ Table '{}' created successfully", table_name);
        Ok(partitions)
    }

    /// 获取表元数据
    pub async fn get_or_load_table(&self, table_name: &str) -> CoreResult<Arc<TableInfo>> {
        let tables = self.tables.read().unwrap();

        if let Some(table) = tables.get(table_name) {
            return Ok(table.clone());
        }
        drop(tables);

        let tables = self.tables.write().unwrap();

        if let Some(table) = tables.get(table_name) {
            return Ok(table.clone());
        }

        let table = Arc::new(self.load_table(table_name).await?);
        {
            let mut tables = self.tables.write().unwrap();
            tables.insert(table_name.to_string(), table.clone());
        }
        Ok(table)
    }

    /// 列出所有表
    pub fn list_tables(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables.keys().cloned().collect()
    }

    /// 删除表
    pub fn drop_table(&self, table_name: &str) -> CoreResult<()> {
        // 从缓存中移除
        let meta = {
            let mut tables = self.tables.write().unwrap();
            tables
                .remove(table_name)
                .ok_or_else(|| CoreError::NotExisted(format!("Table '{}' not found", table_name)))?
        };

        // 删除表目录
        let table_dir = dir::table_dir(&self.work_dir, table_name);
        if table_dir.exists() {
            fs::remove_dir_all(&table_dir).map_err(|e| {
                CoreError::IOError(format!("Failed to remove table directory: {}", e))
            })?;
        }

        println!("✅ Table '{}' dropped successfully", table_name);
        Ok(())
    }

    /// =========================================== partiton operations ===========================================

    /// 创建 partition 目录和元数据
    pub fn create_partition(&self, table_name: &str, partition_name: &str) -> CoreResult<()> {
        // partition 目录直接在 table_dir 下，不需要额外的 segments 子目录
        let partition_dir = dir::partition_dir(&self.work_dir, table_name, partition_name);

        // 创建 partition 目录
        fs::create_dir_all(&partition_dir).map_err(|e| {
            CoreError::IOError(format!(
                "Failed to create partition directory '{}': {}",
                partition_dir.display(),
                e
            ))
        })?;

        // 创建 partition 元数据
        let partition_meta = PartitionMeta::new(partition_name.to_string());
        let meta_path = partition_dir.join("meta.json");
        let content = serde_json::to_string_pretty(&partition_meta).map_err(|e| {
            CoreError::IOError(format!("Failed to serialize partition meta: {}", e))
        })?;

        fs::write(&meta_path, content).map_err(|e| {
            CoreError::IOError(format!(
                "Failed to write partition meta to '{}': {}",
                meta_path.display(),
                e
            ))
        })?;

        log::info!(
            "Created partition directory: {} (name: {})",
            partition_dir.display(),
            partition_name
        );
        Ok(())
    }

    /// 获取 partition 元数据（通过 partition_name）
    pub fn get_partition_meta(
        &self,
        table_name: &str,
        partition_name: &str,
    ) -> CoreResult<PartitionMeta> {
        let partition_meta_path =
            dir::partition_dir(&self.work_dir, table_name, partition_name).join("meta.json");

        if !partition_meta_path.exists() {
            return Err(CoreError::NotExisted(format!(
                "Partition {} meta not found for table '{}'",
                partition_name, table_name
            )));
        }

        self.load_json_from_file(&partition_meta_path)
    }

    /// 保存 partition 元数据
    pub fn save_partition_meta(&self, table_name: &str, meta: &PartitionMeta) -> CoreResult<()> {
        let partition_meta_path =
            dir::partition_dir(&self.work_dir, table_name, &meta.partition_name).join("meta.json");

        let content: String = serde_json::to_string_pretty(meta).map_err(|e| {
            CoreError::IOError(format!("Failed to serialize partition meta: {}", e))
        })?;

        fs::write(&partition_meta_path, content)
            .map_err(|e| CoreError::IOError(format!("Failed to write partition meta: {}", e)))?;

        Ok(())
    }

    // ========== 私有方法 ==========

    /// 保存表元数据到文件
    fn save_table_meta(&self, meta: &TableMeta) -> CoreResult<()> {
        let meta_path = dir::table_dir(&self.work_dir, &meta.table_name).join("meta.json");
        let content = serde_json::to_string_pretty(meta)
            .map_err(|e| CoreError::IOError(format!("Failed to serialize table meta: {}", e)))?;

        fs::write(&meta_path, content).map_err(|e| {
            log::error!(
                "Saved table meta for '{}': {}",
                meta.table_name,
                meta_path.display()
            );
            CoreError::IOError(format!("Failed to write table meta: {}", e))
        })?;

        Ok(())
    }
}

// ===== 分区路由功能 =====
impl Catalog {
    /// 设置分区所属节点
    ///
    /// 用于分布式环境下记录每个分区当前归属的节点
    pub fn set_partition_owner(
        &self,
        table_name: &str,
        partition_name: &str,
        node_id: &str,
    ) -> CoreResult<()> {
        let mut routes = self.partition_routes.write().unwrap();
        routes.insert(
            (table_name.to_string(), partition_name.to_string()),
            node_id.to_string(),
        );
        log::debug!(
            "📍 Partition route set: {}:{} -> {}",
            table_name,
            partition_name,
            node_id
        );
        Ok(())
    }

    /// 获取分区所属节点
    ///
    /// 返回 None 表示分区尚未分配节点
    pub fn get_partition_owner(&self, table_name: &str, partition_name: &str) -> Option<String> {
        let routes = self.partition_routes.read().unwrap();
        routes
            .get(&(table_name.to_string(), partition_name.to_string()))
            .cloned()
    }

    /// 获取某节点拥有的所有分区
    ///
    /// 返回 Vec<(table_name, partition_name)>
    pub fn get_partitions_owned_by(&self, node_id: &str) -> Vec<(String, String)> {
        let routes = self.partition_routes.read().unwrap();
        routes
            .iter()
            .filter(|(_, owner)| owner.as_str() == node_id)
            .map(|((table, partition), _)| (table.clone(), partition.clone()))
            .collect()
    }

    /// 清除节点的所有分区映射
    ///
    /// 用于节点离开或故障时清理路由表
    pub fn clear_node_partitions(&self, node_id: &str) {
        let mut routes = self.partition_routes.write().unwrap();
        routes.retain(|_, owner| owner.as_str() != node_id);
        log::info!("🧹 Cleared all partition routes for node: {}", node_id);
    }

    /// 获取表的所有分区名称
    ///
    /// 从分区策略生成分区列表
    pub fn get_partition_names(&self, table_name: &str) -> CoreResult<Vec<String>> {
        Ok(self
            .partition_routes
            .read()
            .unwrap()
            .keys()
            .filter(|(t_name, _)| t_name == table_name)
            .map(|(_, p_name)| p_name.clone())
            .collect())
    }
}
