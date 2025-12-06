/// Catalog 模块 - 管理表的元数据和目录结构
pub mod schema;
pub mod table_meta;

pub use table_meta::{
    PartitionMeta, PartitionStrategy, SegmentInfo, SegmentStatus, TableMeta, TimeGranularity,
};

use crate::utils::error::{CoreError, CoreResult};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// Catalog - 管理所有表的元数据
pub struct Catalog {
    /// 工作目录
    work_dir: PathBuf,

    /// 表元数据缓存 (table_name -> TableMeta)
    tables: Arc<RwLock<HashMap<String, Arc<TableMeta>>>>,
}

impl Catalog {
    /// 创建新的 Catalog
    pub fn new(work_dir: PathBuf) -> CoreResult<Self> {
        // 确保工作目录存在
        let tables_dir = work_dir.join("tables");
        fs::create_dir_all(&tables_dir)
            .map_err(|e| CoreError::IOError(format!("Failed to create tables directory: {}", e)))?;

        let catalog = Self {
            work_dir,
            tables: Arc::new(RwLock::new(HashMap::new())),
        };

        // 加载已存在的表
        catalog.load_existing_tables()?;

        Ok(catalog)
    }

    /// 创建新表
    pub fn create_table(&self, meta: TableMeta) -> CoreResult<()> {
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

        // 创建表目录结构
        self.create_table_directories(&meta)?;

        // 保存表元数据
        self.save_table_meta(&meta)?;

        // 创建 partition 目录和元数据
        self.create_partition(&meta)?;

        // 添加到缓存
        {
            let mut tables = self.tables.write().unwrap();
            tables.insert(table_name.clone(), Arc::new(meta));
        }

        println!("✅ Table '{}' created successfully", table_name);
        Ok(())
    }

    /// 获取表元数据
    pub fn get_table(&self, table_name: &str) -> CoreResult<Arc<TableMeta>> {
        let tables = self.tables.read().unwrap();
        tables
            .get(table_name)
            .cloned()
            .ok_or_else(|| CoreError::NotExisted(format!("Table '{}' not found", table_name)))
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
        let table_dir = meta.table_dir(&self.work_dir);
        if table_dir.exists() {
            fs::remove_dir_all(&table_dir).map_err(|e| {
                CoreError::IOError(format!("Failed to remove table directory: {}", e))
            })?;
        }

        println!("✅ Table '{}' dropped successfully", table_name);
        Ok(())
    }

    /// 获取 partition 元数据（通过 partition_name）
    pub fn get_partition_meta(
        &self,
        table_name: &str,
        partition_name: &str,
    ) -> CoreResult<PartitionMeta> {
        let table = self.get_table(table_name)?;
        let partition_meta_path = table
            .partition_dir_by_name(&self.work_dir, partition_name)
            .join("meta.json");

        if !partition_meta_path.exists() {
            return Err(CoreError::NotExisted(format!(
                "Partition {} meta not found for table '{}'",
                partition_name, table_name
            )));
        }

        let content = fs::read_to_string(&partition_meta_path)
            .map_err(|e| CoreError::IOError(format!("Failed to read partition meta: {}", e)))?;

        let meta: PartitionMeta = serde_json::from_str(&content)
            .map_err(|e| CoreError::IOError(format!("Failed to parse partition meta: {}", e)))?;

        Ok(meta)
    }

    /// 保存 partition 元数据
    pub fn save_partition_meta(&self, table_name: &str, meta: &PartitionMeta) -> CoreResult<()> {
        let table = self.get_table(table_name)?;
        let partition_meta_path = table
            .partition_dir_by_name(&self.work_dir, &meta.partition_name)
            .join("meta.json");

        let content = serde_json::to_string_pretty(meta).map_err(|e| {
            CoreError::IOError(format!("Failed to serialize partition meta: {}", e))
        })?;

        fs::write(&partition_meta_path, content)
            .map_err(|e| CoreError::IOError(format!("Failed to write partition meta: {}", e)))?;

        Ok(())
    }

    // ========== 私有方法 ==========

    /// 创建表目录结构
    fn create_table_directories(&self, meta: &TableMeta) -> CoreResult<()> {
        let table_dir = meta.table_dir(&self.work_dir);
        let partitions_dir = table_dir.join("partitions");

        fs::create_dir_all(&partitions_dir).map_err(|e| {
            CoreError::IOError(format!("Failed to create table directories: {}", e))
        })?;

        Ok(())
    }

    /// 保存表元数据到文件
    fn save_table_meta(&self, meta: &TableMeta) -> CoreResult<()> {
        let meta_path = meta.table_dir(&self.work_dir).join("meta.json");
        let content = serde_json::to_string_pretty(meta)
            .map_err(|e| CoreError::IOError(format!("Failed to serialize table meta: {}", e)))?;

        fs::write(&meta_path, content)
            .map_err(|e| CoreError::IOError(format!("Failed to write table meta: {}", e)))?;

        Ok(())
    }

    /// 创建 partition 目录和元数据
    fn create_partition(&self, table_meta: &TableMeta) -> CoreResult<()> {
        let partitions = table_meta
            .partition_strategy
            .generate_partitions()
            .unwrap_or_else(Vec::new);

        for partition in partitions {
            // partition 目录直接在 table_dir 下，不需要额外的 segments 子目录
            let partition_dir = table_meta.partition_dir_by_name(&self.work_dir, &partition);

            // 创建 partition 目录
            fs::create_dir_all(&partition_dir).map_err(|e| {
                CoreError::IOError(format!(
                    "Failed to create partition directory '{}': {}",
                    partition_dir.display(),
                    e
                ))
            })?;

            // 创建 partition 元数据
            let partition_meta = PartitionMeta::new(partition.clone());
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
                partition
            );
        }
        Ok(())
    }

    /// 加载已存在的表
    fn load_existing_tables(&self) -> CoreResult<()> {
        let tables_dir = self.work_dir.join("tables");

        if !tables_dir.exists() {
            return Ok(());
        }

        let entries = fs::read_dir(&tables_dir)
            .map_err(|e| CoreError::IOError(format!("Failed to read tables directory: {}", e)))?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                CoreError::IOError(format!("Failed to read directory entry: {}", e))
            })?;
            let path = entry.path();

            if path.is_dir() {
                let meta_path = path.join("meta.json");
                if meta_path.exists() {
                    match self.load_table_meta(&meta_path) {
                        Ok(meta) => {
                            let table_name = meta.table_name.clone();
                            let mut tables = self.tables.write().unwrap();
                            tables.insert(table_name, Arc::new(meta));
                        }
                        Err(e) => {
                            log::error!("⚠️  Failed to load table from {:?}: {}", path, e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 从文件加载表元数据
    fn load_table_meta(&self, meta_path: &PathBuf) -> CoreResult<TableMeta> {
        let content = fs::read_to_string(meta_path)
            .map_err(|e| CoreError::IOError(format!("Failed to read table meta: {}", e)))?;

        let meta: TableMeta = serde_json::from_str(&content)
            .map_err(|e| CoreError::IOError(format!("Failed to parse table meta: {}", e)))?;

        Ok(meta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{field::FieldOption, Schema};
    use tempfile::TempDir;

    #[test]
    fn test_catalog_create_table() {
        let temp_dir = TempDir::new().unwrap();
        let catalog = Catalog::new(temp_dir.path().to_path_buf()).unwrap();

        let schema = Schema::new(
            "test_table".to_string(),
            Some("id".to_string()),
            true,
            vec![
                FieldOption::U64 {
                    name: "id".to_string(),
                    index: true,
                    description: None,
                    default_value: None,
                    nullable: true,
                },
                FieldOption::Keyword {
                    name: "name".to_string(),
                    index: true,
                    is_array: false,
                    persist_option: None,
                    case_sensitive: true,
                    description: None,
                    default_value: None,
                    nullable: true,
                },
            ],
            Default::default(),
            None, // description
        );

        let meta = TableMeta::new(
            "test_table".to_string(),
            schema,
            PartitionStrategy::Hash {
                field: "id".to_string(),
                num_partitions: 4,
            },
        );

        catalog.create_table(meta).unwrap();

        // 验证表存在
        let table = catalog.get_table("test_table").unwrap();
        assert_eq!(table.table_name, "test_table");

        // 验证目录结构
        let table_dir = table.table_dir(&catalog.work_dir);
        assert!(table_dir.exists());
        assert!(table_dir.join("meta.json").exists());

        // 验证 partition 目录
        if let Some(partitions) = table.partition_strategy.generate_partitions() {
            for partition_name in partitions {
                let partition_dir = table.partition_dir_by_name(&catalog.work_dir, &partition_name);
                assert!(partition_dir.exists());
                assert!(partition_dir.join("meta.json").exists());
            }
        }
    }
}
