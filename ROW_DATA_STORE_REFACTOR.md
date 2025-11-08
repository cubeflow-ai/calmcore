# RowDataStore 模块重构总结

## 变更说明

### 新建模块

- **文件**: `/src/segment/field_store/row_data.rs`
- **内容**: 将 `RowDataStore` 相关功能独立为一个专用模块

### 移动的代码

从 `mod.rs` 迁移到 `row_data.rs` 的内容：

1. **`ParquetRowDataReader` 结构体**
   - 用于读取 Parquet 格式的行数据
   - 支持 RowGroup 级别的映射和优化
   - 包含列投影(column projection)优化
   - 支持批量读取(batch read)

2. **`RowDataStore` 枚举**
   - 多后端支持：Memory、Disk (BTree)、Parquet
   - 统一的读写接口
   - 支持列投影优化
   - 支持 Floor 查询

3. **相关实现**
   - `ParquetRowDataReader` 的所有方法
   - `RowDataStore` 的所有方法
   - `Clone` trait 实现

### 模块结构

```
src/segment/field_store/
├── mod.rs                    # 保留：序列化器、特征定义、辅助函数
├── row_data.rs               # 新增：RowDataStore 和 ParquetRowDataReader
├── keyword.rs
├── num_f64.rs
├── num_i64.rs
```

### 公开接口

在 `mod.rs` 中导出：

```rust
pub use row_data::RowDataStore;
```

使用者可以通过 `field_store::RowDataStore` 访问。

### 代码组织优化

**mod.rs 现在专注于：**

- 各种 Serializer 实现 (I64RoaringSerializer, F64RoaringSerializer, StringRoaringSerializer, U32RecordBatchSerializer)
- Trait 定义 (IndexReader, IndexWriter, PkWriter)
- InvertedIndex 泛型结构体
- RoaringBitmap 编码/解码辅助函数

**row_data.rs 专注于：**

- RowDataStore 的所有存储后端实现
- Parquet 文件操作和优化
- 列投影和批量读取

## 优势

1. **代码模块化更清晰**
   - RowDataStore 代码量约 300+ 行，现在独立管理
   - 关注点分离（Separation of Concerns）

2. **易于维护和扩展**
   - 新增存储后端只需修改 `row_data.rs`
   - Parquet 相关优化集中在一个文件

3. **可读性提升**
   - 每个文件的责任更单一
   - 代码导航更容易
   - 文档维护更聚焦

4. **未来扩展空间**
   - 可以独立为 `crate::segment::row_data` 子模块
   - 便于引入新的存储后端（如 Arrow IPC、RocksDB 等）

## 兼容性

- 完全向后兼容
- 所有公开 API 保持不变
- 内部引用通过模块导出机制保持一致

## 验证

- ✅ 代码编译成功
- ✅ 所有导入正确解析
- ✅ 模块结构清晰
- ✅ 测试可正常运行
