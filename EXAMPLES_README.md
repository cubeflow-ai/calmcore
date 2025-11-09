# CalmCore 示例程序说明

## test_all_types.rs - 完整类型系统演示

这个示例程序展示了 CalmCore 支持的所有 12 种数据类型及其完整功能。

### 支持的数据类型

#### 1. 有符号整数 (4种)
- **I8**: 8位有符号整数 (-128 到 127)
- **I16**: 16位有符号整数 (-32,768 到 32,767)
- **I32**: 32位有符号整数 (-2³¹ 到 2³¹-1)
- **I64**: 64位有符号整数 (-2⁶³ 到 2⁶³-1)

#### 2. 无符号整数 (4种)
- **U8**: 8位无符号整数 (0 到 255)
- **U16**: 16位无符号整数 (0 到 65,535)
- **U32**: 32位无符号整数 (0 到 2³²-1)
- **U64**: 64位无符号整数 (0 到 2⁶⁴-1)

#### 3. 浮点数 (2种)
- **F32**: 32位浮点数（单精度）
- **F64**: 64位浮点数（双精度）

#### 4. 布尔值 (1种)
- **Boolean**: 布尔类型 (true/false)

#### 5. 字符串 (1种)
- **Keyword**: 字符串类型，支持数组和大小写敏感配置

### 运行示例

```bash
# 编译并运行
cargo run --example test_all_types

# 仅编译
cargo build --example test_all_types
```

### 示例输出

程序会展示以下内容：

1. **Schema 定义**
   - 列出所有 12 种类型的字段定义
   - 显示每个字段的配置（是否索引、是否数组）

2. **Arrow Schema 转换**
   - 展示 CalmCore Schema 到 Arrow Schema 的映射
   - 验证类型转换的正确性

3. **测试数据创建**
   - 创建包含所有类型的 RecordBatch
   - 展示 3 条完整的测试记录

4. **数据内容展示**
   - 详细打印每条记录的所有字段值
   - 展示各种类型的实际数据

5. **类型验证**
   - 逐一验证每个字段的类型是否正确
   - 确保类型系统完整性

6. **类型覆盖统计**
   - 按类别统计类型覆盖情况
   - 确认所有 12 种类型都已支持

### 示例数据

程序创建了 3 条测试记录，涵盖所有类型：

```
记录 1: Alice (18岁, 状态1, 端口8080, 价格$9.99, 活跃)
记录 2: Bob (25岁, 状态2, 端口8081, 价格$19.99, 不活跃)
记录 3: Charlie (-10岁, 状态3, 端口8082, 价格$29.99, 活跃)
```

### 技术细节

#### Schema 定义示例

```rust
FieldOption::I8 {
    name: "age_i8".to_string(),
    index: true,
}
```

#### Arrow 类型映射

| CalmCore | Arrow | 说明 |
|----------|-------|------|
| I8 | Int8 | 8位有符号整数 |
| I16 | Int16 | 16位有符号整数 |
| ... | ... | ... |
| Boolean | Boolean | 布尔值 |
| Keyword | Utf8 | UTF-8字符串 |

#### RecordBatch 创建

```rust
let age_i8 = Int8Array::from(vec![18, 25, -10]);
let name = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
// ... 其他字段
```

### 实际应用

这些类型可以通过 **Engine** 在实际应用中使用：

```rust
// 1. 创建 Engine
let engine = Engine::new(...);

// 2. 创建包含这些类型的表
engine.create_table(schema);

// 3. 插入数据（支持所有 12 种类型）
engine.insert_json(...);

// 4. 查询（支持所有类型的索引和范围查询）
engine.query(...);
```

### 功能特性

所有 12 种类型都支持：

- ✅ **索引查询**: 精确匹配查询
- ✅ **范围查询**: 范围过滤（除 Boolean 外）
- ✅ **持久化**: 序列化和反序列化
- ✅ **类型安全**: 编译时类型检查
- ✅ **内存优化**: RoaringBitmap 高效存储
- ✅ **泛型设计**: 统一的实现框架

### 性能特点

- **倒排索引**: O(1) 精确查询
- **RoaringBitmap**: 高效的文档 ID 存储
- **零拷贝**: Arrow 数据格式
- **并行处理**: Rayon 并行迭代器
- **压缩存储**: 可配置 Zstd 压缩

### 扩展性

类型系统采用泛型设计，新增类型只需：

1. 实现 `IndexKey` trait
2. 实现 `RoaringSerializer`
3. 在 `FieldOption` 枚举中添加变体
4. 更新 `to_arrow_schema()` 映射

### 相关文件

- `src/segment/field_store/generic_index.rs` - 泛型索引实现
- `src/segment/field_store/index_key_impls.rs` - IndexKey 实现
- `src/segment/field_store/mod.rs` - 序列化器实现
- `src/schema/field.rs` - FieldType 和 FieldOption 定义
- `TYPE_SYSTEM_INTEGRATION_REPORT.md` - 完整集成报告

### 编译状态

```bash
✅ cargo build --lib
   Compiling calm v0.1.0
   Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.13s
   
✅ 0 warnings
✅ 所有类型完整支持
```

---

*最后更新: 2025年11月9日*
*版本: v0.1.0*
