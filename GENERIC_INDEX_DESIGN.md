# 索引字段泛型重构设计文档

## 🎯 重构目标

将 `Keyword`, `NumI64`, `NumF64`, `NumU32`, `NumI32` 等几乎相同的代码统一为一套泛型实现。

## 📊 重构前的问题

### 代码重复

每个字段类型都有独立的实现文件：
- `keyword.rs` (~327 行)
- `num_i64.rs` (~218 行)
- `num_f64.rs` (~229 行)

**重复的代码模式：**
```rust
pub struct NumI64 {
    field: FieldOption,
    indexs: RwLock<InvertedIndex<i64>>,
}

impl NumI64 {
    pub fn new(field: &FieldOption) -> Self { ... }
    pub fn from_disk(...) -> CoreResult<Self> { ... }
    pub fn persist(&self, path: &str) -> CoreResult<Self> { ... }
}

impl IndexWriter for NumI64 { ... }
impl IndexReader for NumI64 { ... }
impl PkWriter for NumI64 { ... }
```

**问题：**
- ❌ 相同逻辑重复实现 5+ 次
- ❌ 新增类型（u64, u32, i32）需要复制粘贴
- ❌ Bug 修复需要在多个文件中重复
- ❌ 维护成本高

## ✨ 重构后的设计

### 核心思想

通过泛型 + trait 抽象，将共同逻辑提取到 `GenericIndexedField<K>`：

```rust
pub struct GenericIndexedField<K: IndexKey> {
    field: FieldOption,
    indexs: RwLock<InvertedIndex<K>>,
    _phantom: PhantomData<K>,
}
```

### IndexKey Trait

定义了索引键需要实现的行为：

```rust
pub trait IndexKey: Clone + Ord + Send + Sync + 'static {
    // 从 Arrow 数组中提取键值
    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_>;
    
    // 从 ScalarValue 转换
    fn from_scalar(value: &ScalarValue) -> Option<Self>;
    
    // 获取序列化器
    fn serializer(zstd_level: i32) -> Box<dyn Serializer<Self, RoaringBitmap>>;
    
    // 键的固定长度（0 = 可变长度）
    fn key_len() -> usize;
    
    // 标准化（如大小写转换）
    fn normalize(&self, case_sensitive: bool) -> Self { self.clone() }
    
    // 是否支持范围查询
    fn supports_range() -> bool { true }
}
```

### 类型别名

让代码更清晰：

```rust
pub type KeywordField = GenericIndexedField<String>;
pub type I64Field = GenericIndexedField<i64>;
pub type U64Field = GenericIndexedField<u64>;
pub type U32Field = GenericIndexedField<u32>;
pub type I32Field = GenericIndexedField<i32>;
pub type F64Field = GenericIndexedField<OrderedF64>;
pub type F32Field = GenericIndexedField<OrderedF32>;
```

## 📁 文件结构

### 新增文件

```
src/segment/field_store/
├── generic_index.rs       # 泛型实现（核心逻辑）
├── index_key_impls.rs     # IndexKey trait 的各类型实现
└── mod.rs                 # 导出类型别名
```

### 迁移计划

**阶段 1：创建泛型框架**
- ✅ 创建 `generic_index.rs`
- ✅ 创建 `index_key_impls.rs`
- ✅ 实现基础类型（String, i64, u64, u32, i32, f64, f32）

**阶段 2：集成到现有代码**
- [ ] 在 `mod.rs` 中导出新类型
- [ ] 更新 `Segment` 使用新类型
- [ ] 更新测试用例

**阶段 3：清理旧代码**
- [ ] 删除 `keyword.rs`（保留一段时间作为参考）
- [ ] 删除 `num_i64.rs`
- [ ] 删除 `num_f64.rs`

## 🎁 收益

### 代码量减少

| 项目 | 重构前 | 重构后 | 减少 |
|------|--------|--------|------|
| Keyword | 327 行 | → 类型别名 | **-327 行** |
| NumI64 | 218 行 | → 类型别名 | **-218 行** |
| NumF64 | 229 行 | → 类型别名 | **-229 行** |
| NumU64 | 需新增 ~220 行 | → 已包含 | **节省 220 行** |
| NumU32 | 需新增 ~220 行 | → 已包含 | **节省 220 行** |
| NumI32 | 需新增 ~220 行 | → 已包含 | **节省 220 行** |
| **总计** | **~1400+ 行** | **~400 行** | **减少 70%+** |

### 维护性提升

**重构前：**
```rust
// 修复 bug 需要改 5 个文件
keyword.rs:    fix_persistence_bug();
num_i64.rs:    fix_persistence_bug();
num_f64.rs:    fix_persistence_bug();
num_u32.rs:    fix_persistence_bug();
num_i32.rs:    fix_persistence_bug();
```

**重构后：**
```rust
// 只需改 1 处
generic_index.rs:  fix_persistence_bug();  // ✅ 所有类型自动修复
```

### 可扩展性

**新增类型非常简单：**

```rust
// 新增 i16 支持，只需 ~30 行
impl IndexKey for i16 {
    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        Box::new(
            arrow_downcast!(arr, Int16Array)
                .iter()
                .enumerate()
                .filter_map(|(idx, opt)| opt.map(|v| (idx, v)))
        )
    }
    
    fn from_scalar(value: &ScalarValue) -> Option<Self> {
        match value {
            ScalarValue::Int16(Some(v)) => Some(*v),
            _ => None,
        }
    }
    
    fn serializer(zstd_level: i32) -> Box<dyn Serializer<Self, RoaringBitmap>> {
        Box::new(I16RoaringSerializer::new(zstd_level))
    }
    
    fn key_len() -> usize { 2 }
}

// 使用
pub type I16Field = GenericIndexedField<i16>;
```

## 🔧 使用示例

### 创建字段

```rust
// Keyword 字段
let keyword_field = KeywordField::new(&field_option);

// I64 字段
let i64_field = I64Field::new(&field_option);

// U32 字段
let u32_field = U32Field::new(&field_option);
```

### 持久化

```rust
// 所有类型使用相同的 API
let disk_field = memory_field.persist("/path/to/index")?;
```

### 查询

```rust
// 精确查询
let results = field.query(&key);

// 范围查询（数值类型）
let results = field.range(&start, true, &end, false);
```

## ⚠️ 注意事项

### 1. Float 类型处理

f64/f32 使用 `OrderedFloat` 包装以实现 `Ord`：

```rust
use ordered_float::OrderedFloat;

pub type OrderedF64 = OrderedFloat<f64>;
pub type F64Field = GenericIndexedField<OrderedF64>;
```

### 2. String 的大小写处理

通过 `normalize()` 方法支持：

```rust
impl IndexKey for String {
    fn normalize(&self, case_sensitive: bool) -> Self {
        if case_sensitive {
            self.clone()
        } else {
            self.to_lowercase()
        }
    }
}
```

### 3. 数组字段支持

`extract_from_array()` 支持 `ListArray`：

```rust
if let Some(list_array) = arr.as_any().downcast_ref::<ListArray>() {
    // 处理数组字段...
}
```

## 🚀 后续优化

### 可能的改进

1. **宏简化 IndexKey 实现**
   ```rust
   impl_index_key!(i64, Int64Array, ScalarValue::Int64, I64RoaringSerializer, 8);
   impl_index_key!(u32, UInt32Array, ScalarValue::UInt32, U32RoaringSerializer, 4);
   ```

2. **自动生成 Serializer**
   - 使用 derive macro 自动实现序列化器

3. **性能优化**
   - 使用 SIMD 加速数组提取
   - 并行化批量索引构建

## 📝 总结

通过泛型重构：
- ✅ **代码量减少 70%+**
- ✅ **维护成本大幅降低**（1 处修改 vs 5 处）
- ✅ **新增类型成本极低**（~30 行 vs ~220 行）
- ✅ **类型安全不变**（编译时检查）
- ✅ **性能不受影响**（零成本抽象）

**这是一个非常值得的重构！** 🎯
