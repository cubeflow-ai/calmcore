# Calm Engine 日志系统使用说明

## 概述

Calm Engine 使用标准的 Rust `log` crate 进行日志输出，提供专业的、可配置的日志系统。

## 日志级别

- **ERROR** - 严重错误，需要立即关注
- **WARN** - 警告信息，潜在问题
- **INFO** - 重要操作信息（默认级别）
- **DEBUG** - 详细调试信息
- **TRACE** - 最详细的跟踪信息

## 使用方式

### 1. 在应用程序中初始化日志

```rust
use env_logger;

fn main() {
    // 简单初始化（使用默认配置）
    env_logger::init();
    
    // 或者使用自定义配置
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info")
    ).init();
    
    // 你的应用代码...
}
```

### 2. 通过环境变量控制日志级别

```bash
# 显示所有 INFO 及以上级别的日志
RUST_LOG=info cargo run --example persist_demo

# 只显示 WARN 和 ERROR
RUST_LOG=warn cargo run --example persist_demo

# 显示详细的 DEBUG 信息
RUST_LOG=debug cargo run --example persist_demo

# 显示所有日志（包括 TRACE）
RUST_LOG=trace cargo run --example persist_demo

# 针对特定模块设置日志级别
RUST_LOG=calm::engine=debug,calm::partition=info cargo run

# 在生产环境禁用日志
cargo run  # 不设置 RUST_LOG
```

### 3. 日志输出示例

#### INFO 级别（默认推荐）
```bash
$ RUST_LOG=info cargo run --example persist_demo --release
```

输出：
```
[2025-01-08T10:30:15Z INFO  calm::engine] Added partition 1
[2025-01-08T10:30:15Z INFO  calm::partition] Flushing segment 0 with 10000 records
[2025-01-08T10:30:15Z INFO  calm::partition] Found 1 unpersisted segments, starting persist...
[2025-01-08T10:30:15Z INFO  calm::partition] Persist completed, 1 segments persisted
[2025-01-08T10:30:16Z INFO  calm::engine] Persisting partition 1...
[2025-01-08T10:30:16Z INFO  calm::partition] [Partition 1] Flushing current segment (5000 records)
[2025-01-08T10:30:16Z INFO  calm::partition] [Partition 1] All data persisted
[2025-01-08T10:30:17Z INFO  calm::engine] Stopping background persist task...
[2025-01-08T10:30:17Z INFO  calm::engine] Background task stopped
[2025-01-08T10:30:17Z INFO  calm::engine] Persisting 2 partitions...
[2025-01-08T10:30:17Z INFO  calm::engine] Engine stop summary:
[2025-01-08T10:30:17Z INFO  calm::engine]   Partitions persisted: 2
[2025-01-08T10:30:17Z INFO  calm::engine]   Total documents: 28000
[2025-01-08T10:30:17Z INFO  calm::engine]   Total segments: 4
[2025-01-08T10:30:17Z INFO  calm::engine]   Unpersisted segments: 0
[2025-01-08T10:30:17Z INFO  calm::engine] All data persisted successfully
[2025-01-08T10:30:17Z INFO  calm::engine] Engine stop completed
```

#### DEBUG 级别（调试时使用）
```bash
$ RUST_LOG=debug cargo run --example persist_demo --release
```

额外输出：
```
[2025-01-08T10:30:15Z DEBUG calm::partition] Persisting segment 0 to disk
[2025-01-08T10:30:15Z DEBUG calm::segment] Persisting segment to: /tmp/calm_persist_demo/partition-1/partition-1/segment-0-9999_tmp (temp)
[2025-01-08T10:30:15Z DEBUG calm::segment] Segment persist completed in 15.288125ms
[2025-01-08T10:30:15Z DEBUG calm::partition] Segment 0 persisted successfully
```

## 日志配置最佳实践

### 开发环境
```bash
export RUST_LOG=debug
```
- 显示详细的调试信息
- 便于问题排查

### 测试环境
```bash
export RUST_LOG=info
```
- 显示关键操作信息
- 性能与可观测性平衡

### 生产环境
```bash
export RUST_LOG=warn
```
- 只显示警告和错误
- 最小化日志开销
- 便于问题告警

## 日志文件输出

如果需要将日志输出到文件：

```bash
# 输出到文件
RUST_LOG=info cargo run --example persist_demo 2>&1 | tee calm.log

# 只保存日志（不在终端显示）
RUST_LOG=info cargo run --example persist_demo 2>&1 > calm.log

# 日志和错误分开
RUST_LOG=info cargo run --example persist_demo 2>error.log 1>output.log
```

## 与现有代码的兼容性

旧代码中的 `println!` 已被替换为相应的日志宏：
- `println!` → `log::info!`（重要操作）
- `println!` → `log::debug!`（详细信息）
- `eprintln!` → `log::error!`（错误信息）
- `eprintln!` → `log::warn!`（警告信息）

**用户友好输出**（如演示程序中的表格、进度提示）仍然保留 `println!`，不受日志系统影响。

## 代码示例

### 在你的代码中使用日志

```rust
use log::{debug, info, warn, error};

fn process_data() {
    info!("开始处理数据...");
    
    match do_something() {
        Ok(result) => {
            debug!("处理结果: {:?}", result);
            info!("数据处理成功");
        }
        Err(e) => {
            error!("数据处理失败: {:?}", e);
        }
    }
    
    if some_condition {
        warn!("检测到潜在问题，但继续执行");
    }
}
```

## 性能说明

- 日志宏在编译时可以被优化掉（如果日志级别不够）
- 使用 `RUST_LOG` 不设置 = 几乎零性能开销
- `debug!` 和 `trace!` 在 release 构建中开销极小
- 生产环境建议使用 `RUST_LOG=warn` 或不设置

## 总结

✅ **专业** - 使用标准的 Rust 日志系统  
✅ **灵活** - 可通过环境变量动态配置  
✅ **高效** - 低性能开销，可按需启用  
✅ **易用** - 简单的初始化，丰富的日志级别  

日志系统让代码更专业、更易于调试和监控！
