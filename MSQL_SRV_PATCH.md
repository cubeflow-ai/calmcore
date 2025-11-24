# MySQL JDBC CLIENT_SECURE_CONNECTION 修复

## 问题描述

MySQL JDBC 驱动(Connector/J)要求服务器在握手时声明支持 `CLIENT_SECURE_CONNECTION` capability flag (0x8000)。
原版 `msql-srv 0.11` 库只设置了 `0x4200`,不包含此 flag,导致 JDBC 连接失败:

```
java.sql.SQLNonTransientConnectionException: CLIENT_SECURE_CONNECTION is required
```

## 解决方案

### 1. 修改 Capability Flags

在 `libs/msql-srv-patched/src/lib.rs` 第 275 行:

```rust
// ❌ 原版本 (0x4200 = 0x0200 | 0x4000)
let capabilities = &mut [0x00, 0x42]; // 4.1 proto

// ✅ Patched 版本 (0xC200 = 0x0200 | 0x4000 | 0x8000)
let capabilities = &mut [0x00, 0xC2]; // 4.1 proto + SECURE_CONNECTION
```

**Capability Flags 说明** (小端序):
- `0x0200` = `CLIENT_PROTOCOL_41` - MySQL 4.1+ 协议
- `0x4000` = `CLIENT_MULTI_STATEMENTS` - 支持多语句
- `0x8000` = `CLIENT_SECURE_CONNECTION` - 支持安全连接认证 ← **JDBC 必需**

### 2. 使用本地 Patched 版本

在 `Cargo.toml`:

```toml
# msql-srv = "0.11"  # 原版本
msql-srv = { path = "libs/msql-srv-patched" }  # Patched 版本
```

## 测试

### 启动服务器

```bash
cargo build --release
./target/release/calm mysql --port 3307
```

### Java JDBC 连接测试

```java
String url = "jdbc:mysql://127.0.0.1:3307/r2api?useSSL=false&allowPublicKeyRetrieval=true";

try (Connection conn = DriverManager.getConnection(url, "root", "")) {
    System.out.println("✅ 连接成功!");
    
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM r2api")) {
        if (rs.next()) {
            System.out.println("表行数: " + rs.getLong(1));
        }
    }
}
```

### 验证 Capability Flags

使用 Wireshark 或 tcpdump 抓包查看握手包:

```bash
# 抓取 MySQL 协议握手包
sudo tcpdump -i lo0 -X 'port 3307' | head -50
```

**预期握手包 (第 13-14 字节)**:
```
Offset  Hex         ASCII
...
0x000c: 00 c2       <- Capabilities (小端序 0xC200)
```

## Capability Flags 完整列表

| Flag | Value | 说明 |
|------|-------|------|
| CLIENT_LONG_PASSWORD | 0x0001 | 使用改进的密码哈希 |
| CLIENT_FOUND_ROWS | 0x0002 | 返回找到的行数而非影响的行数 |
| CLIENT_LONG_FLAG | 0x0004 | 支持长字段标志 |
| CLIENT_CONNECT_WITH_DB | 0x0008 | 连接时可指定数据库 |
| CLIENT_PROTOCOL_41 | 0x0200 | **MySQL 4.1+ 协议** |
| CLIENT_TRANSACTIONS | 0x2000 | 支持事务 |
| CLIENT_MULTI_STATEMENTS | 0x4000 | 支持多语句 |
| **CLIENT_SECURE_CONNECTION** | **0x8000** | **支持安全连接 (JDBC 必需)** ✅ |

## 技术细节

### MySQL 握手协议

```
[Server Initial Handshake Packet]
1 byte:  protocol_version (10)
n bytes: server_version (null-terminated string)
4 bytes: connection_id
8 bytes: auth_plugin_data_part_1 (随机盐)
1 byte:  filler (0x00)
2 bytes: capability_flags_1 (低16位) ← 这里设置 0xC2 0x00
1 byte:  character_set
2 bytes: status_flags
2 bytes: capability_flags_2 (高16位)
...
```

### 修改前后对比

| 版本 | Capability Flags | 支持 JDBC |
|------|------------------|-----------|
| 原版 msql-srv 0.11 | 0x4200 | ❌ |
| Patched 版本 | 0xC200 | ✅ |

**增量**: `0xC200 - 0x4200 = 0x8000` (CLIENT_SECURE_CONNECTION)

## 相关问题

### 为什么不直接在连接字符串禁用?

某些 JDBC 驱动版本(尤其是 8.0+)在底层强制要求 `CLIENT_SECURE_CONNECTION`,无法通过连接参数禁用。
修改服务器端握手包是最根本的解决方案。

### 安全性影响?

`CLIENT_SECURE_CONNECTION` 只是声明服务器**支持**安全认证,实际是否使用加密由 SSL/TLS 决定。
我们的实现:
- ✅ 声明支持 (避免 JDBC 拒绝连接)  
- ❌ 不强制加密 (使用 `useSSL=false` 可以明文连接)
- ✅ 认证流程正常工作

## 后续改进

1. **完整的密码认证**: 当前 `msql-srv` 只做基本认证,可以增强
2. **SSL/TLS 支持**: 添加 `tls` feature 启用加密连接
3. **更多 Capability Flags**: 根据需要添加其他 flags (如 CLIENT_DEPRECATE_EOF)

## 参考资料

- [MySQL Protocol Documentation](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html)
- [MySQL Capability Flags](https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html)
- [msql-srv GitHub](https://github.com/jonhoo/msql-srv)
