# MySQL 认证说明

## 当前认证状态

CalmCore MySQL 协议实现使用 `msql-srv` 库,该库对认证的支持有限。

### 已实现功能

✅ **用户名验证**
- 只有配置的用户名才能登录
- 错误的用户名会被拒绝
- 配置方式：
  ```bash
  # 命令行
  calm --user myuser --password mypass
  
  # 环境变量
  export CALM_USER=myuser
  export CALM_PASSWORD=mypass
  ```

### 限制

❌ **密码验证未实现**
- 当前任何密码都可以登录(只要用户名正确)
- 这是 `msql-srv` 库的限制,不是 bug

### 技术原因

`msql-srv` 库的 `AuthenticationContext` 结构只包含用户名,不包含密码或密码哈希:

```rust
pub struct AuthenticationContext<'a> {
    pub username: Option<Vec<u8>>,
    // 没有 password 字段!
}
```

`after_authentication` 回调在握手完成后调用,此时已无法访问客户端发送的密码。

## 解决方案

### 方案 1: 修改 msql-srv 库 (推荐)

修改 `libs/msql-srv-patched/src/lib.rs`:

1. 在 `AuthenticationContext` 添加 `auth_response` 字段
2. 在握手过程中保存客户端的认证响应
3. 实现 MySQL 密码哈希验证算法(SHA1)

优点:
- 完整的密码验证
- 符合 MySQL 协议标准

缺点:
- 需要理解 MySQL 认证协议
- 需要维护 fork 的库

### 方案 2: 使用其他库

替换为支持完整认证的 MySQL 协议库:
- `mysql_async` - 异步 MySQL 客户端/服务端
- 自己实现完整的 MySQL 协议

优点:
- 完整的认证支持
- 可能有更好的性能

缺点:
- 需要重写大量代码
- 学习新的 API

### 方案 3: 网络层安全

不依赖 MySQL 协议认证,使用外部安全措施:

```bash
# 使用防火墙限制访问
sudo ufw allow from 192.168.1.0/24 to any port 3307

# 使用 SSH 隧道
ssh -L 3307:localhost:3307 user@server

# 使用 VPN
# 只允许 VPN 网络访问 MySQL 端口
```

优点:
- 不需要修改代码
- 网络层安全更可靠

缺点:
- 部署复杂度增加
- 需要额外的基础设施

## 当前最佳实践

在密码验证未实现的情况下,建议:

1. **使用强用户名**
   ```bash
   # 不要使用 'root', 'admin' 等常见用户名
   calm --user "prod_analytics_2024" --password "complex_pass"
   ```

2. **绑定到本地回环**
   ```bash
   # 只监听 localhost,不暴露到外网
   calm --mysql-bind 127.0.0.1:3307
   ```

3. **使用防火墙**
   ```bash
   # 只允许特定 IP 访问
   sudo ufw allow from 192.168.1.100 to any port 3307
   ```

4. **使用 SSH 隧道**
   ```bash
   # 客户端通过 SSH 连接
   ssh -L 3307:localhost:3307 user@calm-server
   mysql -h 127.0.0.1 -P 3307 -u myuser -p
   ```

## 查询特殊处理

### MySQL 客户端初始化查询

以下查询会被自动处理,不会报错:

- `SELECT $$` - 客户端初始化
- `SELECT @@version_comment` - 版本信息
- `SELECT DATABASE()` - 当前数据库
- `SELECT 1` - 连接测试
- `SET NAMES utf8mb4` - 字符集设置
- `SHOW VARIABLES` - 变量查询
- `BEGIN/COMMIT/ROLLBACK` - 事务命令

这些查询会返回合理的默认值或空结果,确保 MySQL 客户端正常工作。

## 未来计划

1. 实现 MySQL 密码验证(方案 1)
2. 添加 SSL/TLS 支持
3. 支持多用户和权限管理
4. 实现完整的 MySQL 协议兼容性

## 相关代码

- `src/protocol/mysql/mod.rs` - MySQL 协议实现
- `libs/msql-srv-patched/` - MySQL 协议库
- `src/bin/calm/config.rs` - 配置管理
