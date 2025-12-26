//! gRPC 端口分配工具
//!
//! 从指定范围自动找到可用端口

use std::net::{SocketAddr, TcpListener};

/// 从指定端口开始查找可用端口
///
/// # 参数
/// - `start_port`: 起始端口号
/// - `max_attempts`: 最大尝试次数
///
/// # 返回
/// 可用的端口号，如果找不到则返回 None
pub fn find_available_port(start_port: u16, max_attempts: u16) -> Option<u16> {
    for offset in 0..max_attempts {
        let port = start_port.saturating_add(offset);
        if port == 0 || port < start_port {
            // 溢出了
            break;
        }

        // 尝试绑定端口
        if is_port_available(port) {
            log::debug!("🔍 [PortAllocator] Found available port: {}", port);
            return Some(port);
        }
    }

    log::warn!(
        "⚠️  [PortAllocator] No available port found in range {}-{}",
        start_port,
        start_port.saturating_add(max_attempts)
    );
    None
}

/// 检查端口是否可用
fn is_port_available(port: u16) -> bool {
    // 尝试在 IPv4 上绑定
    let addr_v4 = format!("0.0.0.0:{}", port);
    if let Ok(addr) = addr_v4.parse::<SocketAddr>() {
        if TcpListener::bind(addr).is_ok() {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_available_port() {
        let port = find_available_port(52000, 100);
        assert!(port.is_some());
        assert!(port.unwrap() >= 52000);
    }

    #[test]
    fn test_is_port_available() {
        // 大部分情况下 65000 应该是可用的
        assert!(is_port_available(65000));
    }
}
