//! Network utility functions

use std::net::{ToSocketAddrs, UdpSocket};
use std::time::Duration;

/// Get the local IP address that would be used to connect to a target address
///
/// This is useful for determining the real IP of the local machine when
/// the listen address is configured as 0.0.0.0 or ::
///
/// Uses UDP socket to determine the local IP without requiring the target service to be running.
/// The UDP "connect" operation only sets the default destination and doesn't send any data.
///
/// # Arguments
/// * `target_addr` - Target address to connect to (e.g., "example.com:80" or "192.168.1.1:7946")
/// * `_timeout` - Unused (kept for API compatibility)
///
/// # Returns
/// The local IP address as a string, or None if unable to determine
///
/// # Example
/// ```ignore
/// use std::time::Duration;
///
/// // Find out what IP we'd use to connect to Google DNS
/// if let Some(ip) = get_real_ip("8.8.8.8:53", Duration::from_secs(1)) {
///     println!("My real IP is: {}", ip);
/// }
/// ```
pub fn get_real_ip(target_addr: &str, _timeout: Duration) -> Option<String> {
    // Parse target address
    let addr = target_addr.to_socket_addrs().ok()?.next()?;

    // Create UDP socket and "connect" to target (doesn't actually send data)
    // This allows the OS to choose the appropriate local IP based on routing
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect(addr).ok()?;

    // Get the local address that was selected
    socket.local_addr().ok().map(|addr| addr.ip().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_real_ip() {
        // Try to connect to a public DNS server to get our IP
        // This test might fail in isolated environments
        let result = get_real_ip("8.8.8.8:53", Duration::from_secs(2));

        // We can't assert the exact IP, but we can check it's a valid format if present
        if let Some(ip) = result {
            assert!(
                ip.contains('.') || ip.contains(':'),
                "IP should contain . or :"
            );
        }
    }
}
