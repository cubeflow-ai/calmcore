//! Network utility functions

use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

/// Get the local IP address that would be used to connect to a target address
///
/// This is useful for determining the real IP of the local machine when
/// the listen address is configured as 0.0.0.0 or ::
///
/// # Arguments
/// * `target_addr` - Target address to connect to (e.g., "example.com:80" or "192.168.1.1:7946")
/// * `timeout` - Connection timeout duration
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
pub fn get_real_ip(target_addr: &str, timeout: Duration) -> Option<String> {
    // Try to resolve and connect to the target
    if let Ok(mut addrs) = target_addr.to_socket_addrs() {
        if let Some(addr) = addrs.next() {
            // Try to connect (with timeout)
            if let Ok(stream) = TcpStream::connect_timeout(&addr, timeout) {
                // Get local address from the socket
                if let Ok(local_addr) = stream.local_addr() {
                    return Some(local_addr.ip().to_string());
                }
            }
        }
    }
    None
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
