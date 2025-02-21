pub fn local_ip(remote_addr: &str) -> Option<String> {
    let remote_addr = remote_addr.replace("http://", "").replace("https://", "");

    let socket = match std::net::UdpSocket::bind("0.0.0.0:0") {
        Ok(s) => s,
        Err(_) => return None,
    };

    match socket.connect(remote_addr) {
        Ok(()) => (),
        Err(_) => return None,
    };

    match socket.local_addr() {
        Ok(addr) => Some(addr.ip().to_string()),
        Err(_) => None,
    }
}
