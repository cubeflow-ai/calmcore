#![allow(dead_code)]

/// ZigZag encoding: converts signed integers to unsigned integers
#[inline]
pub fn encode_zigzag_64(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

/// ZigZag encoding for 32-bit integers
#[inline]
pub fn encode_zigzag_32(n: i32) -> u32 {
    ((n << 1) ^ (n >> 31)) as u32
}

/// ZigZag decoding: converts unsigned integers back to signed integers
#[inline]
pub fn decode_zigzag_64(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

/// ZigZag decoding for 32-bit integers
#[inline]
pub fn decode_zigzag_32(n: u32) -> i32 {
    ((n >> 1) as i32) ^ (-((n & 1) as i32))
}

/// Write a u32 value using varint encoding to any writer
pub fn write_u32<W: std::io::Write>(value: u32, writer: &mut W) -> std::io::Result<()> {
    let mut val = value;
    while val >= 0x80 {
        writer.write_all(&[((val & 0x7f) | 0x80) as u8])?;
        val >>= 7;
    }
    writer.write_all(&[val as u8])
}

/// Write a u64 value using varint encoding to any writer
pub fn write_u64<W: std::io::Write>(value: u64, writer: &mut W) -> std::io::Result<()> {
    let mut val = value;
    while val >= 0x80 {
        writer.write_all(&[((val & 0x7f) | 0x80) as u8])?;
        val >>= 7;
    }
    writer.write_all(&[val as u8])
}

/// Write a u16 value using varint encoding to any writer
pub fn write_u16<W: std::io::Write>(value: u16, writer: &mut W) -> std::io::Result<()> {
    let mut val = value;
    while val >= 0x80 {
        writer.write_all(&[((val & 0x7f) | 0x80) as u8])?;
        val >>= 7;
    }
    writer.write_all(&[val as u8])
}

/// Write an i32 value using zigzag encoding
pub fn write_i32<W: std::io::Write>(value: i32, writer: &mut W) -> std::io::Result<()> {
    write_u32(encode_zigzag_32(value), writer)
}

/// Write an i64 value using zigzag encoding
pub fn write_i64<W: std::io::Write>(value: i64, writer: &mut W) -> std::io::Result<()> {
    write_u64(encode_zigzag_64(value), writer)
}

pub trait BufferRead {
    fn get_byte(&self, pos: usize) -> u8;
    fn size(&self) -> usize;
}

impl BufferRead for memmap2::Mmap {
    fn get_byte(&self, pos: usize) -> u8 {
        unsafe { *self.as_ptr().add(pos) }
    }
    fn size(&self) -> usize {
        self.len()
    }
}

impl BufferRead for [u8] {
    fn get_byte(&self, pos: usize) -> u8 {
        self[pos]
    }
    fn size(&self) -> usize {
        self.len()
    }
}

impl BufferRead for Vec<u8> {
    fn get_byte(&self, pos: usize) -> u8 {
        self[pos]
    }
    fn size(&self) -> usize {
        self.len()
    }
}

/// Read a u32 value using varint decoding
pub fn read_u32<B: BufferRead>(buf: &B, pos: &mut usize) -> u32 {
    let mut result = 0u32;
    let mut shift = 0;

    loop {
        let byte = buf.get_byte(*pos);
        *pos += 1;

        result |= ((byte & 0x7f) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
    }

    result
}

/// Read a u64 value using varint decoding
pub fn read_u64(buf: &memmap2::Mmap, pos: &mut usize) -> u64 {
    let mut result = 0u64;
    let mut shift = 0;

    loop {
        let byte = buf.get_byte(*pos);
        *pos += 1;

        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
    }

    result
}

/// Read a u16 value using varint decoding
pub fn read_u16<B: BufferRead>(buf: &B, pos: &mut usize) -> u16 {
    let mut result = 0u16;
    let mut shift = 0;

    loop {
        let byte = buf.get_byte(*pos);
        *pos += 1;

        result |= ((byte & 0x7f) as u16) << shift;
        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
    }

    result
}

/// Read an i32 value using varint and zigzag decoding
pub fn read_i32<B: BufferRead>(buf: &B, pos: &mut usize) -> i32 {
    let val = read_u32(buf, pos);
    decode_zigzag_32(val)
}

/// Read an i64 value using varint and zigzag decoding
pub fn read_i64(buf: &memmap2::Mmap, pos: &mut usize) -> i64 {
    let val = read_u64(buf, pos);
    decode_zigzag_64(val)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u32_varint() {
        let test_cases = vec![0u32, 1u32, 127u32, 128u32, 16383u32, 16384u32, u32::MAX];

        for value in test_cases {
            let mut buf = Vec::new();

            // Test encoding
            write_u32(value, &mut buf).unwrap();
            println!("u32 Value: {}, Encoded bytes: {:?}", value, buf);

            // Test decoding
            let mut pos = 0;
            let decoded = read_u32(&buf, &mut pos);

            // Verify results
            assert_eq!(value, decoded);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn test_u16_varint() {
        let test_cases = vec![0u16, 1u16, 127u16, 128u16, 16383u16, 16384u16, u16::MAX];

        for value in test_cases {
            let mut buf = Vec::new();

            // Test encoding
            write_u16(value, &mut buf).unwrap();
            println!("u16 Value: {}, Encoded bytes: {:?}", value, buf);

            // Test decoding
            let mut pos = 0;
            let decoded = read_u16(&buf, &mut pos);

            // Verify results
            assert_eq!(value, decoded);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn test_i32_varint() {
        let test_cases = vec![0i32, 1i32, -1i32, i32::MAX, i32::MIN, 12345i32, -12345i32];

        for value in test_cases {
            let mut buf = Vec::new();

            // Test encoding
            write_i32(value, &mut buf).unwrap();
            println!("i32 Value: {}, Encoded bytes: {:?}", value, buf);

            // Test decoding
            let mut pos = 0;
            let decoded = read_i32(&buf, &mut pos);

            // Verify results
            assert_eq!(value, decoded);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn test_zigzag_32() {
        let test_cases = vec![0i32, -1i32, 1i32, -2i32, 2i32, i32::MAX, i32::MIN];

        for value in test_cases {
            let encoded = encode_zigzag_32(value);
            let decoded = decode_zigzag_32(encoded);
            assert_eq!(value, decoded);
        }
    }

    #[test]
    fn test_varint_offset_u16() {
        let test_cases = vec![
            (0u16, 1),     // 1 byte: 0-127
            (100u16, 1),   // 1 byte: 最大边界
            (127u16, 1),   // 1 byte: 最大边界
            (128u16, 2),   // 2 bytes: 最小边界
            (16383u16, 2), // 2 bytes: 最大边界
            (16384u16, 3), // 3 bytes
            (u16::MAX, 3), // 3 bytes: 最大值
        ];

        for (value, expected_size) in test_cases {
            let mut buf = Vec::new();
            write_u16(value, &mut buf).unwrap();

            let mut pos = 0;
            let decoded = read_u16(&buf, &mut pos);

            assert_eq!(value, decoded, "Value mismatch for {}", value);
            assert_eq!(
                expected_size,
                buf.len(),
                "Buffer size mismatch for value {}",
                value
            );
        }
    }

    #[test]
    fn test_varint_offset_u32() {
        let test_cases = vec![
            (0u32, 1),         // 1 byte: 0-127
            (127u32, 1),       // 1 byte: 最大边界
            (128u32, 2),       // 2 bytes: 最小边界
            (16383u32, 2),     // 2 bytes: 最大边界
            (16384u32, 3),     // 3 bytes: 最小边界
            (2097151u32, 3),   // 3 bytes: 最大边界
            (2097152u32, 4),   // 4 bytes
            (268435455u32, 4), // 4 bytes: 最大边界
            (268435456u32, 5), // 5 bytes
            (u32::MAX, 5),     // 5 bytes: 最大值
        ];

        for (value, expected_size) in test_cases {
            let mut buf = Vec::new();
            write_u32(value, &mut buf).unwrap();

            let mut pos = 0;
            let decoded = read_u32(&buf, &mut pos);

            assert_eq!(value, decoded, "Value mismatch for {}", value);
            assert_eq!(
                expected_size,
                buf.len(),
                "Buffer size mismatch for value {}",
                value
            );
        }
    }

    #[test]
    fn test_consecutive_reads() {
        let mut buf = Vec::new();

        // 写入多个连续的值
        let values = vec![127u16, 128u16, 16384u16, 0u16];
        for &v in &values {
            write_u16(v, &mut buf).unwrap();
        }

        // 连续读取并验证位置
        let mut pos = 0;
        for &expected_value in &values {
            let value = read_u16(&buf, &mut pos);
            assert_eq!(value, expected_value, "Value mismatch");
            assert!(pos > 0, "Position should advance");
        }

        // 确保读取到了正确的结束位置
        assert_eq!(pos, buf.len(), "Final position should match buffer length");
    }
}
