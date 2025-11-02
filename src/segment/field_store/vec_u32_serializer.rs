use mem_btree::persist::{ReadSerializer, WriteSerializer};
use roaring::RoaringBitmap;
use std::borrow::Cow;
use std::error::Error;

/// 智能 Vec<u32> 序列化器
///
/// 根据数据特征自动选择最优的存储格式：
/// 1. 小数组（<100个元素）：直接存储
/// 2. 大数组且稀疏：RoaringBitmap
/// 3. 有序数组：Delta 编码
#[derive(Clone)]
pub struct VecU32Serializer {
    zstd_level: i32,
    // 当数组长度超过此值时使用 RoaringBitmap
    bitmap_threshold: usize,
}

#[derive(Clone, Copy)]
enum StorageFormat {
    // 直接存储原始 u32 数组
    Raw = 0,
    // 使用 RoaringBitmap
    Bitmap = 1,
    // Delta 编码
    Delta = 2,
}

impl VecU32Serializer {
    pub fn new(zstd_level: i32, bitmap_threshold: usize) -> Self {
        Self {
            zstd_level,
            bitmap_threshold,
        }
    }

    pub fn default() -> Self {
        Self {
            zstd_level: 3,
            bitmap_threshold: 100, // 经验值，可以通过基准测试调整
        }
    }

    // 检查数组是否有序
    fn is_sorted(data: &[u32]) -> bool {
        data.windows(2).all(|w| w[0] <= w[1])
    }

    // Delta 编码
    fn encode_delta(data: &[u32]) -> Vec<u32> {
        if data.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(data.len());
        result.push(data[0]); // 第一个值原样存储

        for i in 1..data.len() {
            // 计算差值（假设数组已排序）
            let delta = data[i].wrapping_sub(data[i - 1]);
            result.push(delta);
        }

        result
    }

    // Delta 解码
    fn decode_delta(data: &[u32]) -> Vec<u32> {
        if data.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(data.len());
        result.push(data[0]); // 第一个值原样存储

        let mut prev = data[0];
        for &delta in &data[1..] {
            let value = prev.wrapping_add(delta);
            result.push(value);
            prev = value;
        }

        result
    }
}

impl WriteSerializer<String, Vec<u32>> for VecU32Serializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<String>) -> Cow<'a, [u8]> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());

        for key in keys {
            let key_bytes = key.as_bytes();
            buf.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(key_bytes);
        }

        // 使用 zstd 压缩
        match zstd::encode_all(&buf[..], self.zstd_level) {
            Ok(compressed) => Cow::Owned(compressed),
            Err(_) => Cow::Owned(buf),
        }
    }

    fn serialize_value<'a>(&self, value: &'a Vec<u32>) -> Cow<'a, [u8]> {
        let mut buf = Vec::new();

        // 选择存储格式
        let format = if value.len() < self.bitmap_threshold {
            StorageFormat::Raw
        } else if Self::is_sorted(value) {
            StorageFormat::Delta
        } else {
            StorageFormat::Bitmap
        };

        // 写入格式标记
        buf.push(format as u8);

        match format {
            StorageFormat::Raw => {
                // 直接写入长度和数据
                buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
                for &v in value {
                    buf.extend_from_slice(&v.to_be_bytes());
                }
            }
            StorageFormat::Bitmap => {
                // 转换为 RoaringBitmap (数据可能无序，使用 from_iter)
                let bitmap = RoaringBitmap::from_iter(value.iter().copied());
                let mut bitmap_buf = Vec::new();
                bitmap.serialize_into(&mut bitmap_buf).unwrap();
                buf.extend_from_slice(&bitmap_buf);
            }
            StorageFormat::Delta => {
                // Delta 编码
                let encoded = Self::encode_delta(value);
                buf.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
                for &v in &encoded {
                    buf.extend_from_slice(&v.to_be_bytes());
                }
            }
        }

        // 压缩
        match zstd::encode_all(&buf[..], self.zstd_level) {
            Ok(compressed) => Cow::Owned(compressed),
            Err(_) => Cow::Owned(buf),
        }
    }
}

impl ReadSerializer<String, Vec<u32>> for VecU32Serializer {
    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<String> {
        // 解压
        let decompressed = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };

        let mut pos = 0;
        let mut keys = Vec::new();

        if decompressed.len() < 4 {
            return keys;
        }

        let count = u32::from_be_bytes([
            decompressed[pos],
            decompressed[pos + 1],
            decompressed[pos + 2],
            decompressed[pos + 3],
        ]) as usize;
        pos += 4;

        for _ in 0..count {
            if pos + 4 > decompressed.len() {
                break;
            }

            let len = u32::from_be_bytes([
                decompressed[pos],
                decompressed[pos + 1],
                decompressed[pos + 2],
                decompressed[pos + 3],
            ]) as usize;
            pos += 4;

            if pos + len > decompressed.len() {
                break;
            }

            if let Ok(key) = String::from_utf8(decompressed[pos..pos + len].to_vec()) {
                keys.push(key);
            }
            pos += len;
        }

        keys
    }

    fn deserialize_value<'a>(&self, data: &'a [u8]) -> Result<Vec<u32>, Box<dyn Error>> {
        // 解压
        let decompressed = zstd::decode_all(data)?;
        if decompressed.is_empty() {
            return Ok(Vec::new());
        }

        // 读取格式标记
        let format = match decompressed[0] {
            0 => StorageFormat::Raw,
            1 => StorageFormat::Bitmap,
            2 => StorageFormat::Delta,
            _ => return Err("Invalid format".into()),
        };

        let mut pos = 1;
        match format {
            StorageFormat::Raw => {
                if pos + 4 > decompressed.len() {
                    return Ok(Vec::new());
                }
                let count = u32::from_be_bytes([
                    decompressed[pos],
                    decompressed[pos + 1],
                    decompressed[pos + 2],
                    decompressed[pos + 3],
                ]) as usize;
                pos += 4;

                let mut result = Vec::with_capacity(count);
                for _ in 0..count {
                    if pos + 4 > decompressed.len() {
                        break;
                    }
                    let value = u32::from_be_bytes([
                        decompressed[pos],
                        decompressed[pos + 1],
                        decompressed[pos + 2],
                        decompressed[pos + 3],
                    ]);
                    result.push(value);
                    pos += 4;
                }
                Ok(result)
            }
            StorageFormat::Bitmap => {
                let bitmap = RoaringBitmap::deserialize_from(&decompressed[pos..])?;
                Ok(bitmap.iter().collect())
            }
            StorageFormat::Delta => {
                if pos + 4 > decompressed.len() {
                    return Ok(Vec::new());
                }
                let count = u32::from_be_bytes([
                    decompressed[pos],
                    decompressed[pos + 1],
                    decompressed[pos + 2],
                    decompressed[pos + 3],
                ]) as usize;
                pos += 4;

                let mut encoded = Vec::with_capacity(count);
                for _ in 0..count {
                    if pos + 4 > decompressed.len() {
                        break;
                    }
                    let value = u32::from_be_bytes([
                        decompressed[pos],
                        decompressed[pos + 1],
                        decompressed[pos + 2],
                        decompressed[pos + 3],
                    ]);
                    encoded.push(value);
                    pos += 4;
                }
                Ok(Self::decode_delta(&encoded))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_vec() {
        let serializer = VecU32Serializer::default();
        let original = vec![1, 5, 3, 2, 4];
        let key = "test".to_string();

        let serialized = serializer.serialize_value(&original);
        let deserialized = serializer.deserialize_value(&serialized).unwrap();

        assert_eq!(original, deserialized);

        // 验证格式是 Raw
        assert_eq!(
            zstd::decode_all(&*serialized).unwrap()[0],
            StorageFormat::Raw as u8
        );
    }

    #[test]
    fn test_large_vec() {
        let serializer = VecU32Serializer::new(3, 5); // 设置较小的阈值以便测试
        let original: Vec<u32> = (0..100).collect();

        let serialized = serializer.serialize_value(&original);
        let deserialized = serializer.deserialize_value(&serialized).unwrap();

        assert_eq!(original, deserialized);

        // 验证格式是 Delta（因为是有序的）
        assert_eq!(
            zstd::decode_all(&*serialized).unwrap()[0],
            StorageFormat::Delta as u8
        );
    }

    #[test]
    fn test_large_unsorted_vec() {
        let serializer = VecU32Serializer::new(3, 5); // 设置较小的阈值以便测试
        let mut original: Vec<u32> = (0..100).collect();
        original.swap(0, 50); // 打乱顺序

        let serialized = serializer.serialize_value(&original);
        let deserialized = serializer.deserialize_value(&serialized).unwrap();

        assert_eq!(original, deserialized);

        // 验证格式是 Bitmap（因为大于阈值且无序）
        assert_eq!(
            zstd::decode_all(&*serialized).unwrap()[0],
            StorageFormat::Bitmap as u8
        );
    }

    #[test]
    fn test_delta_coding() {
        let serializer = VecU32Serializer::default();
        let original = vec![1, 3, 7, 15, 31];

        // 测试 delta 编码/解码
        let encoded = VecU32Serializer::encode_delta(&original);
        let decoded = VecU32Serializer::decode_delta(&encoded);

        assert_eq!(original, decoded);
        assert_eq!(encoded[0], original[0]); // 第一个值应该保持不变
                                             // delta 值应该是：[1, 2, 4, 8, 16]
        assert_eq!(encoded[1], 2);
        assert_eq!(encoded[2], 4);
        assert_eq!(encoded[3], 8);
        assert_eq!(encoded[4], 16);
    }
}
