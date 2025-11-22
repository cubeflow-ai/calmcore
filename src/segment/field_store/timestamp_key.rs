/// Timestamp 类型的 IndexKey 实现
///
/// 使用 newtype 包装 i64,支持字符串时间自动转换
use super::generic_index::IndexKey;
use super::*;
use crate::utils::timestamp::parse_scalar_to_timestamp;
use datafusion::arrow::array::*;
use std::borrow::Cow;

/// Timestamp 专用序列化器
///
/// 复用 I64RoaringSerializer,但在序列化层面转换 TimestampKey <-> i64
#[derive(Clone)]
pub struct TimestampRoaringSerializer {
    inner: I64RoaringSerializer,
}

impl TimestampRoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self {
            inner: I64RoaringSerializer::new(zstd_level),
        }
    }
}

impl mem_btree::persist::WriteSerializer<TimestampKey, RoaringBitmap>
    for TimestampRoaringSerializer
{
    fn serialize_keys<'a>(&self, keys: &'a Vec<TimestampKey>) -> Cow<'a, [u8]> {
        // 转换 TimestampKey -> i64
        let i64_keys: Vec<i64> = keys.iter().map(|k| k.0).collect();
        // 必须返回 Cow::Owned,因为我们创建了新的数据
        match self.inner.serialize_keys(&i64_keys) {
            Cow::Borrowed(b) => Cow::Owned(b.to_vec()),
            Cow::Owned(v) => Cow::Owned(v),
        }
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        self.inner.serialize_value(value)
    }
}

impl mem_btree::persist::ReadSerializer<TimestampKey, RoaringBitmap>
    for TimestampRoaringSerializer
{
    fn deserialize_keys(&self, data: &[u8]) -> Vec<TimestampKey> {
        // 反序列化 i64 -> TimestampKey
        self.inner
            .deserialize_keys(data)
            .into_iter()
            .map(TimestampKey)
            .collect()
    }

    fn deserialize_value(
        &self,
        data: &[u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        self.inner.deserialize_value(data)
    }
}

/// Timestamp 键类型
///
/// 底层存储为 i64(毫秒),但查询时支持:
/// - 数值型时间戳(自动判断秒/毫秒)
/// - 字符串时间: "2024-01-01", "2024-01-01T10:00:00Z"
/// - Arrow Date/Timestamp 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampKey(pub i64);

impl IndexKey for TimestampKey {
    type Serializer = TimestampRoaringSerializer;

    fn extract_from_array(arr: &ArrayRef) -> Box<dyn Iterator<Item = (usize, Self)> + '_> {
        use datafusion::arrow::array::StringArray;

        // 从 Arrow 数组中提取时间戳
        // 支持 Int64Array (毫秒时间戳)、TimestampMillisecondArray 和 StringArray
        if let Some(int64_array) = arr.as_any().downcast_ref::<Int64Array>() {
            Box::new(
                int64_array
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, opt)| opt.map(|v| (idx, TimestampKey(v)))),
            )
        } else if let Some(ts_array) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
            Box::new(
                ts_array
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, opt)| opt.map(|v| (idx, TimestampKey(v)))),
            )
        } else if let Some(str_array) = arr.as_any().downcast_ref::<StringArray>() {
            // 从字符串解析时间戳
            Box::new(str_array.iter().enumerate().filter_map(|(idx, opt)| {
                opt.and_then(|s| {
                    parse_scalar_to_timestamp(&ScalarValue::Utf8(Some(s.to_string())))
                        .map(|ts| (idx, TimestampKey(ts)))
                })
            }))
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn from_scalar(value: &datafusion::scalar::ScalarValue) -> Option<Self> {
        // 使用 timestamp 模块的解析函数
        // 自动支持字符串时间、数值时间戳、Arrow 时间类型
        parse_scalar_to_timestamp(value).map(TimestampKey)
    }

    fn new_serializer(zstd_level: i32) -> Self::Serializer {
        TimestampRoaringSerializer::new(zstd_level)
    }

    fn key_len() -> usize {
        8
    }

    fn normalize(&self, _case_sensitive: bool) -> Self {
        *self
    }

    fn supports_range() -> bool {
        true
    }
}

// 实现与 i64 的转换,方便序列化
impl From<TimestampKey> for i64 {
    fn from(key: TimestampKey) -> i64 {
        key.0
    }
}

impl From<i64> for TimestampKey {
    fn from(ts: i64) -> TimestampKey {
        TimestampKey(ts)
    }
}
