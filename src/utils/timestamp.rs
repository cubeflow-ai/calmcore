/// Timestamp 工具模块
///
/// 提供时间戳解析和转换功能,支持 SQL 和 ES 查询
use crate::utils::datetime_utils::parse_date_to_timestamp_millis;
use datafusion::scalar::ScalarValue;

/// 解析 ScalarValue 为时间戳(毫秒)
///
/// 支持以下类型:
/// - Int64: 直接作为时间戳(自动判断秒/毫秒)
/// - Utf8/LargeUtf8: 日期时间字符串
/// - Date32: Arrow Date32 类型
/// - Timestamp: Arrow Timestamp 类型
///
/// # Examples
/// ```ignore
/// use datafusion::scalar::ScalarValue;
///
/// // 数值时间戳
/// let ts = parse_scalar_to_timestamp(&ScalarValue::Int64(Some(1704096000000))).unwrap();
///
/// // 字符串时间
/// let ts = parse_scalar_to_timestamp(&ScalarValue::Utf8(Some("2024-01-01T10:00:00Z".into()))).unwrap();
/// ```
pub fn parse_scalar_to_timestamp(value: &ScalarValue) -> Option<i64> {
    match value {
        // 数值型时间戳
        ScalarValue::Int64(Some(ts)) => Some(normalize_timestamp(*ts)),
        ScalarValue::Int32(Some(ts)) => Some(normalize_timestamp(*ts as i64)),
        ScalarValue::UInt64(Some(ts)) => Some(normalize_timestamp(*ts as i64)),
        ScalarValue::UInt32(Some(ts)) => Some(normalize_timestamp(*ts as i64)),

        // 字符串型日期时间
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            parse_date_to_timestamp_millis(s).ok()
        }

        // Arrow Date32 类型 (天数,相对 1970-01-01)
        ScalarValue::Date32(Some(days)) => {
            let ts_sec = *days as i64 * 86400; // 转为秒
            Some(ts_sec * 1000) // 转为毫秒
        }

        // Arrow Timestamp 类型
        ScalarValue::TimestampSecond(Some(ts), _) => Some(*ts * 1000),
        ScalarValue::TimestampMillisecond(Some(ts), _) => Some(*ts),
        ScalarValue::TimestampMicrosecond(Some(ts), _) => Some(*ts / 1000),
        ScalarValue::TimestampNanosecond(Some(ts), _) => Some(*ts / 1_000_000),

        _ => None,
    }
}

/// 标准化时间戳为毫秒
///
/// 自动判断输入是秒还是毫秒:
/// - 小于 10,000,000,000 (2286-11-20) -> 秒级,转为毫秒
/// - 否则认为是毫秒级
pub fn normalize_timestamp(ts: i64) -> i64 {
    if ts < 10_000_000_000 {
        ts * 1000 // 秒 -> 毫秒
    } else {
        ts // 已经是毫秒
    }
}

/// 时间戳格式化为字符串(可选功能)
#[allow(dead_code)]
pub fn format_timestamp(ts_millis: i64, format: Option<&str>) -> String {
    use chrono::{DateTime, Utc};

    let datetime = DateTime::from_timestamp_millis(ts_millis).unwrap_or_else(|| Utc::now());

    match format {
        Some("iso8601") | Some("rfc3339") | None => datetime.to_rfc3339(),
        Some(fmt) => {
            // 自定义格式支持(可扩展)
            datetime.format(fmt).to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_scalar_timestamp() {
        // 毫秒级时间戳
        let ts = parse_scalar_to_timestamp(&ScalarValue::Int64(Some(1704096000000))).unwrap();
        assert_eq!(ts, 1704096000000);

        // 秒级时间戳(自动转换)
        let ts = parse_scalar_to_timestamp(&ScalarValue::Int64(Some(1704096000))).unwrap();
        assert_eq!(ts, 1704096000000);

        // ISO 8601 字符串
        let ts = parse_scalar_to_timestamp(&ScalarValue::Utf8(Some("2024-01-01T10:00:00Z".into())))
            .unwrap();
        assert_eq!(ts, 1704096000000);

        // 日期字符串
        let ts = parse_scalar_to_timestamp(&ScalarValue::Utf8(Some("2024-01-01".into()))).unwrap();
        assert_eq!(ts, 1704067200000); // 2024-01-01 00:00:00 UTC
    }

    #[test]
    fn test_normalize_timestamp() {
        // 秒级 -> 毫秒
        assert_eq!(normalize_timestamp(1704096000), 1704096000000);

        // 已经是毫秒
        assert_eq!(normalize_timestamp(1704096000000), 1704096000000);
    }
}
