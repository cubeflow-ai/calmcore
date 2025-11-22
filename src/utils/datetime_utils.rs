/// 日期时间转换工具
///
/// 支持多种日期时间格式到 Unix 时间戳的转换
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};

#[derive(Debug)]
pub enum DateTimeConversionError {
    InvalidFormat(String),
    ParseError(String),
}

impl std::fmt::Display for DateTimeConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DateTimeConversionError::InvalidFormat(msg) => {
                write!(f, "Invalid date format: {}", msg)
            }
            DateTimeConversionError::ParseError(msg) => write!(f, "Failed to parse date: {}", msg),
        }
    }
}

impl std::error::Error for DateTimeConversionError {}

/// 将日期字符串转换为 Unix 时间戳（毫秒）
///
/// 支持的格式：
/// - ISO 8601 日期: "2025-01-11", "2025-01-11T00:00:00Z"
/// - ISO 8601 日期时间: "2025-01-11T12:34:56Z", "2025-01-11T12:34:56.123Z"
/// - Unix 时间戳（秒）: "1704988800"
/// - Unix 时间戳（毫秒）: "1704988800000"
///
/// # Examples
/// ```
/// use calmcore::utils::datetime_utils::parse_date_to_timestamp_millis;
///
/// // ISO 日期
/// let ts = parse_date_to_timestamp_millis("2025-01-11").unwrap();
///
/// // ISO 日期时间
/// let ts = parse_date_to_timestamp_millis("2025-01-11T12:34:56Z").unwrap();
/// ```
pub fn parse_date_to_timestamp_millis(date_str: &str) -> Result<i64, DateTimeConversionError> {
    let date_str = date_str.trim();

    // 尝试作为 Unix 时间戳解析（秒或毫秒）
    if let Ok(timestamp) = date_str.parse::<i64>() {
        // 如果是秒级时间戳（10位），转换为毫秒
        if timestamp < 10_000_000_000 {
            return Ok(timestamp * 1000);
        }
        // 如果是毫秒级时间戳（13位），直接返回
        if timestamp < 10_000_000_000_000 {
            return Ok(timestamp);
        }
        return Err(DateTimeConversionError::InvalidFormat(
            "Timestamp out of reasonable range".to_string(),
        ));
    }

    // 尝试解析 ISO 8601 日期时间格式
    // 格式1: "2025-01-11T12:34:56Z"
    if let Ok(dt) = DateTime::parse_from_rfc3339(date_str) {
        return Ok(dt.timestamp_millis());
    }

    // 格式2: "2025-01-11T12:34:56.123Z" (带毫秒)
    if date_str.contains('T') {
        if let Ok(dt) = date_str.parse::<DateTime<Utc>>() {
            return Ok(dt.timestamp_millis());
        }
    }

    // 格式3: 纯日期 "2025-01-11" -> 当天 00:00:00 UTC
    if let Ok(naive_date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
        let naive_datetime = naive_date.and_hms_opt(0, 0, 0).ok_or_else(|| {
            DateTimeConversionError::ParseError("Failed to create datetime".to_string())
        })?;
        let dt = Utc.from_utc_datetime(&naive_datetime);
        return Ok(dt.timestamp_millis());
    }

    // 格式4: "2025-01-11 12:34:56" (MySQL风格)
    if let Ok(naive_datetime) = NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S") {
        let dt = Utc.from_utc_datetime(&naive_datetime);
        return Ok(dt.timestamp_millis());
    }

    // 格式5: "2025/01/11" (斜杠分隔)
    if let Ok(naive_date) = NaiveDate::parse_from_str(date_str, "%Y/%m/%d") {
        let naive_datetime = naive_date.and_hms_opt(0, 0, 0).ok_or_else(|| {
            DateTimeConversionError::ParseError("Failed to create datetime".to_string())
        })?;
        let dt = Utc.from_utc_datetime(&naive_datetime);
        return Ok(dt.timestamp_millis());
    }

    Err(DateTimeConversionError::InvalidFormat(format!(
        "Unable to parse date string: {}",
        date_str
    )))
}

/// 检查字符串是否看起来像日期格式
/// 用于快速判断是否需要进行日期转换
pub fn looks_like_date(s: &str) -> bool {
    let s = s.trim();

    // 检查是否为纯数字（可能是时间戳）
    if s.chars().all(|c| c.is_ascii_digit()) {
        // 10位或13位数字可能是时间戳
        let len = s.len();
        return len == 10 || len == 13;
    }

    // 检查常见日期格式模式
    // ISO 8601: "2025-01-11", "2025-01-11T12:34:56Z"
    if s.contains('-') && s.len() >= 10 {
        return true;
    }

    // 斜杠日期: "2025/01/11"
    if s.contains('/') && s.len() >= 10 {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_iso_date() {
        // 测试纯日期格式
        let ts = parse_date_to_timestamp_millis("2025-01-11").unwrap();
        assert!(ts > 0);

        // 2025-01-11 00:00:00 UTC = 1736553600000 毫秒
        assert_eq!(ts, 1736553600000);
    }

    #[test]
    fn test_parse_iso_datetime() {
        // 测试 ISO 8601 日期时间
        let ts = parse_date_to_timestamp_millis("2025-01-11T12:34:56Z").unwrap();
        assert!(ts > 0);

        // 2025-01-11 12:34:56 UTC
        assert_eq!(ts, 1736598896000);
    }

    #[test]
    fn test_parse_local_datetime() {
        let ts = parse_date_to_timestamp_millis("2025-01-11 12:34:56").unwrap();
        assert!(ts > 0);

        // 2025-01-11 12:34:56 UTC
        assert_eq!(ts, 1736598896000);
    }

    #[test]
    fn test_parse_iso_datetime_with_millis() {
        // 测试带毫秒的 ISO 8601
        let ts = parse_date_to_timestamp_millis("2025-01-11T12:34:56.123Z").unwrap();
        assert!(ts > 0);

        assert_eq!(ts, 1736598896123);
    }

    #[test]
    fn test_parse_unix_timestamp_seconds() {
        // 测试秒级时间戳
        let ts = parse_date_to_timestamp_millis("1736553600").unwrap();
        assert_eq!(ts, 1736553600000);
    }

    #[test]
    fn test_parse_unix_timestamp_millis() {
        // 测试毫秒级时间戳
        let ts = parse_date_to_timestamp_millis("1736553600000").unwrap();
        assert_eq!(ts, 1736553600000);
    }

    #[test]
    fn test_parse_mysql_style_datetime() {
        // 测试 MySQL 风格日期时间
        let ts = parse_date_to_timestamp_millis("2025-01-11 12:34:56").unwrap();
        assert_eq!(ts, 1736598896000);
    }

    #[test]
    fn test_parse_slash_date() {
        // 测试斜杠分隔的日期
        let ts = parse_date_to_timestamp_millis("2025/01/11").unwrap();
        assert_eq!(ts, 1736553600000);
    }

    #[test]
    fn test_parse_invalid_date() {
        let result = parse_date_to_timestamp_millis("not-a-date");
        assert!(result.is_err());
    }

    #[test]
    fn test_looks_like_date() {
        assert!(looks_like_date("2025-01-11"));
        assert!(looks_like_date("2025-01-11T12:34:56Z"));
        assert!(looks_like_date("2025/01/11"));
        assert!(looks_like_date("1736553600")); // 10位时间戳
        assert!(looks_like_date("1736553600000")); // 13位时间戳

        assert!(!looks_like_date("123")); // 太短
        assert!(!looks_like_date("hello"));
        assert!(!looks_like_date("12345")); // 5位数字
    }

    #[test]
    fn test_date_range() {
        // 测试日期范围查询的场景
        let start = parse_date_to_timestamp_millis("2025-01-11").unwrap();
        let end = parse_date_to_timestamp_millis("2025-01-12").unwrap();

        assert_eq!(start, 1736553600000); // 2025-01-11 00:00:00
        assert_eq!(end, 1736640000000); // 2025-01-12 00:00:00
        assert!(end > start);
        assert_eq!(end - start, 86400000); // 24小时的毫秒数
    }
}
