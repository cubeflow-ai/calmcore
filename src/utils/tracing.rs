//! 查询链路追踪模块
//!
//! 提供详细的查询执行过程追踪,记录每个步骤的耗时

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// 全局追踪开关
static TRACING_ENABLED: RwLock<bool> = RwLock::new(false);

/// 追踪上下文
#[derive(Clone)]
pub struct TraceContext {
    query_id: String,
    start_time: Instant,
    spans: Arc<RwLock<Vec<TraceSpan>>>,
}

/// 追踪跨度(一个步骤)
#[derive(Debug, Clone)]
pub struct TraceSpan {
    pub name: String,
    pub start: Duration,
    pub duration: Duration,
    pub parent: Option<usize>,
    pub metadata: Vec<(String, String)>,
}

impl TraceContext {
    /// 创建新的追踪上下文
    pub fn new(query_id: String) -> Self {
        Self {
            query_id,
            start_time: Instant::now(),
            spans: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// 开始一个新的跨度
    pub fn span(&self, name: impl Into<String>) -> TraceGuard {
        if !is_tracing_enabled() {
            return TraceGuard::disabled();
        }

        let start = self.start_time.elapsed();
        let span_id = {
            let mut spans = self.spans.write().unwrap();
            let id = spans.len();
            spans.push(TraceSpan {
                name: name.into(),
                start,
                duration: Duration::ZERO,
                parent: None,
                metadata: Vec::new(),
            });
            id
        };

        TraceGuard {
            context: Some(self.clone()),
            span_id,
            start_instant: Instant::now(),
        }
    }

    /// 添加元数据到当前跨度
    pub fn add_metadata(&self, span_id: usize, key: impl Into<String>, value: impl Into<String>) {
        if !is_tracing_enabled() {
            return;
        }

        let mut spans = self.spans.write().unwrap();
        if let Some(span) = spans.get_mut(span_id) {
            span.metadata.push((key.into(), value.into()));
        }
    }

    /// 完成跨度记录
    fn finish_span(&self, span_id: usize, duration: Duration) {
        let mut spans = self.spans.write().unwrap();
        if let Some(span) = spans.get_mut(span_id) {
            span.duration = duration;
        }
    }

    /// 打印追踪报告
    pub fn report(&self) {
        if !is_tracing_enabled() {
            return;
        }

        let spans = self.spans.read().unwrap();
        let total_time = self.start_time.elapsed();

        log::info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log::info!("📊 Query Trace Report: {}", self.query_id);
        log::info!("   Total Time: {:.3}ms", total_time.as_secs_f64() * 1000.0);
        log::info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        for (i, span) in spans.iter().enumerate() {
            let start_ms = span.start.as_secs_f64() * 1000.0;
            let duration_ms = span.duration.as_secs_f64() * 1000.0;
            let percentage = if total_time.as_millis() > 0 {
                (span.duration.as_millis() as f64 / total_time.as_millis() as f64) * 100.0
            } else {
                0.0
            };

            log::info!(
                "  [{:3}] {:40} | +{:8.3}ms | {:8.3}ms ({:5.1}%)",
                i,
                span.name,
                start_ms,
                duration_ms,
                percentage
            );

            // 打印元数据
            for (key, value) in &span.metadata {
                log::info!("        └─ {}: {}", key, value);
            }
        }

        log::info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }
}

/// 追踪守卫,自动记录跨度结束时间
pub struct TraceGuard {
    context: Option<TraceContext>,
    span_id: usize,
    start_instant: Instant,
}

impl TraceGuard {
    fn disabled() -> Self {
        Self {
            context: None,
            span_id: 0,
            start_instant: Instant::now(),
        }
    }

    /// 添加元数据
    pub fn metadata(&self, key: impl Into<String>, value: impl Into<String>) {
        if let Some(ctx) = &self.context {
            ctx.add_metadata(self.span_id, key, value);
        }
    }

    /// 获取跨度ID
    pub fn span_id(&self) -> usize {
        self.span_id
    }
}

impl Drop for TraceGuard {
    fn drop(&mut self) {
        if let Some(ctx) = &self.context {
            let duration = self.start_instant.elapsed();
            ctx.finish_span(self.span_id, duration);
        }
    }
}

/// 启用追踪
pub fn enable_tracing() {
    *TRACING_ENABLED.write().unwrap() = true;
    log::info!("🔍 Query tracing enabled");
}

/// 禁用追踪
pub fn disable_tracing() {
    *TRACING_ENABLED.write().unwrap() = false;
    log::info!("🔍 Query tracing disabled");
}

/// 检查追踪是否启用
pub fn is_tracing_enabled() -> bool {
    *TRACING_ENABLED.read().unwrap()
}

/// 切换追踪状态
pub fn toggle_tracing() -> bool {
    let mut enabled = TRACING_ENABLED.write().unwrap();
    *enabled = !*enabled;
    let new_state = *enabled;
    drop(enabled);

    if new_state {
        log::info!("🔍 Query tracing enabled");
    } else {
        log::info!("🔍 Query tracing disabled");
    }

    new_state
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_trace_context() {
        enable_tracing();

        let ctx = TraceContext::new("test-query-1".to_string());

        {
            let _span1 = ctx.span("step1");
            thread::sleep(Duration::from_millis(10));
        }

        {
            let span2 = ctx.span("step2");
            span2.metadata("rows", "100");
            thread::sleep(Duration::from_millis(5));
        }

        ctx.report();

        let spans = ctx.spans.read();
        assert_eq!(spans.len(), 2);
        assert!(spans[0].duration.as_millis() >= 10);
        assert!(spans[1].duration.as_millis() >= 5);
    }
}
