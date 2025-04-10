use std::{
    collections::HashMap,
    sync::{atomic::AtomicU32, Mutex},
};

use croaring::Bitmap;

use crate::index_store::segment::SegmentReader;

pub type CacheKey = String;

pub struct SearchContext {
    segment_contexts: HashMap<u64, Mutex<SegmentContext>>,
}

impl SearchContext {
    pub fn new(segments: &[SegmentReader]) -> SearchContext {
        let segment_contexts = segments.iter().fold(HashMap::new(), |mut ctx, segment| {
            ctx.insert(segment.start(), Mutex::new(SegmentContext::new()));
            ctx
        });
        SearchContext { segment_contexts }
    }

    pub(crate) fn get(&self, start: u64) -> Option<&Mutex<SegmentContext>> {
        self.segment_contexts.get(&start)
    }
}

#[derive(Default)]
pub struct SegmentContext {
    cache_id: HashMap<CacheKey, u32>,
    cache_value: HashMap<u32, Bitmap>,
    sequence: AtomicU32,
}

impl SegmentContext {
    pub fn new() -> SegmentContext {
        SegmentContext {
            ..Default::default()
        }
    }

    pub fn insert(&mut self, key: String, bm: Bitmap) -> u32 {
        let id = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.cache_id.insert(key, id);
        self.cache_value.insert(id, bm);
        id
    }

    pub fn get(&self, key: &String) -> Option<u32> {
        self.cache_id.get(key).cloned()
    }

    pub fn value_insert(&mut self, bm: Bitmap) -> u32 {
        let id = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.cache_value.insert(id, bm);
        id
    }

    pub fn value_get(&self, id: u32) -> Bitmap {
        self.cache_value.get(&id).cloned().unwrap()
    }

    pub fn value_ref_get(&self, id: u32) -> &Bitmap {
        self.cache_value.get(&id).unwrap()
    }

    pub fn and_value(&mut self, id: u32, filter: &Bitmap) -> &Bitmap {
        // self.cache_value.get(&id).unwrap() & filter
        let v = self.cache_value.get_mut(&id).unwrap();
        v.and_inplace(filter);
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_and_value() {
        // 创建一个新的 SegmentContext
        let mut context = SegmentContext::new();

        // 创建两个 Bitmap 用于测试
        let mut bm1 = Bitmap::new();
        bm1.add(1);
        bm1.add(2);
        bm1.add(3);
        bm1.add(4);

        let mut filter = Bitmap::new();
        filter.add(2);
        filter.add(3);
        filter.add(5);

        // 插入第一个 bitmap 并获取 id
        let id = context.value_insert(bm1);

        // 执行 and_value 操作
        let result = context.and_value(id, &filter);

        // 验证结果
        assert_eq!(result.cardinality(), 2); // 应该只有2个元素（2和3）
        assert!(result.contains(2));
        assert!(result.contains(3));
        assert!(!result.contains(1));
        assert!(!result.contains(4));
        assert!(!result.contains(5));

        // 验证原始数据也被修改了（因为是 in-place 操作）
        let stored = context.value_get(id);
        assert_eq!(stored.cardinality(), 2);
        assert!(stored.contains(2));
        assert!(stored.contains(3));
    }
}
