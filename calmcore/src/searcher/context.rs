pub struct SearchContext {}

impl SearchContext {
    pub fn new() -> SearchContext {
        SearchContext {}
    }

    pub(crate) fn get(&self, start: u64) -> SegmentContext {
        SegmentContext { start }
    }
}

#[allow(dead_code)]
#[derive(Default)]
pub struct SegmentContext {
    start: u64,
}
