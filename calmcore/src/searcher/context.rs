use croaring::Bitmap;

use super::plan::Query;

#[derive(Debug)]
pub struct SearchContext {
    pub need_filter: bool,
    pub need_stream: bool,
}

impl SearchContext {
    pub fn new(query: Option<&Query>, order_by: &Vec<(String, bool)>) -> SearchContext {
        let mut need_filter = false;
        let mut need_stream = false;

        if !order_by.is_empty() {
            need_stream = true;
        }

        if let Some(query) = query {
            let mut _has_phrase = false;
            let mut has_vector = false;
            query.visiter(&mut _has_phrase, &mut has_vector);

            if has_vector {
                need_filter = true;
            }
        }

        if !need_stream {
            need_filter = true;
        }

        SearchContext {
            need_filter,
            need_stream,
        }
    }

    pub(crate) fn get(&self, start: u64) -> SegmentContext {
        SegmentContext {
            start,
            bitmaps: Vec::new(),
        }
    }
}

#[allow(dead_code)]
#[derive(Default)]
pub struct SegmentContext {
    start: u64,
    bitmaps: Vec<Bitmap>,
}

impl SegmentContext {
    pub fn make_iter(&mut self, bitmap: Bitmap) -> croaring::bitmap::BitmapIterator<'_> {
        self.bitmaps.push(bitmap);
        let iter = self.bitmaps.last().unwrap().iter();
        iter
    }
}
