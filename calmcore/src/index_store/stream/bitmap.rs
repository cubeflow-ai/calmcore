use croaring::{bitmap::BitmapIterator, Bitmap};

pub struct BitmapStream {
    start: u64,
    bm: Bitmap,
    value: Option<u32>,
    score: f32,
    end: bool,
}

impl BitmapStream {
    pub fn new(start: u64, bm: Bitmap, score: f32) -> Self {
        Self {
            start,
            bm,
            value: None,
            score,
            end: false,
        }
    }
}

pub struct BitmapStreamIter<'a> {
    stream: &'a mut BitmapStream,
    iter: BitmapIterator<'a>,
}

impl<'a> BitmapStreamIter<'a> {
    fn next(&mut self) {
        if self.stream.end {
            return;
        }
        self.stream.value = self.iter.next();
        if self.stream.value.is_none() {
            self.stream.end = true;
        }
    }
    fn value(&self) -> Option<u64> {
        Some(self.stream.value? as u64 + self.stream.start)
    }

    fn score(&self) -> f32 {
        self.stream.score
    }

    fn next_value(&mut self, value: u64) -> Option<u64> {
        let value = (value - self.stream.start) as u32;
        loop {
            let v = match self.stream.value {
                Some(v) => v,
                None => self.iter.next()?,
            };
            if v >= value {
                return Some(v as u64 + self.stream.start);
            }
            self.iter.next();
        }
    }
}
