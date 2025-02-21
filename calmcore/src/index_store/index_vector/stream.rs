use proto::core::Hit;

use crate::index_store::HitStream;

pub struct VectorHitStream {
    value: i32,
    hits: Vec<Hit>,
    score: f32,
}

impl VectorHitStream {
    pub fn new(mut hits: Vec<Hit>, score: f32) -> Self {
        hits.sort_by(|v1, v2| v1.id.cmp(&v2.id));
        Self {
            value: 0,
            hits,
            score,
        }
    }
}

impl HitStream for VectorHitStream {
    fn next(&mut self) {
        self.value += 1;
    }

    fn value(&self) -> Option<u32> {
        self.hits.get(self.value as usize).map(|h| h.id)
    }

    fn score(&self) -> f32 {
        self.hits
            .get(self.value as usize)
            .map(|h| h.score)
            .unwrap_or(0.0)
            * self.score
    }
}
