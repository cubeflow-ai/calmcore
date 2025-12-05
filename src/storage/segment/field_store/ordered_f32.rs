/// Wrapper for f32 that implements Ord by comparing bit patterns
/// NaN values are treated as equal and greater than all other values
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug)]
pub struct OrderedF32(pub f32);

impl Eq for OrderedF32 {}

impl std::hash::Hash for OrderedF32 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Use the bit pattern for hashing, treating NaN consistently
        if self.0.is_nan() {
            // All NaNs hash to the same value
            state.write_u32(0x7FC00000u32);
        } else {
            state.write_u32(self.0.to_bits());
        }
    }
}

impl Ord for OrderedF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or_else(|| {
            // Handle NaN: NaN == NaN, NaN > everything else
            match (self.0.is_nan(), other.0.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => unreachable!(),
            }
        })
    }
}

impl From<f32> for OrderedF32 {
    fn from(f: f32) -> Self {
        OrderedF32(f)
    }
}

impl From<OrderedF32> for f32 {
    fn from(o: OrderedF32) -> Self {
        o.0
    }
}
