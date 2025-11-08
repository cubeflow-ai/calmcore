/// Wrapper for f64 that implements Ord by comparing bit patterns
/// NaN values are treated as equal and greater than all other values
#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub struct OrderedF64(pub f64);

impl Eq for OrderedF64 {}

impl std::hash::Hash for OrderedF64 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Use the bit pattern for hashing, treating NaN consistently
        if self.0.is_nan() {
            // All NaNs hash to the same value
            state.write_u64(0x7FF8000000000000u64);
        } else {
            state.write_u64(self.0.to_bits());
        }
    }
}

impl Ord for OrderedF64 {
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

impl From<f64> for OrderedF64 {
    fn from(f: f64) -> Self {
        OrderedF64(f)
    }
}

impl From<OrderedF64> for f64 {
    fn from(o: OrderedF64) -> Self {
        o.0
    }
}
