use std::{cmp::Ordering, collections::BinaryHeap};

use faiss::MetricType;
use itertools::Itertools;
use mem_btree::{BTree, BatchWrite};

use crate::util::{CoreError, CoreResult};

pub struct MemoryVectorIndexReader {
    start: u64,
    metric: MetricType,
    dimension: usize,
    index: BTree<u64, Vec<f32>>,
}

impl MemoryVectorIndexReader {
    pub fn new(
        start: u64,
        metric: MetricType,
        dimension: usize,
        index: BTree<u64, Vec<f32>>,
    ) -> Self {
        Self {
            start,
            metric,
            dimension,
            index,
        }
    }

    pub fn search(&self, query: &[f32], size: usize) -> CoreResult<Vec<(f32, u64)>> {
        let mut heap = BinaryHeap::new();
        for item in self.index.iter() {
            let distance = match self.metric {
                MetricType::InnerProduct => dot_product(&item.1, query)?,
                MetricType::L2 => euclidean_distance(&item.1, query)?,
            };
            heap.push((ComparableF32(distance), item.0));

            if heap.len() > size {
                heap.pop();
            }
        }

        Ok(heap.into_iter().map(|(s, i)| (s.0, i)).collect_vec())
    }
}

#[derive(PartialEq, PartialOrd)]
struct ComparableF32(f32);

impl Eq for ComparableF32 {} // 需要同时实现 Eq

impl Ord for ComparableF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Less)
    }
}

fn dot_product(a: &[f32], b: &[f32]) -> CoreResult<f32> {
    super::check_vector_size(a, b)?;

    #[cfg(feature = "simd")]
    {
        use packed_simd::f32x16;
        let size = a.len() - (a.len() % 16);
        let c = a
            .chunks_exact(16)
            .map(f32x16::from_slice_unaligned)
            .zip(b.chunks_exact(16).map(f32x16::from_slice_unaligned))
            .map(|(a, b)| a * b)
            .sum::<f32x16>()
            .sum();
        let d = a[size..].iter().zip(&b[size..]).map(|(p, q)| p * q).sum();
        Ok(-(c + d))
    }
    #[cfg(not(feature = "simd"))]
    {
        Ok(-(a.iter().zip(b).map(|(p, q)| p * q).sum::<f32>()))
    }
}

fn euclidean_distance(a: &[f32], b: &[f32]) -> CoreResult<f32> {
    super::check_vector_size(a, b)?;

    #[cfg(feature = "simd")]
    {
        use packed_simd::f32x16;
        let size = a.len() - (a.len() % 16);
        let c = a
            .chunks_exact(16)
            .map(f32x16::from_slice_unaligned)
            .zip(b.chunks_exact(16).map(f32x16::from_slice_unaligned))
            .map(|(a, b)| {
                let c = (a - b);
                c * c
            })
            .sum::<f32x16>()
            .sum();

        let d = a[size..]
            .iter()
            .zip(&b[size..])
            .map(|(p, q)| (p - q).powi(2))
            .sum();
        Ok((d + c))
    }
    #[cfg(not(feature = "simd"))]
    {
        Ok(a.iter().zip(b).map(|(p, q)| (p - q).powi(2)).sum())
    }
}
