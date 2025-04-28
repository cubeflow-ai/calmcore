use std::{sync::Mutex, time::Duration};

// use croaring::{bitmap, Bitmap};
use roaring::bitmap::RoaringBitmap as Bitmap;

struct Test {
    lock: Mutex<()>,
}

pub fn main() {
    let mut b1 = Bitmap::new();
    let mut b2 = Bitmap::new();

    for i in 0..4_000_000 {
        if i % 2 == 0 {
            // b1.add(i);
            b1.insert(i);
        }
    }

    for i in 1_000_000..5_000_000 {
        if i % 2 == 0 {
            b2.insert(i);
        }
    }

    let start = std::time::Instant::now();
    let mut sum = 0;
    for b in b1.iter() {
        if b == 0 {
            sum += b;
        }
    }

    for b in b2.iter() {
        if b == 0 {
            sum += b;
        }
    }
    println!("use time : {} sum:{}", start.elapsed().as_millis(), sum);

    let start = std::time::Instant::now();
    let b3 = b1 & b2;
    println!("bs{}", b3.len());
    let mut sum = 0;
    for b in b3.iter() {
        if b == 0 {
            sum += b;
        }
    }
    println!("use time : {} sum:{}", start.elapsed().as_millis(), sum);
}
