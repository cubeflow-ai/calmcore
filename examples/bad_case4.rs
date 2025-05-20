use std::{sync::Mutex, time::Duration};

use croaring::{bitmap, Bitmap};
// use roaring::bitmap::RoaringBitmap as Bitmap;

pub fn main() {
    let mut b1 = Bitmap::new();
    let mut b2 = Bitmap::new();

    let start = std::time::Instant::now();

    for i in 0..4_000_000 {
        // b1.add(i);
        b1.add(i);
    }

    for i in 1_000_000..5_000_000 {
        b2.add(i);
    }

    println!("use time : {}", start.elapsed().as_millis());

    let mut v1 = Vec::new();
    let mut v2 = Vec::new();

    let start = std::time::Instant::now();

    for i in 0..4_000_000 {
        // b1.add(i);
        v1.push(i);
    }

    for i in 1_000_000..5_000_000 {
        v2.push(i);
    }

    println!("use time : {}", start.elapsed().as_millis());

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

    let mut sum = 0;
    for b in b3.iter() {
        if b == 0 {
            sum += b;
        }
    }
    println!("use time : {} sum:{}", start.elapsed().as_millis(), sum);
}
