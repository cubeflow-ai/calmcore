use std::{sync::Mutex, time::Duration};

use croaring::{bitmap, Bitmap};

struct Test {
    lock: Mutex<()>,
}

pub fn main() {
    let mut b1 = Bitmap::new();
    let mut b2 = Bitmap::new();

    for i in 0..4_000_000 {
        if i % 2 == 0 {
            b1.add(i);
        }
    }

    for i in 5_000_000..5_000_000 {
        if i % 2 == 0 {
            b2.add(i);
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
    let mut sum = 0;
    for b in b3.iter() {
        if b == 0 {
            sum += b;
        }
    }
    println!("use time : {} sum:{}", start.elapsed().as_millis(), sum);
}
