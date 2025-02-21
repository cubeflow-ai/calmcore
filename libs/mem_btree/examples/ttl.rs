use std::time::Duration;

use mem_btree::{BTree, BatchWrite};

fn main() {
    let mut btree = BTree::new(64);

    for j in 1..11 {
        for i in 1..10001 {
            btree.put_ttl(i * 100000 + j, j * 10, Duration::from_secs(2 * j));
        }
    }

    for _ in 0..10 {
        println!("before {}", btree.len());
        std::thread::sleep(Duration::from_secs(2));
        let now = std::time::Instant::now();
        btree = btree.expir();
        println!("expr use time:{:#?}", now.elapsed());
        println!("end {}", btree.len());
    }

    println!("-------------------------");

    let mut btree = BTree::new(128);

    for j in 1..11 {
        let mut batch = BatchWrite::default();
        for i in 1..10001 {
            batch.put_ttl(i * 100000 + j, j * 10, Duration::from_secs(2 * j));
        }
        btree.write(batch);
    }

    for _ in 0..10 {
        println!("before {}", btree.len());
        std::thread::sleep(Duration::from_secs(2));
        let now = std::time::Instant::now();
        btree = btree.expir();
        println!("expr use time:{:#?}", now.elapsed());
        println!("end {}", btree.len());
    }
}
