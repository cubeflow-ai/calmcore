use std::{collections::BTreeMap, sync::Arc};

use mem_btree::BTree;

fn main() {
    let mut btree = BTree::new(32);

    let mut btreemap = BTreeMap::new();
    let mut btreemap_arc = BTreeMap::new();

    let num = 10240;

    let data: Vec<Vec<usize>> = (0..num).map(|i| vec![i; 128 * 5]).collect();

    //============ insert test
    let now = std::time::Instant::now();

    for i in data.iter() {
        btree.put(i.clone(), i.clone());
    }
    println!("btree insert {:?}", now.elapsed());

    let now = std::time::Instant::now();

    for i in data.iter() {
        btreemap.insert(i.clone(), i.clone());
    }
    println!("btreemap insert {:?}", now.elapsed());

    let now = std::time::Instant::now();

    for i in data.iter() {
        btreemap_arc.insert(Arc::new(i.clone()), Arc::new(i.clone()));
    }
    println!("btreemap_arc insert {:?}", now.elapsed());

    //============ get test

    let now = std::time::Instant::now();

    for i in data.iter() {
        btree.get(i);
    }
    println!("btree get {:?}", now.elapsed());

    let now = std::time::Instant::now();

    for i in data.iter() {
        btreemap.get(i);
    }
    println!("btreemap get {:?}", now.elapsed());

    let now = std::time::Instant::now();

    for i in data.iter() {
        btreemap_arc.get(i);
    }
    println!("btreemap_arc get {:?}", now.elapsed());

    // ============ clone test

    let now = std::time::Instant::now();
    for i in 0..10 {
        if btree.clone().len() - i > num {
            println!("{}", i);
        };
    }
    println!("btree clone {:?}", now.elapsed());

    let now = std::time::Instant::now();
    for i in 0..10 {
        if btreemap.clone().len() - i > num {
            println!("{}", i);
        };
    }
    println!("btreemap clone {:?}", now.elapsed());

    let now = std::time::Instant::now();
    for i in 0..10 {
        if btreemap_arc.clone().len() - i > num {
            println!("{}", i);
        };
    }
    println!("btreemap_arc clone {:?}", now.elapsed());
}
