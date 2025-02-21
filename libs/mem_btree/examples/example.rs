use mem_btree::{BTree, BatchWrite};
use rand::seq::SliceRandom;

pub fn main() {
    // let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    // let mut pairs = Vec::new();
    // for _ in 0..128 {
    //     let key = rng.gen::<u64>();
    //     let value = rng.gen::<u64>();
    //     pairs.push((key, value));
    // }

    // let mut btree = BTree::new(4);

    // pairs.chunks(32).for_each(|c| {
    //     let mut bw = BatchWrite::new();
    //     for v in c {
    //         bw.put(v.0, v.1);
    //     }
    //     btree.write(bw);
    // });

    let mut btree = BTree::new(4);

    let mut vec = {
        let mut v = vec![];
        for i in 0..32 {
            v.push(i * 2);
        }

        v
    };

    vec.shuffle(&mut rand::thread_rng());

    vec.chunks(4).for_each(|c| {
        let mut bw = BatchWrite::default();
        for v in c {
            bw.put(*v, *v);
        }
        btree.write(bw);
    });

    println!("{:?}", btree.len());

    for i in vec.iter() {
        assert!(btree.get(i).is_some());
    }

    // println!("{:?}", pairs);

    // prev_seek_next();
    // seek_next();
    // prev_iter();
    // next_iter();
    // split_off();
    // remove();
}

pub fn prev_seek_next() {
    let num = 32;
    let mut datas = (0..num).into_iter().map(|v| v * 2).collect::<Vec<_>>();

    datas.shuffle(&mut rand::thread_rng());

    let mut btree = mem_btree::BTree::new(4);

    for i in datas.iter() {
        btree.put(i.clone(), i.clone());
    }

    let mut iter = btree.iter();

    iter.seek_prev(&10);

    while let Some(item) = iter.prev() {
        println!("k: {:?} v: {:?}", item.0, item.1);
    }

    println!("-------------------");

    iter.seek_prev(&9);

    while let Some(item) = iter.prev() {
        println!("k: {:?} v: {:?}", item.0, item.1);
    }
}

pub fn seek_next() {
    let num = 32;
    let mut datas = (0..num).into_iter().map(|v| v * 2).collect::<Vec<_>>();

    datas.shuffle(&mut rand::thread_rng());

    let mut btree = mem_btree::BTree::new(4);

    for i in datas.iter() {
        btree.put(i.clone(), i.clone());
    }

    let mut iter = btree.iter();

    iter.seek(&10);

    while let Some(item) = iter.next() {
        println!("k: {:?} v: {:?}", item.0, item.1);
    }

    println!("-------------------");

    iter.seek(&9);

    while let Some(item) = iter.next() {
        println!("k: {:?} v: {:?}", item.0, item.1);
    }
}

pub fn prev_iter() {
    let num = 1024;
    let mut datas = (0..num).into_iter().map(|v| v).collect::<Vec<_>>();

    datas.shuffle(&mut rand::thread_rng());

    let mut btree = mem_btree::BTree::new(32);

    for i in datas.iter() {
        btree.put(i.clone(), i.clone());
    }

    let mut iter = btree.iter();

    while let Some(item) = iter.prev() {
        println!("k: {:?} v: {:?}", item.0, item.1);
    }
}

pub fn next_iter() {
    let num = 1024;
    let mut datas = (0..num).into_iter().map(|v| v).collect::<Vec<_>>();

    datas.shuffle(&mut rand::thread_rng());

    let mut btree = mem_btree::BTree::new(32);

    for i in datas.iter() {
        btree.put(i.clone(), i.clone());
    }

    println!("{:#?}", btree);

    let mut iter = btree.iter();

    while let Some(item) = iter.next() {
        println!("k: {:?} v: {:?}", item.0, item.1);
    }
}

pub fn split_off() {
    for k in 0..33 {
        let num = 32;
        let mut datas = (0..num).into_iter().map(|v| v).collect::<Vec<_>>();

        datas.shuffle(&mut rand::thread_rng());

        let mut btree = mem_btree::BTree::new(4);

        for i in datas.iter() {
            btree.put(i.clone(), i.clone());
        }

        let right = btree.split_off(&k);

        println!("k:{} left: {:?} right: {:?}", k, btree.len(), right.len());
    }
}

pub fn remove() {
    let num = 1024 * 1024;
    let mut datas = (0..num).into_iter().map(|v| v).collect::<Vec<_>>();

    datas.shuffle(&mut rand::thread_rng());

    let mut btree = mem_btree::BTree::new(4);

    for i in datas.iter() {
        btree.put(i.clone(), i.clone());
    }

    println!("{:?}", btree.len());

    for i in datas.iter() {
        btree.remove(i);
    }

    println!("{:?}", btree);
}
