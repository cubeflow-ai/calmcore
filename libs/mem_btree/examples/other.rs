use std::collections::BTreeMap;

use mem_btree::BTree;

pub fn main() {
    let mut btree1 = BTree::new(32);
    btree1.put("a".to_string(), 1);
    let v = btree1.get("a");
    println!("{:?}", v);

    btree1.split_off("a");

    let mut btree2 = BTreeMap::new();
    btree2.insert("a".to_string(), 1);
    let v = btree2.get("a");
    println!("{:?}", v);
}
