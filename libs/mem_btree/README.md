# rust mem_btree 

A Data Structure of BTree Implemented with Rust, support snapshot. not use any unsafe lib.

[https://crates.io/crates/mem_btree](https://crates.io/crates/mem_btree)

## Design:

> 
Although rust officially provides the BTreeMap library. But this library can not achieve copy/read on write, but also can not achieve snapshot, although you can use clone instead of but, clone's price is too expensive, so this project through a very simple way to achieve a snapshot of the BTree structure.
The main idea is to use Arc in a freewheeling way, and then in the process of writing, clone all the pathway nodes, although this will cause the insertion speed to slow down. But compared to the memory operation of the slow is also limited slow.


## future:
* snapshot ✅
* split_off ✅
* put ✅
* delete ✅
* get ✅
* seek ✅
* seek_prev ✅
* prev iter ✅
* next iter ✅
* batch_write ✅
* ttl ✅

## bench
5k kv insert
````
btree insert 120.064954ms
btreemap insert 73.882981ms
btreemap_arc insert 79.869725ms

btree get 102.721024ms
btreemap get 100.939223ms
btreemap_arc get 100.255662ms

btree clone 759ns
btreemap clone 453.955778ms
btreemap_arc clone 24.776548ms
````