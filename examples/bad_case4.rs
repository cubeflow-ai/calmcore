use std::time::Duration;

use croaring::Bitmap;

pub fn main() {
    let kc: u32 = 100_000;
    let ic = 10000;

    // println!("bit");
    // bit_memory(kc, ic);

    std::thread::sleep(Duration::from_secs(3));

    println!("arr");
    arr_memory(kc, ic);
}

fn arr_memory(kc: u32, ic: u32) {
    let mut free_memory = 0;
    match sys_info::mem_info() {
        Ok(mem) => {
            free_memory = mem.free;
        }
        Err(e) => println!("Error: {}", e),
    }

    let mut arr = Vec::with_capacity(kc as usize);

    for i in 0..kc {
        let mut bit = Vec::with_capacity(ic as usize);
        for j in 0..ic {
            bit.push(j * i);
        }
        arr.push(bit);
    }

    match sys_info::mem_info() {
        Ok(mem) => {
            println!("总内存: {} G", mem.total / 1024 / 1024);
            println!("可用内存: {} G", mem.avail / 1024 / 1024);
            println!("使用内存: {} MB", (free_memory - mem.free) / 1024);
        }
        Err(e) => println!("Error: {}", e),
    }

    println!("{:?}", arr.len());
}

fn bit_memory(kc: u32, ic: u32) {
    let mut free_memory = 0;
    match sys_info::mem_info() {
        Ok(mem) => {
            free_memory = mem.free;
        }
        Err(e) => println!("Error: {}", e),
    }

    let mut arr = Vec::with_capacity(kc as usize);

    for i in 0..kc {
        let mut bit = Bitmap::new();
        for j in 0..ic {
            bit.add(j * i);
        }
        bit.run_optimize();
        arr.push(bit);
    }

    match sys_info::mem_info() {
        Ok(mem) => {
            println!("总内存: {} G", mem.total / 1024 / 1024);
            println!("可用内存: {} G", mem.avail / 1024 / 1024);
            println!("使用内存: {} MB", (free_memory - mem.free) / 1024);
        }
        Err(e) => println!("Error: {}", e),
    }

    println!("{:?}", arr.len());
}
