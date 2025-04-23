use std::{sync::Mutex, time::Duration};

struct Test {
    lock: Mutex<()>,
}

pub fn main() {
    let test = Test {
        lock: Mutex::new(()),
    };

    test.t1();
    test.t2();
}

impl Test {
    fn t1(&self) {
        let _lock = self.lock.lock().unwrap();
        println!("t1");
        self.t2();
    }

    fn t2(&self) {
        let _lock = self.lock.lock().unwrap();
        println!("t2");
    }
}
