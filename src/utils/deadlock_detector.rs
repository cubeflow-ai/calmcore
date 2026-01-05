//! 死锁检测工具
//!
//! 使用 parking_lot 的 deadlock_detection 功能自动检测死锁

use std::thread;
use std::time::Duration;

/// 启动死锁检测后台线程
///
/// 每 10 秒检查一次是否有死锁，如果发现死锁会打印详细信息
pub fn start_deadlock_detector() {
    thread::Builder::new()
        .name("deadlock-detector".to_string())
        .spawn(move || {
            let mut check_count = 0;
            loop {
                thread::sleep(Duration::from_secs(5));
                check_count += 1;
                let deadlocks = parking_lot::deadlock::check_deadlock();
                if deadlocks.is_empty() {
                    // 每 12 次检查（1 分钟）打印一次健康日志
                    if check_count % 12 == 0 {
                        log::debug!("🔍 Deadlock detector: {} checks completed, no deadlocks detected", check_count);
                    }
                    continue;
                }

                println!("\n╔══════════════════════════════════════════════════════════════╗");
                println!("║  🚨 DEADLOCK DETECTED! 检测到死锁！                          ║");
                println!("╚══════════════════════════════════════════════════════════════╝\n");

                for (i, threads) in deadlocks.iter().enumerate() {
                    println!("═══ Deadlock #{} (涉及 {} 个线程) ═══", i, threads.len());
                    for thread in threads {
                        println!("\n🔴 Thread: {:?}", thread.thread_id());
                        println!("   等待的锁位置:");
                        let backtrace = thread.backtrace();
                        let frames: Vec<String> = format!("{:?}", backtrace)
                            .lines()
                            .take(20)
                            .map(|s| s.to_string())
                            .collect();
                        for (n, frame) in frames.iter().enumerate() {
                            println!("      #{}: {}", n, frame);
                        }
                    }
                    println!();
                }

                println!("═══════════════════════════════════════════════════════════\n");

                // 可选：检测到死锁后退出进程
                // std::process::exit(1);
            }
        })
        .expect("Failed to spawn deadlock detector thread");
}
