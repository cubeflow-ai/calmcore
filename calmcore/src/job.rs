use std::{
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use itertools::Itertools;

use crate::{index_store::segment::SegmentReader, persist, util::CoreResult, Engine};

/// 全局persisit 锁
static PERSIST_LOCK: Mutex<()> = Mutex::new(());

pub struct Job {
    engines: RwLock<Vec<Arc<Engine>>>,
    segment_max_size: usize,
    // segment persist interval default:3600
    pub flush_interval_secs: u64,
}

impl Job {
    pub fn new(segment_max_size: usize, flush_interval_secs: u64) -> Arc<Self> {
        let segment_max_size = if segment_max_size == 0 {
            usize::MAX
        } else {
            segment_max_size
        };

        let flush_interval_secs = if flush_interval_secs == 0 {
            u64::MAX
        } else {
            flush_interval_secs
        };

        let job = Arc::new(Self {
            engines: RwLock::new(vec![]),
            segment_max_size,
            flush_interval_secs,
        });

        let pjob = job.clone();
        std::thread::spawn(move || {
            pjob.clone().persist_job();
        });

        let sjob = job.clone();
        std::thread::spawn(move || {
            sjob.clone().segment_job();
        });

        job
    }

    pub fn add_engine(&self, engine: Arc<Engine>) {
        self.engines.write().unwrap().push(engine);
    }
}

impl Job {
    fn persist_job(self: Arc<Self>) {
        loop {
            std::thread::sleep(Duration::from_secs(6));
            let engines = self.engines.read().unwrap().clone();
            for engine in engines {
                //TODO : force persist?
                if let Err(e) = Self::persist(engine, true) {
                    log::error!("persist error: {:?}", e);
                };
            }
        }
    }

    fn segment_job(self: Arc<Self>) {
        loop {
            std::thread::sleep(Duration::from_secs(6));
            let engines = self.engines.read().unwrap().clone();
            for engine in engines {
                if let Err(e) = Self::segment(
                    engine,
                    self.segment_max_size as u64,
                    self.flush_interval_secs,
                ) {
                    log::error!("segment error: {:?}", e);
                };
            }
        }
    }

    pub(crate) fn segment(engine: Arc<Engine>, max: u64, ttl: u64) -> CoreResult<()> {
        let segments = engine.segment_readers();

        let engine_name = &engine.scope().schema.name;

        let segment_count = segments.iter().map(|s| (s.start(), s.end())).collect_vec();

        log::info!(
            "engine:{} segments: {:?} hots:{:?} segment_count:{:?}",
            engine_name,
            segments.len(),
            segments
                .iter()
                .sorted_by_key(|s| s.start())
                .filter(|s| s.is_hot())
                .count(),
            segment_count
        );

        let mut iter = segments.into_iter();
        let current = iter.next().unwrap(); // remove current

        if current.end() - current.start() > max || current.live_time().as_secs() > ttl {
            log::info!(
                "engine:{} active current segment:{}-{} freeze it",
                engine_name,
                current.start(),
                current.end()
            );
            engine.store.new_current_segment()?;
        }
        Ok(())
    }

    pub(crate) fn persist(engine: Arc<Engine>, force: bool) -> CoreResult<()> {
        let lock = PERSIST_LOCK.lock().unwrap();
        let segments = engine.segment_readers();
        let engine_name = &engine.scope().schema.name;

        let mut iter = segments.into_iter();
        iter.next().unwrap(); // remove current

        for segment in iter {
            if let SegmentReader::Hot(reader) = segment {
                log::info!(
                    "engine:{} segment:{}-{} to persist",
                    engine_name,
                    reader.start,
                    reader.end,
                );

                //TODO: add mem used condition
                if force {
                    let start_time = std::time::Instant::now();
                    let (start, end) = (reader.start, reader.end);

                    if start == end {
                        log::warn!("engine:{} segment:{}-{} is empty", engine_name, start, end);
                        continue;
                    }

                    log::info!(
                        "engine:{} start persist hot segment:{}-{}",
                        engine_name,
                        start,
                        end,
                    );
                    persist::write_segment(&engine.store, reader)?;

                    log::info!(
                        "engine:{} persist hot segment:{}-{} cost:{:?}",
                        engine_name,
                        start,
                        end,
                        start_time.elapsed()
                    );

                    engine.hot_to_warm(start, end)?;

                    log::info!(
                        "engine:{} hot segment:{}-{} to warm cost:{:?}",
                        engine_name,
                        start,
                        end,
                        start_time.elapsed()
                    );
                }
            }
        }

        drop(lock);

        Ok(())
    }
}
