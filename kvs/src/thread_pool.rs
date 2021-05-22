//! Thread pools.

use crate::err::Result;
use std::thread::spawn;

/// ThreadPool interface
pub trait ThreadPool {
    /// Constructor
    fn new(threads: u32) -> Result<Self> where Self: Sized;
    /// Adds job to one of the threads
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

/// Naive ThreadPool implementation without threads in pool. Spawns a new thread for each job.
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(NaiveThreadPool {})
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        spawn(job);
    }
}

type BoxedJob = Box<dyn FnOnce() + Send + 'static>;

/// SharedQueue thread pool.
pub struct SharedQueueThreadPool {
    js: crossbeam::Sender<BoxedJob>,
    jr: crossbeam::Receiver<BoxedJob>,
    ps: crossbeam::Sender<()>,
    pr: crossbeam::Receiver<()>,
}

struct ThreadGuard {
    s: crossbeam::Sender<()>
}

impl Drop for ThreadGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            println!("panicked");
            self.s.send(()).unwrap();
        }
    }
}

impl SharedQueueThreadPool {
    /// Let thread panic and die because there are situations where panic! can't be captured by catch_unwind
    /// Use ThreadGuard to detect panic in `Drop`
    fn run(r: crossbeam::Receiver<BoxedJob>, _g: ThreadGuard) {
        loop {
            r.recv().map(|f| f());
        }
    }

    fn recover_threads(_js: crossbeam::Sender<BoxedJob>, jr: crossbeam::Receiver<BoxedJob>, ps: crossbeam::Sender<()>, pr: crossbeam::Receiver<()>) {
        loop {
            pr.recv().unwrap();
            let jr = jr.clone();
            let ps = ps.clone();
            spawn(|| SharedQueueThreadPool::run(jr, ThreadGuard{ s: ps }));
        }
    }
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (js, jr) = crossbeam::unbounded();
        let (ps, pr) = crossbeam::unbounded();
        for _ in 0..threads {
            let jr = jr.clone();
            let ps = ps.clone();
            spawn(|| SharedQueueThreadPool::run(jr, ThreadGuard{ s: ps }));
        }
        {
            let js = js.clone();
            let jr = jr.clone();
            let ps = ps.clone();
            let pr = pr.clone();
            spawn(|| SharedQueueThreadPool::recover_threads(js, jr, ps, pr));
        }
        Ok(SharedQueueThreadPool{ js, jr, ps, pr })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.js.send(Box::new(job)).unwrap();
    }
}

/// Rayon thread pool
pub struct RayonThreadPool;

impl ThreadPool for RayonThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        panic!()
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        panic!()
    }
}
