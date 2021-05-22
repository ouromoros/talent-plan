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

/// SharedQueue thread pool.
pub struct SharedQueueThreadPool;

impl ThreadPool for SharedQueueThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        panic!()
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        panic!()
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
