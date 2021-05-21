use std::sync::{Arc, Condvar, Mutex};
use std::thread::{spawn, JoinHandle, Thread};

type Job = dyn FnOnce() + Send + 'static;
type JobQueue = Vec<Box<Job>>;

pub struct ThreadPool {
    pool: Vec<JoinHandle<()>>,
    job_queue: Arc<Mutex<JobQueue>>,
    cv: Arc<Condvar>,
}

impl ThreadPool {
    fn run(cv: Arc<Condvar>, job_queue: Arc<Mutex<JobQueue>>) {
        let mut queue = job_queue.lock().unwrap();
        loop {
            if let Some(job) = queue.pop() {
                std::mem::drop(queue);
                job();
                queue = job_queue.lock().unwrap();
            } else {
                queue = cv.wait(queue).unwrap();
            }
        }
    }

    pub fn new(n: usize) -> ThreadPool {
        let mut pool = Vec::new();
        let cv = Arc::new(Condvar::new());
        let job_queue = Vec::<Box<dyn FnOnce() + Send + 'static>>::new();
        let job_queue = Arc::new(Mutex::new(job_queue));
        for _ in 0..n {
            let cv = cv.clone();
            let job_queue = job_queue.clone();
            let jh = spawn(|| ThreadPool::run(cv, job_queue));
            pool.push(jh);
        }
        ThreadPool {
            pool,
            cv,
            job_queue,
        }
    }

    pub fn add_job(&mut self, job: Box<Job>) {
        {
            let mut job_queue = self.job_queue.lock().unwrap();
            job_queue.push(job);
        }
        // After unlocking job_queue, we try notify workers
        self.cv.notify_one()
    }
}

#[cfg(test)]
mod tests {
    use crate::ThreadPool;
    use std::thread::sleep;

    #[test]
    fn basic() {
        let mut pool = ThreadPool::new(2);
        for _ in 0..4 {
            pool.add_job(Box::new(|| {
                sleep(std::time::Duration::from_secs(3));
                println!("hey");
            }))
        }
        sleep(std::time::Duration::from_secs(7));
    }
}
