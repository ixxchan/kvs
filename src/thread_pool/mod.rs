use crate::Result;

pub use naive::NaiveThreadPool;
pub use rayon::RayonThreadPool;
pub use shared_queue::SharedQueueThreadPool;

/// Trait for a thread pool.
pub trait ThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

mod naive;
mod rayon;
mod shared_queue;
