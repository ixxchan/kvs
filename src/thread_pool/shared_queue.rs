use crossbeam::crossbeam_channel::{bounded, Sender};
use std::panic::{self, AssertUnwindSafe};
use std::thread;

use super::ThreadPool;
use crate::Result;

enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

pub struct SharedQueueThreadPool(Sender<ThreadPoolMessage>);

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (s, r) = bounded(threads as usize);
        for _ in 0..threads {
            let r = r.clone();
            thread::spawn(move || {
                while let Ok(job) = r.recv() {
                    match job {
                        ThreadPoolMessage::RunJob(job) => {
                            let _ = panic::catch_unwind(AssertUnwindSafe(job));
                        }
                        ThreadPoolMessage::Shutdown => {}
                    }
                }
            });
        }
        Ok(SharedQueueThreadPool(s))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0
            .send(ThreadPoolMessage::RunJob(Box::new(job)))
            .unwrap();
    }
}
