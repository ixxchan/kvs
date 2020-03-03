use crossbeam::crossbeam_channel::{bounded, Receiver, Sender};
use std::thread;

use super::ThreadPool;
use crate::Result;

enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

pub struct SharedQueueThreadPool {
    sender: Sender<ThreadPoolMessage>,
    threads: u32,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (s, r) = bounded(threads as usize);
        for _ in 0..threads {
            let r = r.clone();
            thread::spawn(move || {
                Worker(r).run();
            });
        }
        Ok(SharedQueueThreadPool { sender: s, threads })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .send(ThreadPoolMessage::RunJob(Box::new(job)))
            .unwrap();
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.threads {
            self.sender.send(ThreadPoolMessage::Shutdown).unwrap();
        }
    }
}

struct Worker(Receiver<ThreadPoolMessage>);

impl Worker {
    pub fn run(self) {
        while let Ok(job) = self.0.recv() {
            match job {
                ThreadPoolMessage::RunJob(job) => {
                    job();
                }
                ThreadPoolMessage::Shutdown => {
                    break;
                }
            }
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if thread::panicking() {
            let r = self.0.clone();
            thread::spawn(move || {
                Worker(r).run();
            });
        }
    }
}
