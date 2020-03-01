#[macro_use]
extern crate log;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use crossbeam_utils::sync::WaitGroup;
use env_logger::Env;
use rand::prelude::*;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

use kvs::thread_pool::*;
use kvs::{KvStore, KvsClient, KvsServer, Result, SledKvsEngine};

pub fn write_queued_kvstore(c: &mut Criterion) {
    let inputs = &[1, 2, 4, 6, 8, 12];

    c.bench_function_over_inputs(
        "write_queued_kvstore",
        |b, &&threads| {
            let pool = SharedQueueThreadPool::new(threads).unwrap();
            let temp_dir = TempDir::new().unwrap();
            let engine = KvStore::open(temp_dir.path()).unwrap();

            env_logger::from_env(Env::default().default_filter_or("debug")).init();
            info!("wtf");
            let mut server = KvsServer::new(engine, pool);
            thread::spawn(move || server.run("127.0.0.1:5005"));
            // wait for server
            thread::sleep(Duration::from_secs(1));

            let client_pool = SharedQueueThreadPool::new(1000).unwrap();
            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..1000 {
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        match KvsClient::connect("127.0.0.1:5005") {
                            Ok(mut client) => {
                                if let Err(e) = client.set(format!("key{}", i), "value".to_owned())
                                {
                                    eprintln!("2 {}", e);
                                }
                            }
                            Err(e) => {
                                eprintln!("1 {}", e);
                            }
                        }
                        drop(wg);
                    })
                }
                wg.wait();
            });
        },
        inputs,
    );
}

pub fn read_queued_kvstore(c: &mut Criterion) {
    let inputs = &[1, 2, 4, 6, 8, 12];

    c.bench_function_over_inputs(
        "read_queued_kvstore",
        |b, &&threads| {
            let pool = SharedQueueThreadPool::new(threads).unwrap();
            let temp_dir = TempDir::new().unwrap();
            let engine = KvStore::open(temp_dir.path()).unwrap();
            let mut server = KvsServer::new(engine, pool);
            thread::spawn(move || server.run("127.0.0.1:4007"));
            // wait for server
            thread::sleep(Duration::from_secs(1));

            for i in 0..1000 {
                let mut client = KvsClient::connect("127.0.0.1:4007").unwrap();
                assert!(client.set(format!("key{}", i), "value".to_owned()).is_ok());
            }
            let client_pool = SharedQueueThreadPool::new(1000).unwrap();
            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..1000 {
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        let mut client = KvsClient::connect("127.0.0.1:4007").unwrap();
                        assert_eq!(
                            client.get(format!("key{}", i)).unwrap(),
                            (Some("value".to_owned()))
                        );
                        drop(wg);
                    })
                }
                wg.wait();
            });
        },
        inputs,
    );
}

criterion_group!(benches, write_queued_kvstore, read_queued_kvstore);
criterion_main!(benches);
