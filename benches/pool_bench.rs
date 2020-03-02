#[macro_use]
extern crate log;

use criterion::{criterion_group, criterion_main, Criterion};
use crossbeam_utils::sync::WaitGroup;
use env_logger::Env;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

use kvs::thread_pool::*;
use kvs::{KvStore, KvsClient, KvsServer, SledKvsEngine};

// SIZE = 1000 is problematic ???
const SIZE: u32 = 100;
const INPUTS: &[u32; 6] = &[1, 2, 4, 6, 8, 12];

pub fn write_queued_kvstore(c: &mut Criterion) {
    env_logger::from_env(Env::default().default_filter_or("debug")).init();

    c.bench_function_over_inputs(
        "write_queued_kvstore",
        |b, &&threads| {
            let pool = SharedQueueThreadPool::new(threads).unwrap();
            let temp_dir = TempDir::new().unwrap();
            let engine = KvStore::open(temp_dir.path()).unwrap();

            info!("wtf");
            let mut server = KvsServer::new(engine, pool);
            thread::spawn(move || server.run("127.0.0.1:5005"));
            // wait for server
            thread::sleep(Duration::from_secs(1));

            let client_pool = SharedQueueThreadPool::new(SIZE).unwrap();
            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..SIZE {
                    info!("client{}", i);
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        match KvsClient::connect("127.0.0.1:5005") {
                            Ok(mut client) => {
                                if let Err(e) = client.set(format!("key{}", i), "value".to_owned())
                                {
                                    error!("2 {}", e);
                                    //                                    panic!();
                                }
                            }
                            Err(e) => {
                                error!("1 {}", e);
                                //                                panic!();
                            }
                        }
                        drop(wg);
                    })
                }
                wg.wait();
                info!("one iteration ends");
            });
            info!("b.iter ends");
        },
        INPUTS,
    );
}

pub fn read_queued_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "read_queued_kvstore",
        |b, &&threads| {
            let pool = SharedQueueThreadPool::new(threads).unwrap();
            let temp_dir = TempDir::new().unwrap();
            let engine = KvStore::open(temp_dir.path()).unwrap();
            let mut server = KvsServer::new(engine, pool);
            thread::spawn(move || server.run("127.0.0.1:5006"));
            // wait for server
            thread::sleep(Duration::from_secs(1));

            for i in 0..SIZE {
                let mut client = KvsClient::connect("127.0.0.1:5006").unwrap();
                assert!(client.set(format!("key{}", i), "value".to_owned()).is_ok());
            }
            let client_pool = SharedQueueThreadPool::new(SIZE).unwrap();
            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..SIZE {
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        let mut client = KvsClient::connect("127.0.0.1:5006").unwrap();
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
        INPUTS,
    );
}

pub fn write_rayon_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "write_rayon_kvstore",
        |b, &&threads| {
            let pool = RayonThreadPool::new(threads).unwrap();
            let temp_dir = TempDir::new().unwrap();
            let engine = KvStore::open(temp_dir.path()).unwrap();

            info!("wtf");
            let mut server = KvsServer::new(engine, pool);
            thread::spawn(move || server.run("127.0.0.1:5007"));
            // wait for server
            thread::sleep(Duration::from_secs(1));

            let client_pool = RayonThreadPool::new(SIZE).unwrap();
            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..SIZE {
                    info!("client{}", i);
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        match KvsClient::connect("127.0.0.1:5007") {
                            Ok(mut client) => {
                                if let Err(e) = client.set(format!("key{}", i), "value".to_owned())
                                {
                                    error!("2 {}", e);
                                    panic!();
                                }
                            }
                            Err(e) => {
                                error!("1 {}", e);
                                panic!();
                            }
                        }
                        drop(wg);
                    })
                }
                wg.wait();
                info!("one iteration ends");
            });
            info!("b.iter ends");
        },
        INPUTS,
    );
}

pub fn read_rayon_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "read_rayon_kvstore",
        |b, &&threads| {
            let pool = RayonThreadPool::new(threads).unwrap();
            let temp_dir = TempDir::new().unwrap();
            let engine = KvStore::open(temp_dir.path()).unwrap();
            let mut server = KvsServer::new(engine, pool);
            thread::spawn(move || server.run("127.0.0.1:5008"));
            // wait for server
            thread::sleep(Duration::from_secs(1));

            for i in 0..SIZE {
                let mut client = KvsClient::connect("127.0.0.1:5008").unwrap();
                assert!(client.set(format!("key{}", i), "value".to_owned()).is_ok());
            }
            let client_pool = RayonThreadPool::new(SIZE).unwrap();
            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..SIZE {
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        let mut client = KvsClient::connect("127.0.0.1:5008").unwrap();
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
        INPUTS,
    );
}

pub fn write_rayon_sled(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "write_rayon_sled",
        |b, &&threads| {
            let pool = RayonThreadPool::new(threads).unwrap();
            let temp_dir = TempDir::new().unwrap();
            let engine = SledKvsEngine::open(temp_dir.path()).unwrap();

            info!("wtf");
            let mut server = KvsServer::new(engine, pool);
            thread::spawn(move || server.run("127.0.0.1:5009"));
            // wait for server
            thread::sleep(Duration::from_secs(1));

            let client_pool = RayonThreadPool::new(SIZE).unwrap();
            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..SIZE {
                    info!("client{}", i);
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        match KvsClient::connect("127.0.0.1:5009") {
                            Ok(mut client) => {
                                if let Err(e) = client.set(format!("key{}", i), "value".to_owned())
                                {
                                    error!("2 {}", e);
                                    panic!();
                                }
                            }
                            Err(e) => {
                                error!("1 {}", e);
                                panic!();
                            }
                        }
                        drop(wg);
                    })
                }
                wg.wait();
                info!("one iteration ends");
            });
            info!("b.iter ends");
        },
        INPUTS,
    );
}

pub fn read_rayon_sled(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "read_queued_sled",
        |b, &&threads| {
            let pool = RayonThreadPool::new(threads).unwrap();
            let temp_dir = TempDir::new().unwrap();
            let engine = SledKvsEngine::open(temp_dir.path()).unwrap();
            let mut server = KvsServer::new(engine, pool);
            thread::spawn(move || server.run("127.0.0.1:5010"));
            // wait for server
            thread::sleep(Duration::from_secs(1));

            for i in 0..SIZE {
                let mut client = KvsClient::connect("127.0.0.1:5010").unwrap();
                assert!(client.set(format!("key{}", i), "value".to_owned()).is_ok());
            }
            let client_pool = RayonThreadPool::new(SIZE).unwrap();
            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..SIZE {
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        let mut client = KvsClient::connect("127.0.0.1:5010").unwrap();
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
        INPUTS,
    );
}

criterion::criterion_group!(benches, write_queued_kvstore, read_queued_kvstore);
criterion_main!(benches);
