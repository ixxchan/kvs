#[macro_use]
extern crate log;

use clap::{App, AppSettings, Arg};
use env_logger::Env;
use std::fs::File;
use std::io::prelude::*;
use std::{env, process};

use kvs::{KvStore, KvsServer, Result, SledKvsEngine};

fn main() {
    let matches = App::new("kvs-server")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("A key-value store server")
        .setting(AppSettings::DisableHelpSubcommand)
        .arg(
            Arg::with_name("addr")
                .long("addr")
                .takes_value(true)
                .value_name("IP-PORT")
                .default_value("127.0.0.1:4000")
                .help("the server address"),
        )
        .arg(
            Arg::with_name("engine")
                .long("engine")
                .takes_value(true)
                .value_name("ENGINE-NAME")
                .possible_values(&["kvs", "sled"])
                .help("the server address"),
        )
        .get_matches();

    let addr = matches.value_of("addr").unwrap();
    let input_engine = matches.value_of("engine");
    let engine = &get_engine(input_engine);

    env_logger::from_env(Env::default().default_filter_or("info")).init();

    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", addr);

    if let Err(e) = run_engine(engine, addr) {
        eprintln!("{}", e);
        process::exit(1);
    }
}

fn get_engine(input_engine: Option<&str>) -> String {
    let mut buf = String::new();
    let old_engine;
    match File::open("ENGINE") {
        Ok(mut f) => {
            if f.read_to_string(&mut buf).is_ok() {
                old_engine = Some(buf.as_str());
            } else {
                old_engine = None;
            }
        }
        Err(_) => {
            old_engine = None;
        }
    }

    let engine;
    match (input_engine, old_engine) {
        (None, None) => engine = "kvs",
        (None, Some(e)) => {
            engine = e;
        }
        (Some(e), None) => {
            engine = e;
        }
        (Some(e1), Some(e2)) => {
            if e1 == e2 {
                engine = e1;
            } else {
                eprintln!("Inconsistent engine: {} {}", e1, e2);
                process::exit(1)
            }
        }
    }
    engine.to_owned()
}

fn run_engine(engine: &str, addr: &str) -> Result<()> {
    let mut f = File::create("ENGINE")?;
    f.write_all(engine.as_bytes())?;
    match engine {
        "kvs" => {
            let mut server = KvsServer::new(KvStore::open(env::current_dir()?)?);
            server.run(addr)
        }
        "sled" => {
            let mut server = KvsServer::new(SledKvsEngine::open(env::current_dir()?)?);
            server.run(addr)
        }
        _ => panic!("invalid engine {}", engine),
    }
}
