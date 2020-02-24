use clap::{App, AppSettings, Arg};
use kvs::*;
use std::{env, fs::File, process};

#[macro_use]
extern crate log;
use log::LevelFilter;
use simplelog::*;
use std::io::{Error, Read, Write};

fn main() -> Result<()> {
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

    let addr = matches.value_of("addr").expect("wtf");
    let input_engine = matches.value_of("engine");

    let mut buf = String::new();
    let mut old_engine;
    match File::open("ENGINE") {
        Ok(mut f) => {
            f.read_to_string(&mut buf);
            old_engine = Some(buf.as_str());
        }
        Err(_) => {
            old_engine = None;
        }
    }

    let mut engine;
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

    TermLogger::init(LevelFilter::Debug, Config::default(), TerminalMode::Stderr)?;
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", addr);

    run_engine(engine, addr)
}

fn run_engine(engine: &str, addr: &str) -> Result<()> {
    let mut f = File::create("ENGINE")?;
    f.write(engine.as_bytes());
    match engine {
        "kvs" => {
            let mut server = KvsServer::new((KvStore::open(env::current_dir()?)?));
            server.run(addr)
        }
        "sled" => {
            let mut server = KvsServer::new((SledKvsEngine::open(env::current_dir()?)?));
            server.run(addr)
        }
        _ => panic!("invalid engine {}", engine),
    }
}
