use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{KvStore, Result};
use std::path::Path;
use std::process;

#[macro_use]
extern crate log;
use log::LevelFilter;
use simplelog::*;

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
                .default_value("kvs")
                .help("the server address"),
        )
        .get_matches();

    let addr = matches.value_of("addr").expect("wtf");
    let engine = matches.value_of("engine").expect("wtf");

    TermLogger::init(LevelFilter::Info, Config::default(), TerminalMode::Stderr);
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", addr);
    Ok(())
}
