use clap::{App, AppSettings, SubCommand};
use kvs::{KvStore, Result};
use std::path::Path;
use std::process;

fn main() -> Result<()> {
    let mut kv = KvStore::open(Path::new("."))?;

    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .subcommands(vec![
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .args_from_usage(
                    "<KEY> 'the key you want to set'
                    <VALUE> 'the value you want to set to the key'",
                ),
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg_from_usage("<KEY> 'the key you want to look up'"),
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg_from_usage("<KEY> 'the key you want to remove'"),
        ])
        .get_matches();

    match matches.subcommand() {
        ("set", Some(matches)) => kv.set(
            matches.value_of("KEY").unwrap().to_owned(),
            matches.value_of("VALUE").unwrap().to_owned(),
        )?,
        ("get", Some(matches)) => {
            let value = kv.get(matches.value_of("KEY").unwrap().to_owned())?;
            match value {
                Some(value) => {
                    println!("{}", value);
                }
                None => {
                    println!("Key not found");
                }
            }
        }
        ("rm", Some(matches)) => {
            if let Err(_) = kv.remove(matches.value_of("KEY").unwrap().to_owned()) {
                println!("Key not found");
                process::exit(1)
            }
        }
        _ => {
            panic!("no args");
        }
    };
    Ok(())
}
