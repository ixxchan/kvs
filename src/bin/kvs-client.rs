use clap::{App, AppSettings, Arg, SubCommand};
use env_logger::Env;
use kvs::{KvsClient, Result};
use std::process;

fn main() -> Result<()> {
    // TODO: The repeated args in subcommands look ugly. How to improve it?
    let matches = App::new("kvs-client")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("A key-value store client")
        .setting(AppSettings::DisableHelpSubcommand)
        .subcommands(vec![
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .args_from_usage(
                    "<KEY> 'the key you want to set'
                    <VALUE> 'the value you want to set to the key'",
                )
                .arg(
                    Arg::with_name("addr")
                        .long("addr")
                        .takes_value(true)
                        .value_name("IP-PORT")
                        .default_value("127.0.0.1:4000")
                        .help("the server address"),
                ),
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg_from_usage("<KEY> 'the key you want to look up'")
                .arg(
                    Arg::with_name("addr")
                        .long("addr")
                        .takes_value(true)
                        .value_name("IP-PORT")
                        .default_value("127.0.0.1:4000")
                        .help("the server address"),
                ),
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg_from_usage("<KEY> 'the key you want to remove'")
                .arg(
                    Arg::with_name("addr")
                        .long("addr")
                        .takes_value(true)
                        .value_name("IP-PORT")
                        .default_value("127.0.0.1:4000")
                        .help("the server address"),
                ),
        ])
        .get_matches();

    env_logger::from_env(Env::default().default_filter_or("debug")).init();

    match matches.subcommand() {
        ("set", Some(matches)) => {
            let addr = matches.value_of("addr").expect("wtf");
            let mut client = KvsClient::connect(addr)?;
            client.set(
                matches.value_of("KEY").unwrap().to_owned(),
                matches.value_of("VALUE").unwrap().to_owned(),
            )?
        }
        ("get", Some(matches)) => {
            let addr = matches.value_of("addr").expect("wtf");
            let mut client = KvsClient::connect(addr)?;
            let value = client.get(matches.value_of("KEY").unwrap().to_owned())?;
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
            let addr = matches.value_of("addr").expect("wtf");
            let mut client = KvsClient::connect(addr)?;
            if client
                .remove(matches.value_of("KEY").unwrap().to_owned())
                .is_err()
            {
                eprintln!("Key not found");
                process::exit(1)
            }
        }
        _ => {
            panic!("no args");
        }
    };
    Ok(())
}
