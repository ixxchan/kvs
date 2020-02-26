use clap::{App, AppSettings, Arg, SubCommand};
use env_logger::Env;
use std::process;

use kvs::{KvsClient, Result};

struct Opt {
    addr: String,
    cmd: Command,
}

enum Command {
    Set { key: String, value: String },
    Rm { key: String },
    Get { key: String },
}

fn get_opt() -> Opt {
    let addr;
    let cmd;

    let addr_arg = Arg::with_name("addr")
        .long("addr")
        .takes_value(true)
        .value_name("IP-PORT")
        .default_value("127.0.0.1:4000")
        .help("the server address");
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
                .arg(addr_arg.clone()),
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg_from_usage("<KEY> 'the key you want to look up'")
                .arg(addr_arg.clone()),
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg_from_usage("<KEY> 'the key you want to remove'")
                .arg(addr_arg),
        ])
        .get_matches();
    match matches.subcommand() {
        ("set", Some(matches)) => {
            addr = matches.value_of("addr").expect("wtf").to_owned();
            let key = matches.value_of("KEY").unwrap().to_owned();
            let value = matches.value_of("VALUE").unwrap().to_owned();
            cmd = Command::Set { key, value };
        }
        ("get", Some(matches)) => {
            addr = matches.value_of("addr").expect("wtf").to_owned();
            let key = matches.value_of("KEY").unwrap().to_owned();
            cmd = Command::Get { key };
        }
        ("rm", Some(matches)) => {
            addr = matches.value_of("addr").expect("wtf").to_owned();
            let key = matches.value_of("KEY").unwrap().to_owned();
            cmd = Command::Rm { key };
        }
        _ => {
            eprintln!("No command specified");
            process::exit(1);
        }
    };
    Opt { addr, cmd }
}

fn run(opt: Opt) -> Result<()> {
    let mut client = KvsClient::connect(opt.addr)?;
    match opt.cmd {
        Command::Set { key, value } => client.set(key, value),
        Command::Rm { key } => client.remove(key),
        Command::Get { key } => {
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
            Ok(())
        }
    }
}

fn main() {
    env_logger::from_env(Env::default().default_filter_or("debug")).init();

    if let Err(e) = run(get_opt()) {
        eprintln!("{}", e);
        process::exit(1);
    }
}
