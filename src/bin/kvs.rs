use clap::{App, AppSettings, SubCommand};
use kvs::Result;

fn main() -> Result<()> {
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
        ("set", Some(_matches)) => {
            panic!("unimplemented");
        }
        ("get", Some(_matches)) => {
            panic!("unimplemented");
        }
        ("rm", Some(_matches)) => {
            panic!("unimplemented");
        }
        _ => {
            panic!("no args");
        }
    }
}
