extern crate clap;
#[macro_use]
extern crate serde_derive;

mod connection;
mod dsa;
mod dto;
mod operators;
mod tasks;
mod util;

use clap::{App, Arg, SubCommand};
use std::path::Path;
use tasks::{load, post_stats, unusual_activity, who_to_follow};

fn main() {
    let matches = App::new("DSPA")
        .arg(
            Arg::with_name("records")
                .short("r")
                .long("records")
                .help("Set the number of records to read.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("path")
                .short("p")
                .long("path")
                .default_value("data/1k-users-sorted")
                .help("Set the path to the directory containing streams & tables")
                .takes_value(true),
        )
        .subcommand(
            SubCommand::with_name("post-stats")
                .about("Active posts(12 hours) statistics updated every 30 minutes."),
        )
        .subcommand(SubCommand::with_name("who-to-follow").about("Friend recommendation service."))
        .subcommand(
            SubCommand::with_name("unusual-activity")
                .about("Suggests users that post unusual content."),
        )
        .get_matches();

    let records = match matches.is_present("records") {
        true => Some(
            matches
                .value_of("records")
                .unwrap()
                .parse()
                .expect("records must be integer"),
        ),
        false => None,
    };

    let path = matches.value_of("path").unwrap();
    let path = Path::new(path);
    if !path.is_dir() {
        panic!("Specified path is not a directory");
    }

    let streams_path = path.join("streams");
    if !streams_path.is_dir() {
        panic!("Specified path does not contain streams directory");
    }
    let tables_path = path.join("tables");
    if !tables_path.is_dir() {
        panic!("Specified path does not contain tables directory");
    }

    load::run(records, &streams_path);

    if let ("post-stats", _) = matches.subcommand() {
        post_stats::run();
    }

    if let ("who-to-follow", _) = matches.subcommand() {
        who_to_follow::run(tables_path);
    }

    if let ("unusual-activity", _) = matches.subcommand() {
        unusual_activity::run();
    }
}
