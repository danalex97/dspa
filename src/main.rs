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
use std::thread;
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

    if let ("post-stats", _) = matches.subcommand() {
        thread::spawn(move || {
            load::run(records);
        });
        post_stats::run();
    }

    if let ("who-to-follow", _) = matches.subcommand() {
        thread::spawn(move || {
            load::run(records);
        });
        who_to_follow::run();
    }

    if let ("unusual_activity", _) = matches.subcommand() {
        thread::spawn(move || {
            load::run(records);
        });
        unusual_activity::run();
    }
}
