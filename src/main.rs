#[macro_use]
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
        .subcommand(
            SubCommand::with_name("load")
                .about("Load data from dataset to Kafka.")
                .arg(
                    Arg::with_name("records")
                        .help("Number of records loaded.(all data is loaded when not provided)"),
                ),
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

    if let ("load", Some(args)) = matches.subcommand() {
        let records = match value_t!(args.value_of("records"), usize) {
            Ok(records) => Some(records),
            Err(_) => Some(1000),
        };

        load::run(records);
        return;
    }

    if let ("post-stats", _) = matches.subcommand() {
        post_stats::run();
    }

    if let ("who-to-follow", _) = matches.subcommand() {
        who_to_follow::run();
    }

    if let ("unusual_activity", _) = matches.subcommand() {
        unusual_activity::run();
    }

    // load::run(Some(1000));
    // post_stats::run();
    thread::spawn(move || {
        load::run(Some(10000));
    });
    unusual_activity::run();
}
