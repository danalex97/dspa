#[macro_use]
extern crate clap;
#[macro_use]
extern crate serde_derive;

mod connection;
mod dsa;
mod dto;
mod operators;
mod tasks;

use crate::tasks::who_to_follow;
use clap::{App, Arg, SubCommand};
use tasks::load;
use tasks::post_stats;

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
        // TODO: load static data from path
        who_to_follow::run();
    }

    load::run(Some(1000));
    who_to_follow::run();
}
