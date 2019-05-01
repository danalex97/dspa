#[macro_use]
extern crate clap;
#[macro_use]
extern crate serde_derive;

mod connection;
mod dsa;
mod dto;
mod operators;
mod tasks;

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
        .subcommand(
            SubCommand::with_name("who-to-follow")
                .about("Friend recommendation service.")
                .arg(
                    Arg::with_name("static-data")
                        .help("Path to directory containing static data")
                )
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

    if let ("who-to-follow", Some(args)) = matches.subcommand() {
        let path = match value_t!(args.value_of("static-data"), String) {
            Ok(path) => path,
            Err(_) => panic!(),
        };
        // TODO: load static data from path
    }

    load::run(Some(100));
    post_stats::run();
}
