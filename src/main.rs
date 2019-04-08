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
        .get_matches();

    if let ("load", Some(args)) = matches.subcommand() {
        let records = match value_t!(args.value_of("records"), usize) {
            Ok(records) => Some(records),
            Err(_) => None,
        };

        load::run(records);
    }

    if let ("post-stats", _) = matches.subcommand() {
        post_stats::run();
    }

    load::run(Some(200));
    post_stats::run();
}
