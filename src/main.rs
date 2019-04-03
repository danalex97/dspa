#[macro_use]
extern crate clap;
extern crate timely;

mod dto;
mod source;
mod connection;

use dto::post::Post;
use source::KafkaSource;
use connection::producer::Producer;

use timely::dataflow::operators::Inspect;

use clap::{App, SubCommand, Arg};

static POSTS_PATH: &'static str = "data/1k-users-sorted/streams/post_event_stream.csv";

fn main() {
    let matches = App::new("DSPA")
       .subcommand(SubCommand::with_name("load")
            .about("Load data from dataset to Kafka.")
            .arg(Arg::with_name("records")
                .help("Number of records loaded.(all data is loaded when not provided)")
            )
        )
       .subcommand(SubCommand::with_name("post-stats")
            .about("Active posts(12 hours) statistics updated every 30 minutes.")
        )
       .get_matches();

    if let ("load", Some(args)) = matches.subcommand() {
        let mut producer = Producer::new("posts".to_string());
        let records = match value_t!(args.value_of("records"), usize) {
            Ok(records) => Some(records),
            Err(_) => None,
        };

        let loaded = producer.write_file(POSTS_PATH, records);
        println!("{} records loaded to Kafka.", loaded);
    }

    if let ("post-stats", _) = matches.subcommand() {
        timely::execute_from_args(std::env::args(), |worker| {
            worker.dataflow::<usize, _, _>(|scope| {
                scope.kafka_string_source::<Post>("posts".to_string());
            });
        }).unwrap();
    }
}
