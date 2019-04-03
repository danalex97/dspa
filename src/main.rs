extern crate timely;
extern crate clap;

mod dto;
mod stream;
mod connection;

use dto::post::Post;
use stream::KafkaSource;
use connection::producer::Producer;

use timely::dataflow::operators::Inspect;

use clap::{App, SubCommand};

fn main() {
    let matches = App::new("DSPA")
       .subcommand(SubCommand::with_name("task0"))
       .subcommand(SubCommand::with_name("task1"))
       .get_matches();

    if let ("task0", _) = matches.subcommand() {
        let mut producer = Producer::new("posts".to_string());
        producer.write_file("data/1k-users-sorted/streams/post_event_stream.csv");
    }

    if let ("task1", _) = matches.subcommand() {
        timely::execute_from_args(std::env::args(), |worker| {
            worker.dataflow::<usize, _, _>(|scope| {
                scope
                    .kafka_string_source::<Post>("posts".to_string());
            });
        }).unwrap();
    }
}
