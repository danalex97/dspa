extern crate timely;

use crate::dto::post::Post;
use crate::operators::source::KafkaSource;

use timely::dataflow::operators::Inspect;

pub fn run() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            scope.kafka_string_source::<Post>("posts".to_string())
                .inspect(|x| println!("{:?}", x));
        });
    }).unwrap();
}
