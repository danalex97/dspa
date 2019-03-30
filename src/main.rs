mod dto;
mod connection;
mod stream;

use stream::listen;
use connection::import::parse_csv;
use connection::producer::Producer;

fn main() {
    let mut producer = Producer::new("posts".to_string());
    producer.write_file("data/1k-users-sorted/streams/post_event_stream.csv");
    listen("posts".to_string());
}
