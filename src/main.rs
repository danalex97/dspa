mod dto;
mod connection;
mod stream;

use dto::person::Person;

use stream::listen;
use connection::import::parse_csv;
use connection::producer::Producer;

fn main() {
    let mut producer = Producer::new("person".to_string());
    parse_csv::<Person, _>("data/1k-users-sorted/tables/person.csv", |person| {
        producer.write(format!("{:?}", person));
    });
    listen("person".to_string());
}
