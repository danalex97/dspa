static POSTS_PATH: &'static str = "data/1k-users-sorted/streams/post_event_stream.csv";
static COMMENTS_PATH: &'static str = "data/1k-users-sorted/streams/comment_event_stream.csv";

use crate::connection::producer::Producer;

pub fn run(records : Option<usize>) {
    let mut producer = Producer::new("posts".to_string());
    println!("{} posts loaded to Kafka.", producer.write_file(POSTS_PATH, records));
    println!("{} comments loaded to Kafka.", producer.write_file(COMMENTS_PATH, records));
}
