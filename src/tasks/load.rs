const POSTS_PATH: &str = "data/1k-users-sorted/streams/post_event_stream.csv";
const COMMENTS_PATH: &str = "data/1k-users-sorted/streams/comment_event_stream.csv";
const LIKES_PATH: &str = "data/1k-users-sorted/streams/likes_event_stream.csv";

use crate::connection::producer::Producer;

pub fn run(records: Option<usize>) {
    println!(
        "{} posts loaded to Kafka.",
        Producer::new("posts".to_string()).write_file(POSTS_PATH, records)
    );
    println!(
        "{} comments loaded to Kafka.",
        Producer::new("comments".to_string()).write_file(COMMENTS_PATH, records)
    );
    println!(
        "{} likes loaded to Kafka.",
        Producer::new("likes".to_string()).write_file(LIKES_PATH, records)
    );
}
