const POSTS_PATH: &str = "data/1k-users-sorted/streams/post_event_stream.csv";
const COMMENTS_PATH: &str = "data/1k-users-sorted/streams/comment_event_stream.csv";
const LIKES_PATH: &str = "data/1k-users-sorted/streams/likes_event_stream.csv";

use crate::connection::producer::Producer;
use chrono::DateTime;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub fn run(records: Option<usize>) {
    let mut creation_time = None;

    let posts_file = File::open(POSTS_PATH).unwrap();
    let posts_file = BufReader::new(posts_file);

    for line in posts_file.lines().skip(1) {
        let line = line.unwrap();
        let fields: Vec<&str> = line.split("|").collect();
        creation_time = Some(DateTime::parse_from_rfc3339(fields[2]).unwrap());
        break;
    }
    Producer::new("posts".to_string()).write_file(POSTS_PATH, records, &(creation_time.unwrap()));
    Producer::new("comments".to_string()).write_file(
        COMMENTS_PATH,
        records,
        &(creation_time.unwrap()),
    );
    Producer::new("likes".to_string()).write_file(LIKES_PATH, records, &(creation_time.unwrap()));
}
