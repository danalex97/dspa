use crate::connection::producer::Producer;
use chrono::DateTime;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::thread;

pub fn run(records: Option<usize>, streams_path: &PathBuf) {
    let posts_path = streams_path.join("post_event_stream.csv");
    let comments_path = streams_path.join("comment_event_stream.csv");
    let likes_path = streams_path.join("likes_event_stream.csv");

    let mut creation_time = None;

    let posts_file = File::open(posts_path.clone()).unwrap();
    let posts_file = BufReader::new(posts_file);

    for line in posts_file.lines().skip(1) {
        let line = line.unwrap();
        let fields: Vec<&str> = line.split("|").collect();
        creation_time = Some(DateTime::parse_from_rfc3339(fields[2]).unwrap());
        break;
    }
    thread::spawn(move || {
        Producer::new("posts".to_string()).write_file(
            posts_path.to_str().unwrap(),
            records,
            &(creation_time.unwrap()),
        );
    });
    thread::spawn(move || {
        Producer::new("comments".to_string()).write_file(
            comments_path.to_str().unwrap(),
            records,
            &(creation_time.unwrap()),
        );
    });
    thread::spawn(move || {
        Producer::new("likes".to_string()).write_file(
            likes_path.to_str().unwrap(),
            records,
            &(creation_time.unwrap()),
        );
    });
}
