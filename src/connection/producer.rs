extern crate futures;
extern crate rand;
extern crate rdkafka;

use futures::*;
use std::option;

use rand::Rng;
use rdkafka::client::EmptyContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use self::rdkafka::message::ToBytes;
use chrono::{DateTime, FixedOffset, Duration, Utc};
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::collections::BinaryHeap;
use chrono::offset::TimeZone;

const FIXED_BOUNDED_DELAY: u32 = 500; //seconds

pub struct Producer {
    producer: FutureProducer<EmptyContext>,
    topic: String,
    key: u32,
}

impl Producer {
    pub fn new(topic: String) -> Producer {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("produce.offset.report", "true")
            .set("message.timeout.ms", "5000")
            .create::<FutureProducer<_>>()
            .expect("Producer creation error");
        Producer {
            producer : producer,
            topic : topic,
            key : 0u32,
        }
    }

    pub fn write_file(&mut self, file_name: &str) {
        let f = File::open(file_name).unwrap();
        let f = BufReader::new(f);

        let mut epoch_start_time = FixedOffset::east(0).ymd(2000, 1, 1).and_hms_milli(12, 0, 0, 0);
        let mut buffer: BinaryHeap<(i64, String)> = BinaryHeap::new();

        for line in f.lines().skip(1) {
            let line = line.unwrap();
            let fields: Vec<&str> = line.split("|").collect();
            let creation_time = DateTime::parse_from_rfc3339(fields[2]).unwrap();
            if creation_time > epoch_start_time + Duration::seconds(FIXED_BOUNDED_DELAY as i64) {
                for (time, data) in buffer.into_sorted_vec() {
                    self.producer.send_copy(
                        &self.topic,
                        None,
                        Some(&data),
                        Some(&self.key.to_string()),
                        Some(time),
                        0
                    );
                    self.key += 1;
                }
                epoch_start_time = creation_time;
                buffer = BinaryHeap::new();
            }
            let offset = Duration::seconds(rand::thread_rng().gen_range(1, FIXED_BOUNDED_DELAY) as i64);
            let insertion_time = creation_time + offset;
            buffer.push((insertion_time.timestamp(), line));
        }

        // For the last ones
        for (time, data) in buffer.into_sorted_vec() {
            self.producer.send_copy(
                &self.topic,
                None,
                Some(&data),
                Some(&self.key.to_string()),
                Some(time),
                0
            );
            self.key += 1;
        }
    }
}
