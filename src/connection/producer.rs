extern crate futures;
extern crate rand;
extern crate rdkafka;

use crate::dsa::stash::{Stash, Stashable};
use chrono::{DateTime, Duration, FixedOffset};
use rand::Rng;

use rdkafka::config::ClientConfig;
use rdkafka::message::ToBytes;
use rdkafka::producer::{FutureProducer, FutureRecord};

use futures::future::Future;
use std::fmt::Debug;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub const FIXED_BOUNDED_DELAY: usize = 300; //seconds

pub struct Producer {
    producer: FutureProducer,
    topic: String,
    key: u32,
}

trait Data: Debug + ToBytes {}

impl Producer {
    pub fn new(topic: String) -> Producer {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("produce.offset.report", "true")
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");
        Producer {
            producer: producer,
            topic: topic,
            key: 0u32,
        }
    }

    pub fn write_file(
        &mut self,
        file_name: &str,
        lines: Option<usize>,
        start_time: &DateTime<FixedOffset>,
    ) -> usize {
        let f = File::open(file_name).unwrap();
        let f = BufReader::new(f);

        let mut stash = Stash::new();

        let mut epoch_start_time = start_time.clone();
        let mut futures = Vec::new();

        let mut cnt = 0;
        for line in f.lines().skip(1) {
            let line = line.unwrap();
            let fields: Vec<&str> = line.split("|").collect();
            let creation_time = DateTime::parse_from_rfc3339(fields[2]).unwrap();

            if creation_time <= epoch_start_time + Duration::seconds(FIXED_BOUNDED_DELAY as i64) {} else {
                let old_time = epoch_start_time.timestamp();
                // Generate some watermarks for every five minute period between
                // and output the stashed lines
                while creation_time
                    > epoch_start_time + Duration::seconds(FIXED_BOUNDED_DELAY as i64)
                    {
                        // Gen watermark
                        for i in 0..4 {
                            stash.stash(
                                epoch_start_time.timestamp() as usize,
                                (
                                    epoch_start_time,
                                    "Watermark|".to_owned() + &epoch_start_time.timestamp().to_string(),
                                    Some(i),
                                ),
                            );
                        }
                        epoch_start_time =
                            epoch_start_time + Duration::seconds(FIXED_BOUNDED_DELAY as i64);
                    }
                let mut stashed_lines = stash.extract(
                    (epoch_start_time.timestamp() - old_time + 1) as usize,
                    epoch_start_time.timestamp() as usize,
                );
                for (timestamp, stashed_line, maybe_partition) in stashed_lines.drain(..) {
                    let future = match maybe_partition {
                        None => {
                            self.producer.send(
                                FutureRecord::to(&self.topic)
                                    .payload(&stashed_line)
                                    .key(&self.key.to_string())
                                    .timestamp(timestamp.timestamp()),
                                0,
                            )
                        },
                        Some(partition) => {
                            self.producer.send(
                                FutureRecord::to(&self.topic)
                                    .payload(&stashed_line)
                                    .partition(partition)
                                    .key(&self.key.to_string())
                                    .timestamp(timestamp.timestamp()),
                                0,
                            )
                        }
                    };
                    self.key += 1;
                    futures.push(future);
                    if let Some(lines) = lines {
                        if cnt >= lines {
                            for future in futures {
                                match future.wait() {
                                    Ok(_) => (),
                                    Err(e) => println!("{:?}", e),
                                }
                            }
                            return cnt;
                        }
                    }
                }
            }
            // Stash the line with a random fixed bounded delay
            let offset =
                Duration::seconds(rand::thread_rng().gen_range(1, FIXED_BOUNDED_DELAY) as i64);
            let insertion_time = creation_time + offset;
            stash.stash(insertion_time.timestamp() as usize, (insertion_time, line, None));
            cnt += 1;
        }

        for future in futures {
            match future.wait() {
                Ok(_) => (),
                Err(e) => println!("{:?}", e),
            }
        }
        cnt
    }
}
