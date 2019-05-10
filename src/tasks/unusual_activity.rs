extern crate rand;

use std::string::ToString;
use crate::operators::source::KafkaSource;
use crate::dto::post::Post;
use rand::Rng;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::inspect::Inspect;
use crate::dsa::stash::*;
use std::collections::HashSet;
use std::cmp::min;

const MAX_POST_LENGTH: u8 = 50;
const NUM_CLUSTERS: usize = 5;
const FIRST_NOTIFY: usize = 10 * 60 * 60; // seconds

pub fn run() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            let posts = scope.kafka_string_source::<Post>("posts".to_string());

            type Point = (f64, f64);

            let mut clusters: Vec<Point> = vec![];
            for _ in 0..NUM_CLUSTERS {
                clusters.push((rand::thread_rng().gen_range(0f64, 1f64), rand::thread_rng().gen_range(0f64, 1f64)));
            }

            let mut first_notified = false;
            let mut stash = Stash::new();
            posts.unary_notify(Pipeline, "Unusual Activity", None, move |input, output, notificator| {
                let mut vec = vec![];
                while let Some((time, data)) = input.next() {
                    data.swap(&mut vec);
                    if !first_notified {
                        notificator.notify_at(time.delayed(&(time.time() + FIRST_NOTIFY)));
                        first_notified = true;
                    }
                    for post in vec.drain(..) {
                        let text: String = post.content;
                        let split = text.split_whitespace();
                        let mut text_length = 0;
                        let mut unique_words = HashSet::new();
                        for word in split {
                            text_length += 1;
                            unique_words.insert(word.to_lowercase());
                        }
                        if text_length != 0 {
                            //let normalised_length = min(text_length as f64 / MAX_POST_LENGTH as f64, 1f64);
                            let normalised_length : f64 =
                                if text_length > MAX_POST_LENGTH {
                                    1f64
                                } else {
                                    text_length as f64 / MAX_POST_LENGTH as f64
                                }
                            ;
                            let ratio_unique = unique_words.len() as f64 / text_length as f64;
                            stash.stash(*time.time(), ((ratio_unique, normalised_length), post.id));
                        }
                    }
                }

            }).inspect(|x| println!("{:?}", x));
        })
    }).unwrap();
}
