extern crate rand;

use std::string::ToString;
use crate::operators::source::KafkaSource;
use crate::connection::producer::FIXED_BOUNDED_DELAY;
use crate::operators::buffer::Buffer;
use crate::dto::post::Post;
use rand::Rng;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::inspect::Inspect;
use crate::dsa::stash::*;
use std::collections::HashSet;
use std::collections::HashMap;
use std::cmp::min;

const MAX_POST_LENGTH: u8 = 200;
const NUM_CLUSTERS: usize = 20;
const NOTIFY_PERIOD: usize = 12 * 60 * 60; // seconds
const MIN_POINTS: usize = 1000;

type Point = (f64, f64);

fn sqr_dist((x, y): &Point, (x2, y2): &Point) -> f64 {
    (x - x2) * (x - x2) + (y - y2) * (y - y2)
}

fn compute_centers(centers: &mut Vec<Point>, points: &Vec<Point>) {
    if centers.len() == 0 {
        for _ in 0..NUM_CLUSTERS {
            let i = rand::thread_rng().gen_range(0, points.len());
            centers.push(points[i]);
        }
    }

    for _ in 0..20 {
        let mut clusters = Vec::new();
        for _ in 0..NUM_CLUSTERS {
            clusters.push(vec![]);
        }
        for point in points {
            let mut i_closest = 0;
            for (i, center) in centers.iter().enumerate() {
                if sqr_dist(&center, &point) < sqr_dist(&centers[i_closest], &point) {
                    i_closest = i;
                }
            }

            clusters[i_closest].push(point);
        }

        let mut new_centers = vec![];
        for cluster in clusters {
            let mut x_c = 0f64;
            let mut y_c = 0f64;
            for (x, y) in cluster.iter() {
                x_c += x;
                y_c += y;
            }
            x_c /= cluster.len() as f64;
            y_c /= cluster.len() as f64;
            new_centers.push((x_c, y_c));
        }

        let mut tot_diff = 0f64;
        for i in 0..NUM_CLUSTERS {
            tot_diff += sqr_dist(&new_centers[i], &centers[i]);
        }

        if tot_diff < 0.00001 {
            break;
        }
        for i in 0..NUM_CLUSTERS {
            centers[i] = new_centers[i];
        }
    }
    println!("final {:?}", centers);
}

pub fn run() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            let posts = scope.kafka_string_source::<Post>("posts".to_string());
            let buffered_posts =
                posts.buffer(Exchange::new(|p: &Post| p.id as u64), FIXED_BOUNDED_DELAY);

            let mut centers: Vec<Point> = vec![];
            let mut points: Vec<Point> = vec![];

            let mut first_notified = false;
            let mut stash = Stash::new();
            buffered_posts.unary_notify(Pipeline, "Unusual Activity", None, move |input, output, notificator| {
                let mut vec = vec![];
                while let Some((time, data)) = input.next() {
                    data.swap(&mut vec);
                    if !first_notified {
                        notificator.notify_at(time.delayed(&(time.time() + NOTIFY_PERIOD)));
                        first_notified = true;
                    }
                    for post in vec.drain(..) {
                        let text: String = post.content.clone();
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
                                };
                            let ratio_unique = unique_words.len() as f64 / text_length as f64;
                            stash.stash(*time.time(), ((ratio_unique, normalised_length), post));
                        }
                    }

                    notificator.for_each(|cap, _, notificator| {
                        notificator.notify_at(cap.delayed(&(cap.time() + NOTIFY_PERIOD)));

                        let mut possible_outliers = HashMap::new();
                        for (point, post) in stash.extract(NOTIFY_PERIOD, *cap.time()) {
                            possible_outliers.insert(post.id, point);
                            points.push(point);
                        }

                        if points.len() > MIN_POINTS {
                            compute_centers(&mut centers, &points);
                        }

                        let mut session = output.session(&cap);
                        session.give(1);
                    });
                }
            });//.inspect(|x| println!("{:?}", x));
        })
    }).unwrap();
}
