extern crate rand;
extern crate timely;

use crate::dsa::kmeans::*;
use crate::dsa::stash::*;
use crate::dto::post::Post;
use crate::operators::buffer::Buffer;
use crate::operators::source::KafkaSource;
use crate::util::Plotter;

use std::collections::HashSet;
use std::iter::FromIterator;
use std::string::ToString;

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::inspect::Inspect;

const MAX_POST_LENGTH: usize = 64;
const NUM_CLUSTERS: usize = 10;
const MIN_COVERAGE: usize = 30;
const NOTIFY_PERIOD: usize = 12 * 60 * 60; // seconds
const MIN_POINTS: usize = 2000;

fn get_data_point(text: &String) -> Option<Point> {
    let mut alpha_text = text.clone();

    // remove punctuation
    for sep in "?!.,;:".chars() {
        alpha_text = alpha_text.replace(sep, " ");
    }

    // keep only alpha text
    alpha_text.retain(|c| c.is_alphabetic() || c.is_whitespace());
    let mut words: Vec<_> = alpha_text
        .split_whitespace()
        .map(|x| x.to_string())
        .collect();

    // lower case
    words = words.iter().map(|x| x.to_lowercase()).collect();

    let text_len = words.len();
    if text_len < 2 {
        return None;
    }

    let unique_words: HashSet<_> = HashSet::from_iter(words.iter().cloned());
    let mut unique_bigrams = HashSet::new();
    for i in 0..words.len() - 2 {
        unique_bigrams.insert(words[i].clone() + " " + &words[i + 1].clone());
    }

    let uniq_words_len = unique_words.len();
    let uniq_bigam_len = unique_bigrams.len();

    let uniq_words_len = if uniq_words_len > MAX_POST_LENGTH {
        1.
    } else {
        uniq_words_len as f64 / MAX_POST_LENGTH as f64
    };
    let uniq_bigam_len = if uniq_bigam_len > MAX_POST_LENGTH {
        1.
    } else {
        uniq_bigam_len as f64 / MAX_POST_LENGTH as f64
    };

    return Some((uniq_words_len, uniq_bigam_len));
}

pub fn run() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            let posts = scope.kafka_string_source::<Post>("posts".to_string());
            let buffered_posts = posts.buffer(Exchange::new(|p: &Post| p.id as u64));

            let mut points: Vec<Point> = vec![];
            let mut centers: Vec<Point> = vec![];

            let mut first_notified = false;
            let mut stash = Stash::new();

            let mut plotter = Plotter::new();
            buffered_posts
                .unary_notify(
                    Pipeline,
                    "Unusual Activity",
                    None,
                    move |input, output, notificator| {
                        let mut vec = vec![];
                        while let Some((time, data)) = input.next() {
                            data.swap(&mut vec);
                            if !first_notified {
                                notificator.notify_at(time.delayed(&(time.time() + NOTIFY_PERIOD)));
                                first_notified = true;
                            }
                            for post in vec.drain(..) {
                                if let Some(data_point) = get_data_point(&post.content.clone()) {
                                    stash.stash(*time.time(), (data_point, post));
                                }
                            }

                            notificator.for_each(|cap, _, notificator| {
                                notificator.notify_at(cap.delayed(&(cap.time() + NOTIFY_PERIOD)));

                                let possible_outliers = stash.extract(NOTIFY_PERIOD, *cap.time());
                                for (point, _) in possible_outliers.iter() {
                                    points.push(*point);
                                }

                                if points.len() > MIN_POINTS {
                                    centers = compute_centers(
                                        &centers,
                                        &points,
                                        NUM_CLUSTERS,
                                        MIN_COVERAGE,
                                    );
                                    let outliers = compute_outliers(&centers, &points);

                                    // plot points for debugging
                                    plotter.plot_points(&centers, &points, &outliers);

                                    // finding outliers from current batch
                                    let mut session = output.session(&cap);
                                    let mut people_ids = HashSet::new();
                                    for outlier in outliers {
                                        for (point, post) in possible_outliers.iter() {
                                            if sqr_dist(point, &outlier) < EPS {
                                                if !people_ids.contains(&post.person_id) {
                                                    session.give(post.person_id);
                                                    people_ids.insert(&post.person_id);
                                                }
                                            }
                                        }
                                    }
                                }
                            });
                        }
                    },
                )
                .inspect(|x| println!("{:?}", x));
        })
    })
    .unwrap();
}
