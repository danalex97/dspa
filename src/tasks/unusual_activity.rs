extern crate rand;
extern crate plotlib;

use plotlib::page::Page;
use plotlib::repr::Scatter;
use plotlib::view::ContinuousView;
use plotlib::style::{PointMarker, PointStyle};

use std::f64::NAN;
use std::cmp::Ordering::Equal;
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
use std::iter::FromIterator;
use std::cmp::min;

const MAX_POST_LENGTH: usize = 80;
const NUM_CLUSTERS: usize = 20;
const NOTIFY_PERIOD: usize = 12 * 60 * 60; // seconds
const MIN_POINTS: usize = 2000;

type Point = (f64, f64);

fn sqr_dist((x, y): &Point, (x2, y2): &Point) -> f64 {
    (x - x2) * (x - x2) + (y - y2) * (y - y2)
}

fn plot_points(centers : &Vec<Point>, points: &Vec<Point>) {
    let s1 = Scatter::from_slice(points).style(
       PointStyle::new()
           .marker(PointMarker::Square)
           .colour("#DD3355")
           .size(1.),
   );
   let s2 = Scatter::from_slice(centers).style(
       PointStyle::new()
           .colour("#35C788")
           .size(2.),
   );

   let v = ContinuousView::new()
        .add(&s1)
        .add(&s2)
        .x_range(0., 1.)
        .y_range(0., 1.)
        .x_label("Some varying variable")
        .y_label("The response of something");

    // A page with a single view is then saved to an SVG file
    Page::single(&v).save("scatter.svg").unwrap();
}

fn compute_centers(old_centers : &Vec<Point>, points: &Vec<Point>) -> Vec<Point> {
    let mut centers = vec![];

    let mut ord: Vec<usize> = (0..old_centers.len()).collect();
    rand::thread_rng().shuffle(&mut ord);

    // [TODO]: keep centers with most coverage!
    // keep half of the old centers
    let mut keep = NUM_CLUSTERS/2;
    for i in ord.iter() {
        if keep == 0 {
            break;
        }
        let (x, y) = old_centers[*i];
        keep -= 1;
    }

    // add new centers
    while centers.len() < NUM_CLUSTERS {
        let i = rand::thread_rng().gen_range(0, points.len());
        centers.push(points[i]);
    }

    for i in 0..20 {
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
    centers.retain(|(x, y)| *x != NAN && *y != NAN);
    centers
}

fn compute_outliers(centers : &Vec<Point>, points: &Vec<Point>) -> Vec<Point> {
    // calculate distances to closest cluster
    let mut dist = vec![];
    for point in points.iter() {
        let mut min_dist = 2.;
        for center in centers.iter() {
            if sqr_dist(center, point) < min_dist {
                min_dist = sqr_dist(center, point);
            }
        }
        dist.push(min_dist);
    }

    // compute 99th percentile
    dist.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Equal));
    let pct = (0.99 * dist.len() as f64) as usize;

    // find minimum distance for an outlier
    let mut dist_outlier = 2.;
    for i in pct..dist.len() - 1 {
        if dist[i + 1] - dist[i] > (dist[i] - dist[i - 1]) * 2. {
            dist_outlier = dist[i + 1];
            break;
        }
    }

    // find outliers
    let mut outliers = vec![];
    for point in points.iter() {
        let mut min_dist = 2.;
        for center in centers.iter() {
            if sqr_dist(center, point) < min_dist {
                min_dist = sqr_dist(center, point);
            }
        }

        if min_dist >= dist_outlier {
            outliers.push(*point);
        }
    }

    outliers
}

fn get_data_point(text : &String) -> Option<Point> {
    let mut alpha_text = text.clone();

    // remove punctuation
    for sep in "?!.,;:".chars() {
        alpha_text.replace(sep, " ");
    }
    alpha_text.retain(|c| c.is_alphabetic() || c.is_whitespace());

    let mut words: Vec<_> = alpha_text
        .split_whitespace()
        .map(|x| x.to_string())
        .collect();
    // remove proper nouns and extra spaces
    words = words.iter().map(|x| x.to_lowercase()).collect();

    let text_len = words.len();

    if text_len < 2 {
        return None;
    }

    let unique_words : HashSet<_> = HashSet::from_iter(words.iter().cloned());
    let uniq_len = unique_words.len();

    let mut unique_bigrams = HashSet::new();
    for i in 0..words.len() - 2 {
        unique_bigrams.insert(words[i].clone() + " " + &words[i + 1].clone());
    }
    // temporary
    let text_len = unique_words.len();
    let uniq_len = unique_bigrams.len();

    let text_len = if text_len > MAX_POST_LENGTH
        { 1. } else { text_len as f64 / MAX_POST_LENGTH as f64 };
    let uniq_len = if uniq_len > MAX_POST_LENGTH
        { 1. } else { uniq_len as f64 / MAX_POST_LENGTH as f64 };

    return Some((text_len, uniq_len))
}

pub fn run() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            let posts = scope.kafka_string_source::<Post>("posts".to_string());
            let buffered_posts =
                posts.buffer(Exchange::new(|p: &Post| p.id as u64), FIXED_BOUNDED_DELAY);

            let mut points: Vec<Point> = vec![];
            let mut centers: Vec<Point> = vec![];

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
                        if let Some(data_point) = get_data_point(&post.content.clone()) {
                            stash.stash(*time.time(), (data_point, post));
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
                            centers  = compute_centers(&centers, &points);
                            let outliers = compute_outliers(&centers, &points);

                            plot_points(&outliers, &points);
                        }

                        let mut session = output.session(&cap);
                        session.give(1);
                    });
                }
            });//.inspect(|x| println!("{:?}", x));
        })
    }).unwrap();
}
