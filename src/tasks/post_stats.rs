extern crate timely;

use std::collections::HashMap;
use std::cmp::min;

use crate::dto::post::Post;
use crate::dto::comment::Comment;
use crate::dto::common::Timestamped;
use crate::operators::source::KafkaSource;

use timely::dataflow::operators::{Inspect, FrontierNotificator};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Pipeline;

const COLLECTION_PERIOD : usize = 1800; // seconds
const MAX_DELAY : usize = 500; // seconds

fn stash<T>(container : &mut HashMap<usize, Vec<T>>, time : usize, value : T) {
    container
        .entry(time)
        .or_insert(vec![])
        .push(value);
}

fn drain_period<T>(container : &mut HashMap<usize, Vec<T>>, t1 : usize, t2 : usize) -> Vec<T> {
    let mut all = Vec::new();
    for t in t1..t2 {
        container
            .entry(t)
            .and_modify(|vec| all.extend(vec.drain(..)));
    }
    return all;
}

pub fn run() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            let mut comments_buffer: HashMap<usize, Vec<Comment>> = HashMap::new();
            let mut posts_buffer: HashMap<usize, Vec<Post>> = HashMap::new();

            let posts = scope.kafka_string_source::<Post>("posts".to_string());
            let comments = scope.kafka_string_source::<Comment>("comments".to_string());

            posts
                .binary_notify(&comments, Pipeline, Pipeline, "CollectComments", None,
                    move |p_input, c_input, output, notificator| {
                    // note that we know that the timestamp of the first event should be from
                    // a post, since we can't recive likes or comments without any post
                    let mut scheduled = false;
                    let mut first = true;
                    let mut p_vector = Vec::new();

                    p_input.for_each(|cap, posts| {
                        posts.swap(&mut p_vector);
                        for post in p_vector.drain(..) {
                            // buffer posts
                            let time = post.timestamp().clone();
                            stash(&mut posts_buffer, post.timestamp(), post);

                            // schedule finding the first chronolgical event at first received event
                            if !scheduled {
                                scheduled = true;
                                notificator.notify_at(cap.delayed(&(time + MAX_DELAY)));
                            }
                        }
                    });

                    let mut c_vector = Vec::new();
                    c_input.for_each(|cap, comments| {
                        // buffer comments
                        comments.swap(&mut c_vector);
                        for comment in c_vector.drain(..) {
                            stash(&mut comments_buffer, comment.timestamp(), comment);
                        }
                    });

                    notificator.for_each(|cap, _, notificator| {
                        let time = cap.time().clone();

                        if first {
                            first = false;

                            // find first event
                            for t in time - MAX_DELAY..time {
                                if posts_buffer.contains_key(&t) {
                                    // found first event timestamp, so we periodic notifications
                                    notificator.notify_at(cap.delayed(&(t + COLLECTION_PERIOD)));
                                    break;
                                }
                            }
                        } else {
                            // get all newly created posts in last 30 minutes window
                            let mut session = output.session(&cap);

                            // process all posts
                            for post in drain_period(
                                &mut posts_buffer, time - COLLECTION_PERIOD, time
                            ).drain(..) {
                                // [TODO]: process posts
                                println!("{:?}", post);
                            }

                            // process all comments
                            for comment in drain_period(
                                &mut comments_buffer, time - COLLECTION_PERIOD, time
                            ).drain(..) {
                                // [TODO]: process comments
                                println!("{:?}", comment);
                            }

                            // [TODO]: simplify version
                            session.give(1);

                            // schedule next periodic notification
                            notificator.notify_at(cap.delayed(&(time + COLLECTION_PERIOD)));
                        }
                    });
                });
        });
    }).unwrap();
}
