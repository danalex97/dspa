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

pub fn run() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            let mut posts_buffer: HashMap<usize, Vec<Post>> = HashMap::new();

            // produces a list of all the new posts together with the timestamp of the
            // segment that needs to be processed
            let new_posts = scope
                .kafka_string_source::<Post>("posts".to_string())
                .unary_notify(Pipeline, "CollectPosts", None, move |input, output, notificator| {
                    // note that we know that the timestamp of the first event should be from
                    // a post, since we can't recive likes or comments without any post
                    let mut scheduled = false;
                    let mut first = true;
                    let mut vector = Vec::new();

                    input.for_each(|cap, posts| {
                        let time = cap.time().clone();

                        posts.swap(&mut vector);
                        for post in vector.drain(..) {
                            posts_buffer
                                .entry(time)
                                .or_insert(vec![])
                                .push(post);
                        }

                        // just schedule first point at the first received timestamp
                        if !scheduled {
                            scheduled = true;
                            notificator.notify_at(cap.delayed(&(time + MAX_DELAY)));
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

                            let mut all_posts = Vec::new();
                            for t in time - COLLECTION_PERIOD..time {
                                posts_buffer
                                    .entry(t)
                                    .and_modify(|posts| all_posts.extend(posts.drain(..)));
                            }
                            session.give((time, all_posts));

                            // schedule next periodic notification
                            notificator.notify_at(cap.delayed(&(time + COLLECTION_PERIOD)));
                        }
                    });
                });
                // .inspect(|x| println!("{:?}", x));

            // scope.kafka_string_source::<Post>("posts".to_string())
            //     .inspect(|x| println!("{:?}", x));
        });
    }).unwrap();
}
