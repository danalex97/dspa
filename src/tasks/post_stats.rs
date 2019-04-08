extern crate timely;

use crate::dto::post::Post;
use crate::dto::comment::Comment;
use crate::dto::common::Timestamped;
use crate::operators::source::KafkaSource;
use crate::connection::producer::FIXED_BOUNDED_DELAY;

use crate::dsa::stash::*;

use timely::dataflow::operators::{Inspect, FrontierNotificator};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pact::Exchange;
use crate::dto::like::Like;
use crate::operators::buffer::Buffer;
use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::operators::input::Input;
use std::collections::HashMap;
use std::hash::Hash;

const COLLECTION_PERIOD : usize = 1800; // seconds

pub fn run() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            let mut comments_buffer: Stash<Comment> = Stash::new();

            let posts = scope.kafka_string_source::<Post>("posts".to_string());
            let comments = scope.kafka_string_source::<Comment>("comments".to_string());
            let likes = scope.kafka_string_source::<Like>("likes".to_string());

            let buffered_likes = likes.buffer(
                Exchange::new(|l: &Like| l.post_id as u64),
                FIXED_BOUNDED_DELAY
            );

            let mut all_posts = HashMap::new();
            let mut scheduled_first_notification = false;
            let mut posts_buffer: Stash<Post> = Stash::new();
            let mut likes_buffer: Stash<Like> = Stash::new();

            posts.buffer(Exchange::new(|p: &Post| p.id as u64), FIXED_BOUNDED_DELAY)
                .binary_notify(
                    &buffered_likes,
                    Pipeline,
                    Pipeline,
                    "AggregateLikes",
                    None,
                    move |p_input, l_input, output, notificator| {
                        let mut p_data = Vec::new();
                        p_input.for_each(|cap, input| {
                            if !scheduled_first_notification {
                                // First post received
                                scheduled_first_notification = true;
                                notificator.notify_at(cap.delayed(&(cap.time() + COLLECTION_PERIOD - FIXED_BOUNDED_DELAY)));
                            }
                            input.swap(&mut p_data);
                            for post in p_data.drain(..) {
                                let time = post.timestamp().clone();
                                posts_buffer.stash(time, post);
                            }
                        });

                        let mut l_data = Vec::new();
                        l_input.for_each(|cap, input| {
                            input.swap(&mut l_data);
                            for like in l_data.drain(..) {
                                let time = like.timestamp().clone();
                                likes_buffer.stash(time, like);
                            }
                        });

                        notificator.for_each(|cap, count, notificator| {
                            notificator.notify_at(cap.delayed(&(cap.time() + COLLECTION_PERIOD)));
                            for post in posts_buffer.extract(COLLECTION_PERIOD, *cap.time()) {
                                all_posts.insert(post.id, post);
                            }
                            for like in likes_buffer.extract(COLLECTION_PERIOD, *cap.time()) {
                                match all_posts.get_mut(&like.post_id) {
                                    None => {/* no-op, probably because data is ordered poorly */},
                                    Some(post) => post.likes += 1,
                                }
                            }
                            let counts: Vec<(u32, u32)> = all_posts.iter().map(|(id, post)| {
                                (id.clone(), post.likes)
                            }).collect();
                            output.session(&cap).give(counts);
                        });


                    }
                )
                .inspect_batch(|t, xs| println!("@{}: {:?}", t, xs));
        });
    }).unwrap();
}
