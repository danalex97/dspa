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
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use self::timely::dataflow::operators::broadcast::Broadcast;
use crate::dsa::dsu::Dsu;
use std::collections::binary_heap::BinaryHeap;

const COLLECTION_PERIOD : usize = 1800; // seconds

fn add_edge(dsu: &mut Dsu<(u32, u32), (u32, u32, u32)>, parent: (u32, u32), child: (u32, u32)) {
    let r2 = match dsu.value(child) {
        None => 0,
        Some((_, r2, _)) => *r2,
    };
    match dsu.value_mut(parent) {
        None => {},
        Some((_, replies, _)) => {
            *replies += r2;
            dsu.union(parent, child);
        },
    }
}

pub fn run() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            let posts = scope.kafka_string_source::<Post>("posts".to_string());
            let comments = scope.kafka_string_source::<Comment>("comments".to_string());
            let likes = scope.kafka_string_source::<Like>("likes".to_string());

            let buffered_likes = likes.buffer(
                Exchange::new(|l: &Like| l.post_id as u64),
                FIXED_BOUNDED_DELAY
            );

            let mut all_posts = HashSet::new();
            let mut dsu = Dsu::new();
            let mut scheduled_first_notification = false;
            let mut posts_buffer: Stash<Post> = Stash::new();
            let mut likes_buffer: Stash<Like> = Stash::new();

            let aggregated_likes = posts.buffer(Exchange::new(|p: &Post| p.id as u64), FIXED_BOUNDED_DELAY)
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
                            let mut new_posts = vec![];
                            for post in posts_buffer.extract(COLLECTION_PERIOD, *cap.time()) {
                                new_posts.push(post);
                                dsu.insert((0, post.id.clone()), (0, 0, 0));
                                all_posts.insert(post);
                            }
                            for like in likes_buffer.extract(COLLECTION_PERIOD, *cap.time()) {
                                match dsu.value_mut((0, like.post_id)) {
                                    None => {/* no-op, probably because data is ordered poorly */},
                                    Some(counts) => counts.2 += 1,
                                }
                            }
                            output.session(&cap).give_vec(&mut new_posts);
                        });


                    }
                );

            let mut comments_buffer: Stash<Comment> = Stash::new();
            let mut dsu: Dsu<(u32, u32), (u32, u32, u32)> = Dsu::new();
            comments.broadcast()
                .binary_notify(
                    &aggregated_likes,
                    Pipeline,
                    Pipeline,
                    "MatchComments",
                    None,
                    move |c_input, p_input, output, notificator| {
                        let mut c_data = Vec::new();
                        c_input.for_each(|cap, input| {
                            input.swap(&mut c_data);
                            for comment in c_data.drain(..) {
                                let time = comment.timestamp().clone();
                                comments_buffer.stash(time, comment);
                            }
                        });

                        p_input.for_each(|cap, _| {
                            notificator.notify_at(cap.retain());
                        });

                        notificator.for_each(|cap, _, _| {
                            let mut replies = BinaryHeap::new();
                            for comment in comments_buffer.extract(COLLECTION_PERIOD, *cap.time()) {
                                match comment.reply_to_post_id {
                                    Some(post_id) => {
                                        // I'm a comment
                                        match dsu.value_mut((0, post_id)) {
                                            None => {/*I'm not on this node*/},
                                            Some((comments, _, _)) => {
                                                add_edge(&mut dsu, (0, post_id), (1, comment.id));
                                                *comments += 1;
                                            },
                                        }
                                    },
                                    None => {
                                        replies.push(comment);
                                    }
                                }
                            }
                            for reply in replies.into_sorted_vec().drain(..) {
                                match dsu.value_mut((1, reply.reply_to_comment_id.unwrap())) {
                                    None => {/*I'm not on this node*/},
                                    Some(_) => {
                                        add_edge(&mut dsu, (1, reply.reply_to_comment_id.unwrap()), (1, reply.id))
                                    },
                                }
                            }
                            output.session(&cap).give_iterator(all_posts.iter());
                        });
                    }
                );
        });
    }).unwrap();
}
