extern crate timely;

use crate::connection::producer::FIXED_BOUNDED_DELAY;
use crate::dto::comment::Comment;
use crate::dto::common::Timestamped;
use crate::dto::post::Post;
use crate::operators::source::KafkaSource;

use crate::dsa::stash::*;

use crate::dsa::dsu::Dsu;
use crate::dto::like::Like;
use crate::operators::buffer::Buffer;
use std::collections::binary_heap::BinaryHeap;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::broadcast::Broadcast;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::input::Input;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::operators::{FrontierNotificator, Inspect};
use timely::dataflow::ProbeHandle;

const COLLECTION_PERIOD: usize = 1800; // seconds

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum KeyType {
    Post,
    Comment,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
struct Key {
    event: KeyType,
    id: u32,
}

fn add_edge(dsu: &mut Dsu<Key, (u32, u32, u32)>, parent: Key, child: Key) {
    let r2 = match dsu.value(child.clone()) {
        None => 0,
        Some((_, r2, _)) => *r2,
    };
    match dsu.value_mut(parent.clone()) {
        None => {}
        Some((_, replies, _)) => {
            *replies += r2;
            dsu.union(parent, child);
        }
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
                FIXED_BOUNDED_DELAY,
            );

            let mut all_posts = HashSet::new();
            let mut scheduled_first_notification = false;
            let mut posts_buffer: Stash<Post> = Stash::new();
            let mut likes_buffer: Stash<Like> = Stash::new();

            let posts_with_likes = posts
                .buffer(Exchange::new(|p: &Post| p.id as u64), FIXED_BOUNDED_DELAY)
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
                                notificator.notify_at(cap.delayed(
                                    &(cap.time() + COLLECTION_PERIOD - FIXED_BOUNDED_DELAY),
                                ));
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
                            }
                            for like in likes_buffer.extract(COLLECTION_PERIOD, *cap.time()) {
                                // [TODO:] we need the likes only to find the active period; that
                                // is we do not need to actually count them
                            }
                            output.session(&cap).give_vec(&mut new_posts);
                        });
                    },
                );

            let mut dsu: Dsu<Key, (u32, u32, u32)> = Dsu::new();
            let mut comments_buffer: Stash<Comment> = Stash::new();
            comments
                .broadcast()
                .binary_notify(
                    &posts_with_likes,
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

                        let mut p_data = Vec::new();
                        p_input.for_each(|cap, input| {
                            notificator.notify_at(cap.retain());

                            input.swap(&mut p_data);
                            for post in p_data.drain(..) {
                                // insert all values into DSU
                                dsu.insert(
                                    Key {
                                        event: KeyType::Post,
                                        id: post.id,
                                    },
                                    (0, 0, 0),
                                );

                                // [TODO]: modify when looking at active posts
                                all_posts.insert(post);
                            }
                        });

                        notificator.for_each(|cap, _, _| {
                            let mut replies = BinaryHeap::new();
                            for comment in comments_buffer.extract(COLLECTION_PERIOD, *cap.time()) {
                                match comment.reply_to_post_id {
                                    Some(post_id) => {
                                        // I'm a comment
                                        let mut correct_worker = false;
                                        match dsu.value_mut(Key {
                                            event: KeyType::Post,
                                            id: post_id,
                                        }) {
                                            None => {}
                                            Some((comments, _, _)) => {
                                                println!("yes {:?}", post_id);
                                                correct_worker = true;
                                                *comments += 1;
                                            }
                                        }

                                        if correct_worker {
                                            add_edge(
                                                &mut dsu,
                                                Key {
                                                    event: KeyType::Post,
                                                    id: post_id,
                                                },
                                                Key {
                                                    event: KeyType::Comment,
                                                    id: comment.id,
                                                },
                                            );
                                        }
                                    }
                                    None => {
                                        replies.push(comment);
                                    }
                                }
                            }
                            for reply in replies.into_sorted_vec().drain(..) {
                                let parent_key = Key {
                                    event: KeyType::Comment,
                                    id: reply.reply_to_comment_id.unwrap(),
                                };
                                let key = Key {
                                    event: KeyType::Comment,
                                    id: reply.id,
                                };
                                match dsu.value_mut(parent_key.clone()) {
                                    None => {}
                                    Some(_) => add_edge(&mut dsu, parent_key, key),
                                }
                            }

                            let mut session = output.session(&cap);
                            for post in all_posts.iter() {
                                match dsu.value(Key {
                                    event: KeyType::Post,
                                    id: post.id,
                                }) {
                                    None => {}
                                    Some(all) => session.give(all.clone()),
                                }
                            }
                        });
                    },
                )
                .inspect(|x| println!("Out: {:?}", x));
        });
    })
    .unwrap();
}
