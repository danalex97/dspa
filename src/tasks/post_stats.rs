extern crate timely;

use crate::connection::producer::FIXED_BOUNDED_DELAY;
use crate::operators::source::KafkaSource;
use crate::operators::buffer::Buffer;

use crate::dto::like::Like;
use crate::dto::comment::Comment;
use crate::dto::common::Timestamped;
use crate::dto::post::Post;

use crate::dsa::stash::*;
use crate::dsa::dsu::Dsu;

use std::collections::binary_heap::BinaryHeap;
use std::collections::HashSet;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::broadcast::Broadcast;
use timely::dataflow::operators::Inspect;

const COLLECTION_PERIOD: usize = 1800; // seconds
const POST: u32 = 0;
const COMMENT: u32 = 1;

type Node = (u32, u32);

fn add_edge(dsu: &mut Dsu<Node, (u32, u32)>, parent: Node, child: Node) {
    let (c, r) = match dsu.value(child.clone()) {
        None => (0, 0),
        Some((c, r)) => (*c, *r),
    };
    match dsu.value_mut(parent.clone()) {
        None => {}
        Some((comments, replies)) => {
            *comments += c;
            *replies  += r;

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
                            // schedule the first notification
                            if !scheduled_first_notification {
                                scheduled_first_notification = true;
                                notificator.notify_at(cap.delayed(
                                    &(cap.time() + COLLECTION_PERIOD - FIXED_BOUNDED_DELAY),
                                ));
                            }

                            // stash all posts
                            input.swap(&mut p_data);
                            for post in p_data.drain(..) {
                                let time = post.timestamp().clone();
                                posts_buffer.stash(time, post);
                            }
                        });

                        // stash all likes
                        let mut l_data = Vec::new();
                        l_input.for_each(|_, input| {
                            input.swap(&mut l_data);
                            for like in l_data.drain(..) {
                                let time = like.timestamp().clone();
                                likes_buffer.stash(time, like);
                            }
                        });

                        notificator.for_each(|cap, _, notificator| {
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

            // Map from (type, id) to (#comments, #replies)
            let mut dsu: Dsu<Node, (u32, u32)> = Dsu::new();
            let mut comments_buffer: Stash<Comment> = Stash::new();

            // [TODO:] all_posts is only used for debugging
            let mut all_posts = HashSet::new();
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
                        c_input.for_each(|_, input| {
                            input.swap(&mut c_data);
                            for comment in c_data.drain(..) {
                                let time = comment.timestamp().clone();
                                comments_buffer.stash(time, comment);
                            }
                        });

                        let mut p_data = Vec::new();
                        p_input.for_each(|cap, input| {
                            input.swap(&mut p_data);
                            for post in p_data.drain(..) {
                                // insert posts values into DSU
                                dsu.insert((POST, post.id), (0, 0));

                                // [TODO:] modify when looking at active posts
                                all_posts.insert(post);
                            }

                            // notify in the future with FIXED_BOUNDED_DELAY so that we have the
                            // guarantee that all replies are attached to comments
                            notificator.notify_at(cap.delayed(
                                &(cap.time() + FIXED_BOUNDED_DELAY),
                            ));
                        });

                        notificator.for_each(|cap, _, _| {
                            // [TODO: check again what to do with this timestamp]
                            // note the time here is in the past since we waited for the delay
                            let time = *cap.time() - FIXED_BOUNDED_DELAY;
                            let mut replies = BinaryHeap::new();

                            // handle comments and stash replies
                            for comment in comments_buffer.extract(COLLECTION_PERIOD, time) {
                                match comment.reply_to_post_id {
                                    Some(post_id) => {
                                        // insert comment if post present
                                        match dsu.value((POST, post_id)) {
                                            Some(_) => dsu.insert((COMMENT, comment.id), (1, 0)),
                                            None    => {/* the post is on a different worker */},
                                        }

                                        // add edge
                                        add_edge(&mut dsu, (POST, post_id), (COMMENT, comment.id));
                                    },
                                    None => {
                                        // reply
                                        replies.push(comment);
                                    }
                                }
                            }

                            // handle replies
                            for reply in replies.into_sorted_vec().drain(..) {
                                match reply.reply_to_comment_id {
                                    Some(comm_id) => {
                                        // insert reply if comment present
                                        match dsu.value((COMMENT, comm_id)) {
                                            Some(_) => dsu.insert((COMMENT, reply.id), (0, 1)),
                                            None    => {/* the comment is on a different worker */},
                                        }

                                        // add edge
                                        add_edge(&mut dsu, (COMMENT, comm_id), (COMMENT, reply.id));
                                    }
                                    None => {/* reply attached to nothing */},
                                }
                            }

                            // [TODO:] this is currently used only for debugging
                            let mut session = output.session(&cap);

                            for post in all_posts.iter() {
                                match dsu.value((POST, post.id)) {
                                    None => {}
                                    Some(all) => {
                                        if all.0 != 0 {
                                            session.give((post.id, all.clone()));
                                        }
                                    },
                                }
                            }
                        });
                    },
                ).inspect_batch(|t, xs| println!("{:?} {:?}", t, xs));
        });
    })
    .unwrap();
}
