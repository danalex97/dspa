extern crate timely;

use crate::connection::producer::FIXED_BOUNDED_DELAY;
use crate::operators::buffer::Buffer;
use crate::operators::source::KafkaSource;

use crate::operators::active_posts::ActivePosts;
use crate::operators::link_replies::LinkReplies;

use crate::dto::comment::Comment;
use crate::dto::common::Timestamped;
use crate::dto::like::Like;
use crate::dto::post::Post;

use crate::dsa::stash::*;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::broadcast::Broadcast;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::Inspect;

use std::collections::HashMap;

const COLLECTION_PERIOD: usize = 1800; // seconds
const ACTIVE_POST_PERIOD: usize = 43200; // seconds

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

            let buffered_posts =
                posts.buffer(Exchange::new(|p: &Post| p.id as u64), FIXED_BOUNDED_DELAY);

            let linked_comments = comments.broadcast().link_replies(
                &buffered_posts,
                Pipeline,
                Pipeline,
                FIXED_BOUNDED_DELAY,
            );

            let active_posts = linked_comments.active_post_ids(
                &buffered_likes,
                Pipeline,
                Pipeline,
                FIXED_BOUNDED_DELAY,
                ACTIVE_POST_PERIOD,
            );

            let mut first_notified_engaged = false;
            let mut engaged = HashMap::new();
            let mut active_post_snapshot = Vec::new();
            active_posts
                .binary_notify(
                    &linked_comments,
                    Pipeline,
                    Pipeline,
                    "Counts",
                    None,
                    move |p_input, c_input, output, notificator| {
                        // only used for synchonization
                        // TODO: check if needed
                        let mut c_data = Vec::new();
                        c_input.for_each(|cap, input| {
                            input.swap(&mut c_data);
                            for comment in c_data.drain(..) {
                                if !first_notified_engaged {
                                    notificator.notify_at(cap.delayed(
                                        &(cap.time() + 2 * COLLECTION_PERIOD - FIXED_BOUNDED_DELAY),
                                    ));
                                    first_notified_engaged = true;
                                }
                            }
                        });

                        // actual processing
                        p_input.for_each(|cap, input| {
                            input.swap(&mut active_post_snapshot);
                            for (post_id, engaged_people) in active_post_snapshot.clone() {
                                let mut entry =
                                    engaged.entry(post_id).or_insert(engaged_people.len());
                                *entry = engaged_people.len();
                            }
                        });

                        notificator.for_each(|cap, _, notificator| {
                            notificator
                                .notify_at(cap.delayed(&(cap.time() + 2 * COLLECTION_PERIOD)));

                            let mut session = output.session(&cap);
                            for (post_id, _) in active_post_snapshot.drain(..) {
                                let engaged_users = match engaged.get(&post_id) {
                                    Some(value) => *value,
                                    None => 0,
                                };
                                session.give((post_id, engaged_users));
                            }
                        });
                    },
                )
                .inspect_batch(|t, xs| println!("@t {:?}: {:?}", t, xs));

            let mut comment_counts: HashMap<u32, u64> = HashMap::new();
            let mut reply_counts: HashMap<u32, u64> = HashMap::new();
            let mut first_notified = false;
            let mut active_post_snapshot = Vec::new();
            active_posts
                .binary_notify(
                    &linked_comments,
                    Pipeline,
                    Pipeline,
                    "Counts",
                    None,
                    move |p_input, c_input, output, notificator| {
                        let mut c_data = Vec::new();
                        c_input.for_each(|cap, input| {
                            input.swap(&mut c_data);
                            for comment in c_data.drain(..) {
                                if !first_notified {
                                    notificator.notify_at(cap.delayed(
                                        &(cap.time() + COLLECTION_PERIOD - FIXED_BOUNDED_DELAY),
                                    ));
                                    first_notified = true;
                                }
                                match comment.reply_to_comment_id {
                                    Some(_) => {
                                        // Reply
                                        let count = reply_counts
                                            .entry(comment.reply_to_post_id.unwrap())
                                            .or_insert(0);
                                        *count += 1;
                                    }
                                    None => {
                                        // Comment
                                        let count = comment_counts
                                            .entry(comment.reply_to_post_id.unwrap())
                                            .or_insert(0);
                                        *count += 1;
                                    }
                                }
                            }
                        });

                        p_input.for_each(|cap, input| {
                            input.swap(&mut active_post_snapshot);
                        });

                        notificator.for_each(|cap, _, notificator| {
                            notificator.notify_at(cap.delayed(&(cap.time() + COLLECTION_PERIOD)));
                            let mut session = output.session(&cap);
                            for (post_id, _) in active_post_snapshot.drain(..) {
                                let replies = match reply_counts.get(&post_id) {
                                    Some(count) => *count,
                                    None => 0,
                                };
                                let comments = match comment_counts.get(&post_id) {
                                    Some(count) => *count,
                                    None => 0,
                                };
                                session.give((post_id, comments, replies));
                            }
                        })
                    },
                )
                .inspect_batch(|t, xs| println!("@t2 {:?}: {:?}", t, xs));
        });
    })
    .unwrap();
}
