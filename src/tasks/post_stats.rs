extern crate timely;

use crate::connection::producer::FIXED_BOUNDED_DELAY;
use crate::operators::buffer::Buffer;
use crate::operators::source::KafkaSource;

use crate::operators::active_posts::ActivePosts;
use crate::operators::engaged_users::EngagedUsers;
use crate::operators::link_replies::LinkReplies;

use crate::dto::comment::Comment;
use crate::dto::common::Watermarkable;
use crate::dto::like::Like;
use crate::dto::post::Post;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::broadcast::Broadcast;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::Inspect;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use timely::Configuration;

const COLLECTION_PERIOD: usize = 1800; // seconds
const ACTIVE_POST_PERIOD: usize = 43200; // seconds

pub fn run() {
    timely::execute(Configuration::Process(4), |worker| {
        let index = worker.index();
        worker.dataflow::<usize, _, _>(|scope| {
            let posts = scope.kafka_string_source::<Post>("posts".to_string(), index);
            let comments = scope.kafka_string_source::<Comment>("comments".to_string(), index);
            let likes = scope.kafka_string_source::<Like>("likes".to_string(), index);

            let buffered_likes = likes.buffer(Exchange::new(|l: &Like| {
                if l.is_watermark {
                    return l.post_id as u64;
                }
                let mut hasher = DefaultHasher::new();
                hasher.write_u32(l.post_id);
                hasher.finish()
            }));
            let buffered_posts = posts.buffer(Exchange::new(|p: &Post| {
                if p.is_watermark {
                    return p.id as u64;
                }
                let mut hasher = DefaultHasher::new();
                hasher.write_u32(p.id);
                hasher.finish()
            }));

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

            active_posts
                .engaged_users(Pipeline, 2 * COLLECTION_PERIOD)
                .inspect_batch(|t, xs| println!("#uniquely engaged people {:?}: {:?}", t, xs));

            let mut comment_counts: HashMap<u32, u64> = HashMap::new();
            let mut reply_counts: HashMap<u32, u64> = HashMap::new();
            let mut first_notified = false;
            let mut active_posts_at_time: HashMap<usize, Vec<u32>> = HashMap::new();
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
                                    notificator
                                        .notify_at(cap.delayed(&(cap.time() + COLLECTION_PERIOD)));
                                    first_notified = true;
                                }

                                // discard watermarks
                                if comment.is_watermark() {
                                    continue;
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
                            let vec = input.iter().map(|(x, _)| *x).collect();
                            active_posts_at_time.insert(*cap.time(), vec);
                        });

                        notificator.for_each(|cap, _, notificator| {
                            notificator.notify_at(cap.delayed(&(cap.time() + COLLECTION_PERIOD)));
                            let mut session = output.session(&cap);
                            for post_id in active_posts_at_time
                                .remove(cap.time())
                                .unwrap_or(vec![])
                                .drain(..)
                            {
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
                .inspect_batch(|t, xs| println!("#comments and replies {:?}: {:?}", t, xs));
        });
    })
    .unwrap();
}
