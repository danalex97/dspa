extern crate timely;

use crate::connection::producer::FIXED_BOUNDED_DELAY;
use crate::operators::buffer::Buffer;
use crate::operators::link_replies::LinkReplies;
use crate::operators::source::KafkaSource;

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

            let mut scheduled_first_notification = false;
            let mut comments_buffer: Stash<Comment> = Stash::new();
            let mut likes_buffer: Stash<Like> = Stash::new();

            let mut last_active_time: HashMap<u32, Option<usize> > = HashMap::new();

            let buffered_posts =
                posts.buffer(Exchange::new(|p: &Post| p.id as u64), FIXED_BOUNDED_DELAY);

            let linked_comments = comments.broadcast().link_replies(
                &buffered_posts,
                Pipeline,
                Pipeline,
                FIXED_BOUNDED_DELAY,
            );

            linked_comments
                .binary_notify(
                    &buffered_likes,
                    Pipeline,
                    Pipeline,
                    "LinkCommentsLikes",
                    None,
                    move |c_input, l_input, output, notificator| {
                        let mut c_data = Vec::new();
                        c_input.for_each(|cap, input| {
                            input.swap(&mut c_data);
                            for comment in c_data.drain(..) {
                                let time = comment.timestamp().clone();
                                comments_buffer.stash(time, comment);
                            }
                            notificator.notify_at(cap.retain());
                        });
                        let mut l_data = Vec::new();
                        l_input.for_each(|cap, input| {
                            input.swap(&mut l_data);
                            for like in l_data.drain(..) {
                                let time = like.timestamp().clone();
                                likes_buffer.stash(time, like);
                            }
                            notificator.notify_at(cap.retain());
                        });

                        notificator.for_each(|cap, _, _| {
                            let mut session = output.session(&cap);
                            let comments =
                                comments_buffer.extract(FIXED_BOUNDED_DELAY, *cap.time());
                            let likes = likes_buffer.extract(FIXED_BOUNDED_DELAY, *cap.time());
                            let mut likes_timestamps: Vec<_> = likes
                                .iter()
                                .map(|l: &Like| (l.timestamp, l.post_id))
                                .collect();
                            let mut comments_timestamps: Vec<_> = comments
                                .iter()
                                .map(|c: &Comment| (c.timestamp, c.reply_to_post_id.unwrap()))
                                .collect();

                            // modify the deque
                            let mut all_timestamps = likes_timestamps;
                            all_timestamps.append(&mut comments_timestamps);
                            let time_lhs = cap.time().clone() - ACTIVE_POST_PERIOD;

                            // update all stale entries even if we don't receive new data
                            for option in last_active_time.values_mut() {
                                // pop elements outside my window in the lhs
                                if let Some(timestamp) = option {
                                    // the interval is non-inclusive in the lhs
                                    if *timestamp <= time_lhs {
                                        *option = None;
                                    }
                                }
                            }

                            // push new data
                            for (new_timestamp, post_id) in all_timestamps.drain(..) {
                                // get current timestamp
                                if let Some(current_timestamp) = last_active_time
                                        .entry(post_id)
                                        .or_insert(Some(new_timestamp)) {
                                    // and if I have a better one, update it
                                    if new_timestamp > *current_timestamp {
                                        last_active_time.insert(post_id, Some(new_timestamp));
                                    }
                                }
                            }

                            // go through all posts and output the active ones
                            for (post_id, option) in last_active_time.iter() {
                                if let Some(timestamp) = option {
                                    session.give(post_id.clone());
                                }
                            }
                        })
                    },
                )
                .inspect(|x| println!("{:?}", x));
        });
    })
    .unwrap();
}
