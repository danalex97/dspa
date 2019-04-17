use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};
use timely::Data;

use crate::dto::common::Timestamped;
use crate::dto::comment::Comment;
use crate::dto::like::Like;

use crate::dsa::stash::*;
use std::collections::HashMap;

pub trait ActivePosts<G, P, P2>
where
    G: Scope,
    P: ParallelizationContract<usize, Comment>,
    P2: ParallelizationContract<usize, Like>,
{
    fn active_post_ids(
        &self,
        likes: &Stream<G, Like>,
        c_pact: P,
        l_pact: P2,
        delay: usize,
        active_post_period: usize,
    ) -> Stream<G, u32>;
}

// PRE: likes and comments are buffered
impl<G, P, P2> ActivePosts<G, P, P2> for Stream<G, Comment>
where
    G: Scope<Timestamp = usize>,
    P: ParallelizationContract<usize, Comment>,
    P2: ParallelizationContract<usize, Like>,
{
    fn active_post_ids(
        &self,
        likes: &Stream<G, Like>,
        c_pact: P,
        l_pact: P2,
        delay: usize,
        active_post_period: usize,
    ) -> Stream<G, u32> {
        let mut comments_buffer: Stash<Comment> = Stash::new();
        let mut likes_buffer: Stash<Like> = Stash::new();
        let mut last_active_time: HashMap<u32, Option<usize> > = HashMap::new();

        self.binary_notify(
            &likes,
            c_pact,
            l_pact,
            "ActivePosts",
            None,
            move |c_input, l_input, output, notificator| {
                // stash comments and likes
                let mut c_data = Vec::new();
                c_input.for_each(|cap, input| {
                    input.swap(&mut c_data);
                    for comment in c_data.drain(..) {
                        let time = comment.timestamp().clone();
                        comments_buffer.stash(time, comment);
                    }
                    // deliver notifications for both for synchonization
                    notificator.notify_at(cap.retain());
                });
                let mut l_data = Vec::new();
                l_input.for_each(|cap, input| {
                    input.swap(&mut l_data);
                    for like in l_data.drain(..) {
                        let time = like.timestamp().clone();
                        likes_buffer.stash(time, like);
                    }
                    // deliver notifications for both for synchonization
                    notificator.notify_at(cap.retain());
                });

                notificator.for_each(|cap, _, _| {
                    // get timestamps for likes and comments
                    let comments =
                        comments_buffer.extract(delay, *cap.time());
                    let likes = likes_buffer.extract(delay, *cap.time());
                    let mut likes_timestamps: Vec<_> = likes
                        .iter()
                        .map(|l: &Like| (l.timestamp, l.post_id))
                        .collect();
                    let mut comments_timestamps: Vec<_> = comments
                        .iter()
                        .map(|c: &Comment| (c.timestamp, c.reply_to_post_id.unwrap()))
                        .collect();

                    // group all timestamp together
                    let mut all_timestamps = likes_timestamps;
                    all_timestamps.append(&mut comments_timestamps);

                    // update all stale entries even if we don't receive new data
                    let time_lhs = cap.time().clone() - active_post_period;
                    for option in last_active_time.values_mut() {
                        // pop elements outside my window in the lhs
                        if let Some(timestamp) = option {
                            // the interval is non-inclusive in the lhs
                            if *timestamp <= time_lhs {
                                *option = None;
                            }
                        }
                    }

                    // push new timestamps
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
                    let mut session = output.session(&cap);
                    for (post_id, option) in last_active_time.iter() {
                        if let Some(timestamp) = option {
                            session.give(post_id.clone());
                        }
                    }
                })
            },
        )
    }
}
