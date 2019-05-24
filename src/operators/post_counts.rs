extern crate timely;

use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

use crate::dto::comment::Comment;
use crate::dto::common::Watermarkable;

use std::collections::HashMap;
use std::collections::HashSet;

type PostId = u32;
#[allow(dead_code)]
type PersonId = u32;

pub trait PostCounts<G, P, P2>
where
    G: Scope,
    P: ParallelizationContract<usize, (PostId, HashSet<PersonId>)>,
    P2: ParallelizationContract<usize, Comment>,
{
    fn counts(
        &self,
        comments: &Stream<G, Comment>,
        p_pact: P,
        c_pact: P2,
        collection_period: usize,
    ) -> Stream<G, (PostId, usize, usize)>;
}

impl<G, P, P2> PostCounts<G, P, P2> for Stream<G, (PostId, HashSet<PersonId>)>
where
    G: Scope<Timestamp = usize>,
    P: ParallelizationContract<usize, (PostId, HashSet<PersonId>)>,
    P2: ParallelizationContract<usize, Comment>,
{
    fn counts(
        &self,
        linked_comments: &Stream<G, Comment>,
        p_pact: P,
        c_pact: P2,
        collection_period: usize,
    ) -> Stream<G, (PostId, usize, usize)> {
        let mut comment_counts: HashMap<u32, usize> = HashMap::new();
        let mut reply_counts: HashMap<u32, usize> = HashMap::new();
        let mut first_notified = false;
        let mut active_posts_at_time: HashMap<usize, Vec<u32>> = HashMap::new();

        self.binary_notify(
            &linked_comments,
            p_pact,
            c_pact,
            "Counts",
            None,
            move |p_input, c_input, output, notificator| {
                let mut c_data = Vec::new();
                c_input.for_each(|cap, input| {
                    input.swap(&mut c_data);
                    for comment in c_data.drain(..) {
                        if !first_notified {
                            notificator.notify_at(cap.delayed(&(cap.time() + collection_period)));
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
                    notificator.notify_at(cap.delayed(&(cap.time() + collection_period)));
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
    }
}
