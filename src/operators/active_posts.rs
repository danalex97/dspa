use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

use crate::dto::comment::Comment;
use crate::dto::common::{Timestamped, Watermarkable};
use crate::dto::like::Like;

use crate::dsa::stash::*;
use std::collections::HashMap;
use std::collections::HashSet;

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
    ) -> Stream<G, (u32, HashSet<u32>)>;
}

// PRE: likes and comments are buffered
impl<G, P, P2> ActivePosts<G, P, P2> for Stream<G, Comment>
where
    G: Scope<Timestamp = usize>,
    P: ParallelizationContract<usize, Comment>,
    P2: ParallelizationContract<usize, Like>,
{
    // outputs (post id, user ids)
    fn active_post_ids(
        &self,
        likes: &Stream<G, Like>,
        c_pact: P,
        l_pact: P2,
        delay: usize,
        active_post_period: usize,
    ) -> Stream<G, (u32, HashSet<u32>)> {
        let mut comments_buffer: Stash<Comment> = Stash::new();
        let mut likes_buffer: Stash<Like> = Stash::new();
        let mut last_active_time: HashMap<u32, Option<usize>> = HashMap::new();

        // all persons that interact with this post from the beginning
        let mut interactions_by_post = HashMap::new();

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
                    // deliver only one notification since the streams are
                    // synchronized by the same initial watermark
                    notificator.notify_at(cap.retain());
                });
                let mut l_data = Vec::new();
                l_input.for_each(|_, input| {
                    input.swap(&mut l_data);
                    for like in l_data.drain(..) {
                        let time = like.timestamp().clone();
                        likes_buffer.stash(time, like);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    // get timestamps for likes and comments
                    let raw_comments = comments_buffer.extract(delay, *cap.time());
                    let raw_likes = likes_buffer.extract(delay, *cap.time());

                    let likes: Vec<_> = raw_likes
                        .iter()
                        .filter(|l| !l.is_watermark())
                        .map(|l| (l.timestamp, l.post_id, l.person_id))
                        .collect();
                    let mut comments: Vec<_> = raw_comments
                        .iter()
                        .filter(|c| !c.is_watermark())
                        .map(|c| (c.timestamp, c.reply_to_post_id.unwrap(), c.person_id))
                        .collect();

                    // group all timestamp together
                    let mut all_info = likes;
                    all_info.append(&mut comments);

                    // update all stale entries even if we don't receive new data
                    let time_lhs = if *cap.time() > active_post_period {
                        cap.time().clone() - active_post_period
                    } else {
                        0
                    };
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
                    for (new_timestamp, post_id, person_id) in all_info.drain(..) {
                        // get current timestamp
                        if let Some(current_timestamp) = last_active_time
                            .entry(post_id)
                            .or_insert(Some(new_timestamp))
                        {
                            // and if I have a better one, update it
                            if new_timestamp > *current_timestamp {
                                last_active_time.insert(post_id, Some(new_timestamp));
                            }
                        }

                        interactions_by_post
                            .entry(post_id)
                            .or_insert(HashSet::new())
                            .insert(person_id);
                    }

                    // go through all posts and output the active ones; the boundary of the data
                    // stream is before the actual capability, possibly meaning we miss some data
                    // between notifications.
                    let mut session = output.session(&cap);
                    for (post_id, option) in last_active_time.iter() {
                        if let Some(_timestamp) = option {
                            let interactions = match interactions_by_post.get(&post_id) {
                                Some(people) => people.clone(),
                                None => HashSet::new(),
                            };

                            session.give((post_id.clone(), interactions));
                        }
                    }
                })
            },
        )
    }
}

#[cfg(test)]
#[rustfmt::skip]
mod active_posts_tests {
    extern crate timely;

    use std::collections::HashSet;
    use std::iter::FromIterator;

    use crate::dto::common::Watermarkable;
    use crate::dto::like::Like;
    use crate::dto::comment::Comment;

    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::InputHandle;
    use timely::dataflow::operators::{Input, Inspect, Probe};

    use crate::operators::active_posts::ActivePosts;

    #[test]
    fn test_active_post_ids_detected_correctly() {
        timely::execute_from_args(std::env::args(), |worker| {
            let mut comms_input = InputHandle::new();
            let mut likes_input = InputHandle::new();

            let default_comm = Comment{is_watermark:false, ..Comment::from_watermark("0", 0)};
            let default_like = Like{is_watermark:false, ..Like::from_watermark("0", 0)};
            let comms_data = vec![
                Comment{timestamp:16, person_id:1, reply_to_post_id: Some(2), ..default_comm.clone()},
                Comment{timestamp:16, reply_to_post_id: Some(5), ..default_comm.clone()},
                Comment{timestamp:17, reply_to_post_id: Some(5), ..default_comm.clone()},
                Comment::from_watermark("20", 0),
                Comment{timestamp:22, reply_to_post_id: Some(5), ..default_comm.clone()},
                Comment{timestamp:23, reply_to_post_id: Some(11), ..default_comm.clone()},
                Comment{timestamp:23, person_id:2, reply_to_post_id: Some(10), ..default_comm.clone()},
                Comment::from_watermark("25", 0),
                Comment{timestamp:25, reply_to_post_id: Some(10), ..default_comm.clone()},
                Comment{timestamp:26, reply_to_post_id: Some(11), ..default_comm.clone()},
                Comment{timestamp:27, reply_to_post_id: Some(10), ..default_comm.clone()},
                Comment::from_watermark("30", 0),
                Comment::from_watermark("35", 0),
            ];
            let likes_data = vec![
                Like{timestamp:16, person_id:3, post_id: 1, ..default_like.clone()},
                Like::from_watermark("20", 0),
                Like{timestamp:22, post_id: 3, ..default_like.clone()},
                Like::from_watermark("25", 0),
                Like{timestamp:27, post_id: 3, ..default_like.clone()},
                Like::from_watermark("30", 0),
                Like::from_watermark("35", 0),
            ];


            let (likes_probe, comms_probe) = worker.dataflow(|scope| {
                let likes = scope.input_from(&mut likes_input);
                let comms = scope.input_from(&mut comms_input);

                comms.active_post_ids(
                    &likes,
                    Pipeline,
                    Pipeline,
                    5,
                    10,
                ).inspect_batch(move |t, xs: &[(u32, HashSet<u32>)]| match t {
                    20 => {
                        let mut vec = xs.to_vec();
                        vec.sort_by(|(x, _), (y, _)| x.cmp(&y));
                        assert_eq!(vec[0], (1, HashSet::from_iter(vec![3].iter().cloned())));
                        assert_eq!(vec[1], (2, HashSet::from_iter(vec![1].iter().cloned())));
                        assert_eq!(vec[2], (5, HashSet::from_iter(vec![0].iter().cloned())));
                    },
                    25 => {
                        let mut vec = xs.to_vec();
                        vec.sort_by(|(x, _), (y, _)| x.cmp(&y));
                        assert_eq!(vec[0], (1, HashSet::from_iter(vec![3].iter().cloned())));
                        assert_eq!(vec[1], (2, HashSet::from_iter(vec![1].iter().cloned())));
                        assert_eq!(vec[2], (3, HashSet::from_iter(vec![0].iter().cloned())));
                        assert_eq!(vec[3], (5, HashSet::from_iter(vec![0].iter().cloned())));
                        assert_eq!(vec[4], (10, HashSet::from_iter(vec![2].iter().cloned())));
                        assert_eq!(vec[5], (11, HashSet::from_iter(vec![0].iter().cloned())));
                    },
                    30 => {
                        let mut vec = xs.to_vec();
                        vec.sort_by(|(x, _), (y, _)| x.cmp(&y));
                        assert_eq!(vec[0], (3, HashSet::from_iter(vec![0].iter().cloned())));
                        assert_eq!(vec[1], (5, HashSet::from_iter(vec![0].iter().cloned())));
                        assert_eq!(vec[2], (10, HashSet::from_iter(vec![2, 0].iter().cloned())));
                        assert_eq!(vec[3], (11, HashSet::from_iter(vec![0].iter().cloned())));
                    },
                    _ => unreachable!(),
                });

                (likes.probe(), comms.probe())
            });

            let batches = vec![
                (20, comms_data[0..4].to_vec(), likes_data[0..2].to_vec()),
                (25, comms_data[4..8].to_vec(), likes_data[2..4].to_vec()),
                (30, comms_data[8..12].to_vec(), likes_data[4..6].to_vec()),
                (30, comms_data[12..13].to_vec(), likes_data[6..7].to_vec()),
            ];
            for (t, mut c_data, mut l_data) in batches {
                comms_input.send_batch(&mut c_data);
                comms_input.advance_to(t);
                likes_input.send_batch(&mut l_data);
                likes_input.advance_to(t);

                while comms_probe.less_than(comms_input.time()) {
                     worker.step();
                }
                while likes_probe.less_than(likes_input.time()) {
                     worker.step();
                }
            }
        })
        .unwrap();
    }
}
