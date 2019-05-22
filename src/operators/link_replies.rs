use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

use crate::dto::comment::Comment;
use crate::dto::common::{Timestamped, Watermarkable};
use crate::dto::post::Post;

use crate::dsa::dsu::*;
use crate::dsa::stash::*;

use std::collections::binary_heap::BinaryHeap;

pub trait LinkReplies<G, P, P2>
where
    G: Scope,
    P: ParallelizationContract<usize, Comment>,
    P2: ParallelizationContract<usize, Post>,
{
    fn link_replies(
        &self,
        posts: &Stream<G, Post>,
        c_pact: P,
        p_pact: P2,
        delay: usize,
    ) -> Stream<G, Comment>;
}

const POST: u32 = 0;
const COMMENT: u32 = 1;

type Node = (u32, u32);

fn add_edge(dsu: &mut Dsu<Node, Option<u32>>, parent: Node, child: Node) {
    match dsu.value(child.clone()) {
        None => {}
        Some(child_value) => {
            assert!(child_value.is_none());
        }
    };

    match dsu.value_mut(parent.clone()) {
        None => {}
        Some(_) => {
            dsu.union(parent, child);
        }
    }
}

impl<G, P, P2> LinkReplies<G, P, P2> for Stream<G, Comment>
where
    G: Scope<Timestamp = usize>,
    P: ParallelizationContract<usize, Comment>,
    P2: ParallelizationContract<usize, Post>,
{
    fn link_replies(
        &self,
        posts: &Stream<G, Post>,
        c_pact: P,
        p_pact: P2,
        delay: usize,
    ) -> Stream<G, Comment> {
        let mut dsu: Dsu<Node, Option<u32>> = Dsu::new();
        let mut comments_buffer: Stash<Comment> = Stash::new();

        self.binary_notify(
            posts,
            c_pact,
            p_pact,
            "LinkReplies",
            None,
            move |c_input, p_input, output, notificator| {
                // stash comments
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
                        dsu.insert((POST, post.id), Some(post.id));
                    }

                    // notify when all the posts and comments before the current
                    // watermark have been emitted
                    notificator.notify_at(cap.retain());
                });

                notificator.for_each(|cap, _, _| {
                    let time = *cap.time();
                    let mut replies = BinaryHeap::new();
                    let mut all_comments = Vec::new();

                    // handle comments and stash replies
                    for comment in comments_buffer.extract(delay, time) {
                        all_comments.push(comment.clone());

                        // if the comment is a watermark we don't process it
                        if !comment.is_watermark {
                            match comment.reply_to_post_id {
                                Some(post_id) => {
                                    // insert comment if post present
                                    match dsu.value((POST, post_id)) {
                                        Some(_) => dsu.insert((COMMENT, comment.id), None),
                                        None => { /* the post is on a different worker */ }
                                    }

                                    // add edge
                                    add_edge(&mut dsu, (POST, post_id), (COMMENT, comment.id));
                                }
                                None => {
                                    // reply
                                    replies.push(comment);
                                }
                            }
                        }
                    }

                    // handle replies
                    for reply in replies.into_sorted_vec().drain(..) {
                        match reply.reply_to_comment_id {
                            Some(comm_id) => {
                                // insert reply if comment present
                                match dsu.value((COMMENT, comm_id)) {
                                    Some(_) => dsu.insert((COMMENT, reply.id), None),
                                    None => { /* the comment is on a different worker */ }
                                }

                                // add edge
                                add_edge(&mut dsu, (COMMENT, comm_id), (COMMENT, reply.id));
                            }
                            None => { /* reply attached to nothing */ }
                        }
                    }

                    // attach the post_id to all comments/replies
                    let mut session = output.session(&cap);
                    for mut comment in all_comments.drain(..) {
                        if comment.is_watermark() {
                            session.give(comment);
                            continue;
                        }

                        match dsu.value((COMMENT, comment.id)) {
                            None => { /* comment/reply is not attached to our post */ }
                            Some(maybe_post) => {
                                if let Some(post) = maybe_post {
                                    comment.reply_to_post_id = Some(*post);
                                    session.give(comment);
                                }
                            }
                        }
                    }
                });
            },
        )
    }
}

#[cfg(test)]
#[rustfmt::skip]
mod link_replies_tests {
    extern crate timely;

    use crate::dto::common::Watermarkable;
    use crate::dto::post::Post;
    use crate::dto::comment::Comment;

    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::InputHandle;
    use timely::dataflow::operators::{Input, Inspect, Probe};

    use crate::operators::link_replies::LinkReplies;

    #[test]
    fn test_link_replies_links_all_comments () {
        timely::execute_from_args(std::env::args(), |worker| {
            let mut posts_input = InputHandle::new();
            let mut comments_input = InputHandle::new();

            let default_post = Post{is_watermark:false, ..Post::from_watermark("0")};
            let default_comment = Comment{is_watermark:false, ..Comment::from_watermark("0")};
            let posts_data = vec![
                Post{id:1, timestamp:7, ..default_post.clone()},
                Post{id:2, timestamp:7, ..default_post.clone()},
                Post{id:3, timestamp:7, ..default_post.clone()},
                Post::from_watermark("10"),
                Post::from_watermark("15"),
                Post::from_watermark("20"),
            ];
            let comments_data = vec![
                Comment{id:1, reply_to_post_id: Some(1), timestamp:5, ..default_comment.clone()},
                Comment{id:2, reply_to_post_id: Some(3), timestamp:5, ..default_comment.clone()},
                Comment{id:3, reply_to_comment_id: Some(2), timestamp:6, ..default_comment.clone()},
                Comment::from_watermark("10"),
                Comment{id:4, reply_to_comment_id: Some(1), timestamp:11, ..default_comment.clone()},
                Comment{id:5, reply_to_comment_id: Some(3), timestamp:12, ..default_comment.clone()},
                Comment{id:6, reply_to_comment_id: Some(2), timestamp:13, ..default_comment.clone()},
                Comment::from_watermark("15"),
                Comment::from_watermark("20"),
            ];


            let (posts_probe, comments_probe) = worker.dataflow(|scope| {
                let posts = scope.input_from(&mut posts_input);
                let comments = scope.input_from(&mut comments_input);

                let get_reply_to_post_id = |xs: &[Comment]| xs.iter()
                    .filter(|x| !x.is_watermark())
                    .filter(|x| x.reply_to_post_id != None)
                    .map(|x| x.reply_to_post_id.unwrap())
                    .collect::<Vec<_>>();

                comments.link_replies(
                    &posts,
                    Pipeline,
                    Pipeline,
                    5,
                ).inspect_batch(move |t, xs: &[Comment]| match t {
                    10  => assert_eq!(get_reply_to_post_id(xs), vec![1, 3, 3]),
                    15  => assert_eq!(get_reply_to_post_id(xs), vec![1, 3, 3]),
                    _  => unreachable!(),
                });

                (posts.probe(), comments.probe())
            });

            let batches = vec![
                (10, posts_data[0..4].to_vec(), comments_data[0..4].to_vec()),
                (15, posts_data[4..5].to_vec(), comments_data[4..8].to_vec()),
                (20, posts_data[5..6].to_vec(), comments_data[8..9].to_vec()),
            ];
            for (t, mut p_data, mut c_data) in batches {
                posts_input.send_batch(&mut p_data);
                posts_input.advance_to(t);
                comments_input.send_batch(&mut c_data);
                comments_input.advance_to(t);

                while posts_probe.less_than(posts_input.time()) {
                     worker.step();
                }
                while comments_probe.less_than(comments_input.time()) {
                     worker.step();
                }
            }
        })
        .unwrap();
    }
}
