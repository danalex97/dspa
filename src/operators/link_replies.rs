use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};
use timely::Data;

use crate::dto::common::Timestamped;
use crate::dto::comment::Comment;
use crate::dto::post::Post;

use crate::dsa::dsu::*;
use crate::dsa::stash::*;

use std::collections::binary_heap::BinaryHeap;
use std::collections::HashSet;
use std::collections::HashMap;

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
        Some(parent_value) => {
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

                    // notify in the future with delay so that we have the
                    // guarantee that all replies are attached to comments
                    notificator.notify_at(cap.delayed(&(cap.time() + delay)));
                });

                notificator.for_each(|cap, _, _| {
                    // note the time here is in the past since we waited for the delay
                    let time = *cap.time() - delay;
                    let mut replies = BinaryHeap::new();
                    let mut all_comments = Vec::new();

                    // handle comments and stash replies
                    for comment in comments_buffer.extract(delay, time) {
                        all_comments.push(comment.clone());

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
