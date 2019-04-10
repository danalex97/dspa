use crate::dto::common::Timestamped;
use std::collections::HashMap;
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};
use timely::Data;

use crate::dto::comment::Comment;
use crate::dto::post::Post;

use crate::dsa::stash::*;
use crate::dsa::dsu::*;
use std::collections::binary_heap::BinaryHeap;
use std::collections::HashSet;

pub trait LinkReplies<G, P, P2> where
        G: Scope,
        P: ParallelizationContract<usize, Comment>,
        P2: ParallelizationContract<usize, Post> {
    fn link_replies(&self, posts: &Stream<G, Post>, c_pact: P, p_pact: P2, delay: usize) -> Stream<G, Comment>;
}

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

impl<G, P, P2> LinkReplies<G, P, P2> for Stream<G, Comment> where
        G: Scope<Timestamp = usize>,
        P: ParallelizationContract<usize, Comment>,
        P2: ParallelizationContract<usize, Post> {
    fn link_replies(&self, posts: &Stream<G, Post>, c_pact: P, p_pact: P2, delay: usize) -> Stream<G, Comment> {
        let mut dsu: Dsu<Node, Option<u32>> = Dsu::new();
        let mut comments_buffer: Stash<Comment> = Stash::new();

        self.binary_notify(
            posts,
            c_pact,
            p_pact,
            "LinkReplies",
            None,
            move |c_input, p_input, output, notificator| {
                // let mut c_data = Vec::new();
                // c_input.for_each(|_, input| {
                //     input.swap(&mut c_data);
                //     for comment in c_data.drain(..) {
                //         let time = comment.timestamp().clone();
                //         comments_buffer.stash(time, comment);
                //     }
                // });
                //
                // let mut p_data = Vec::new();
                // p_input.for_each(|cap, input| {
                //     input.swap(&mut p_data);
                //     for post in p_data.drain(..) {
                //         // insert posts values into DSU
                //         dsu.insert((POST, post.id), (0, 0));
                //
                //         // [TODO:] modify when looking at active posts
                //         all_posts.insert(post);
                //     }
                //
                //     // notify in the future with FIXED_BOUNDED_DELAY so that we have the
                //     // guarantee that all replies are attached to comments
                //     notificator.notify_at(cap.delayed(
                //         &(cap.time() + FIXED_BOUNDED_DELAY),
                //     ));
                // });
                //
                // notificator.for_each(|cap, _, _| {
                //     // [TODO: check again what to do with this timestamp]
                //     // note the time here is in the past since we waited for the delay
                //     let time = *cap.time() - FIXED_BOUNDED_DELAY;
                //     let mut replies = BinaryHeap::new();
                //
                //     // handle comments and stash replies
                //     for comment in comments_buffer.extract(COLLECTION_PERIOD, time) {
                //         match comment.reply_to_post_id {
                //             Some(post_id) => {
                //                 // insert comment if post present
                //                 match dsu.value((POST, post_id)) {
                //                     Some(_) => dsu.insert((COMMENT, comment.id), (1, 0)),
                //                     None    => {/* the post is on a different worker */},
                //                 }
                //
                //                 // add edge
                //                 add_edge(&mut dsu, (POST, post_id), (COMMENT, comment.id));
                //             },
                //             None => {
                //                 // reply
                //                 replies.push(comment);
                //             }
                //         }
                //     }
                //
                //     // handle replies
                //     for reply in replies.into_sorted_vec().drain(..) {
                //         match reply.reply_to_comment_id {
                //             Some(comm_id) => {
                //                 // insert reply if comment present
                //                 match dsu.value((COMMENT, comm_id)) {
                //                     Some(_) => dsu.insert((COMMENT, reply.id), (0, 1)),
                //                     None    => {/* the comment is on a different worker */},
                //                 }
                //
                //                 // add edge
                //                 add_edge(&mut dsu, (COMMENT, comm_id), (COMMENT, reply.id));
                //             }
                //             None => {/* reply attached to nothing */},
                //         }
                //     }
                //
                //     // [TODO:] this is currently used only for debugging
                //     let mut session = output.session(&cap);
                //
                //     for post in all_posts.iter() {
                //         match dsu.value((POST, post.id)) {
                //             None => {}
                //             Some(all) => {
                //                 if all.0 != 0 {
                //                     session.give((post.id, all.clone()));
                //                 }
                //             },
                //         }
                //     }
                // });
            }
        )
    }
}
