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
const ACTIVE_POST_PERIOD: usize = 4 * 60 * 60; // seconds

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
                    &buffered_posts,
                    Pipeline,
                    Pipeline,
                    "WhoToFollow",
                    None,
                    move |ap_input, bp_input, output, notificator| {
                    }
                )
                .inspect_batch(|t, xs| println!("@t {:?}: {:?}", t, xs));
        });
    })
        .unwrap();
}
