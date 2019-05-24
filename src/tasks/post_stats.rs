extern crate timely;

use crate::connection::producer::FIXED_BOUNDED_DELAY;
use crate::operators::buffer::Buffer;
use crate::operators::source::KafkaSource;

use crate::operators::active_posts::ActivePosts;
use crate::operators::engaged_users::EngagedUsers;
use crate::operators::link_replies::LinkReplies;
use crate::operators::post_counts::PostCounts;

use crate::dto::comment::Comment;
use crate::dto::like::Like;
use crate::dto::post::Post;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::broadcast::Broadcast;
use timely::dataflow::operators::Inspect;

use std::collections::hash_map::DefaultHasher;
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
                .inspect_batch(|t, xs| println!("#uniquely engaged people @t={:?}: {:?}", t, xs));

            active_posts
                .counts(&linked_comments, Pipeline, Pipeline, COLLECTION_PERIOD)
                .inspect_batch(|t, xs| println!("#comments and replies @t={:?}: {:?}", t, xs));
        });
    })
    .unwrap();
}
