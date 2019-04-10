extern crate timely;

use crate::connection::producer::FIXED_BOUNDED_DELAY;
use crate::operators::source::KafkaSource;
use crate::operators::buffer::Buffer;
use crate::operators::link_replies::LinkReplies;

use crate::dto::like::Like;
use crate::dto::comment::Comment;
use crate::dto::common::Timestamped;
use crate::dto::post::Post;

use crate::dsa::stash::*;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::broadcast::Broadcast;
use timely::dataflow::operators::Inspect;

const COLLECTION_PERIOD: usize = 1800; // seconds

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
            let mut posts_buffer: Stash<Post> = Stash::new();
            let mut likes_buffer: Stash<Like> = Stash::new();

            let buffered_posts = posts
                .buffer(Exchange::new(|p: &Post| p.id as u64), FIXED_BOUNDED_DELAY);

            comments
                .broadcast()
                .link_replies(&buffered_posts, Pipeline, Pipeline, FIXED_BOUNDED_DELAY)
                .inspect(|x| println!("{:?} {:?}", x.reply_to_post_id, x.reply_to_comment_id));
        });
    })
    .unwrap();
}
