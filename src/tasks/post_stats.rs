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

            let posts_with_likes = posts
                .buffer(Exchange::new(|p: &Post| p.id as u64), FIXED_BOUNDED_DELAY)
                .binary_notify(
                    &buffered_likes,
                    Pipeline,
                    Pipeline,
                    "AggregateLikes",
                    None,
                    move |p_input, l_input, output, notificator| {
                        let mut p_data = Vec::new();
                        p_input.for_each(|cap, input| {
                            // schedule the first notification
                            if !scheduled_first_notification {
                                scheduled_first_notification = true;
                                notificator.notify_at(cap.delayed(
                                    &(cap.time() + COLLECTION_PERIOD - FIXED_BOUNDED_DELAY),
                                ));
                            }

                            // stash all posts
                            input.swap(&mut p_data);
                            for post in p_data.drain(..) {
                                let time = post.timestamp().clone();
                                posts_buffer.stash(time, post);
                            }
                        });

                        // stash all likes
                        let mut l_data = Vec::new();
                        l_input.for_each(|_, input| {
                            input.swap(&mut l_data);
                            for like in l_data.drain(..) {
                                let time = like.timestamp().clone();
                                likes_buffer.stash(time, like);
                            }
                        });

                        notificator.for_each(|cap, _, notificator| {
                            notificator.notify_at(cap.delayed(&(cap.time() + COLLECTION_PERIOD)));
                            let mut new_posts = vec![];
                            for post in posts_buffer.extract(COLLECTION_PERIOD, *cap.time()) {
                                new_posts.push(post);
                            }
                            for like in likes_buffer.extract(COLLECTION_PERIOD, *cap.time()) {
                                // [TODO:] we need the likes only to find the active period; that
                                // is we do not need to actually count them
                            }
                            output.session(&cap).give_vec(&mut new_posts);
                        });
                    },
                );

            comments
                .broadcast()
                .link_replies(&posts_with_likes, Pipeline, Pipeline, FIXED_BOUNDED_DELAY)
                .inspect_batch(|t, xs| println!("{:?} {:?}", t, xs));
        });
    })
    .unwrap();
}
